package shardkv

import "raft-kv/shardctl"

func (kv *ShardKv) handleConfigChange(command RaftCommand) *OperationReply {
	switch command.CommandType {
	case ConfigChange:
		newConfig := command.Data.(shardctl.Config)
		return kv.applyNewChange(newConfig)
	case ShardMigration:
		shardOperationReply := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardOperationReply)
	case ShardGC:
		shardOperationArgs := command.Data.(ShardOperationArgs)
		return kv.applyShardGC(&shardOperationArgs)
	default:
		panic("unsupported command")
	}
}

func (kv *ShardKv) applyNewChange(newConfig shardctl.Config) *OperationReply {
	if kv.currentConfig.Num+1 != newConfig.Num {
		return &OperationReply{
			Err: ErrWrongConfig,
		}
	}

	// apply config change in a group
	for i := 0; i < shardctl.NShards; i++ {
		if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
			// move out data to another group
			if newConfig.Shards[i] != InvalidGroupId {
				kv.shards[i].Status = MoveOut
			}

		} else if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
			// move in data to this group
			if kv.currentConfig.Shards[i] != InvalidGroupId {
				kv.shards[i].Status = MoveIn
			}
		}
	}

	kv.prevConfig = kv.currentConfig
	kv.currentConfig = newConfig

	return &OperationReply{Err: OK}
}

func (kv *ShardKv) applyShardMigration(reply *ShardOperationReply) *OperationReply {
	if kv.currentConfig.Num != reply.ConfigNum {
		return &OperationReply{
			Err: ErrWrongConfig,
		}
	}

	// move to state machine
	for shardId, shardData := range reply.ShardsData {
		shard := kv.shards[shardId]
		if shard.Status == MoveIn {
			for k, v := range shardData {
				shard.Map[k] = v
			}
			// after move data into shard, we should wait for cleanness
			shard.Status = GC
		} else {
			break
		}
	}

	// move to duplicate table
	for k, v := range reply.DuplicateTable {
		curLastReply, ok := kv.duplicateTable[k]
		// no record or record out dated, then to update table
		if !ok || curLastReply.SeqId < v.SeqId {
			kv.duplicateTable[k] = v
		}
	}

	return &OperationReply{Err: OK}
}

func (kv *ShardKv) applyShardGC(shardInfo *ShardOperationArgs) *OperationReply {
	if kv.currentConfig.Num == shardInfo.ConfigNum {
		for _, shardId := range shardInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == MoveOut {
				kv.shards[shardId] = NewStateMachine()
			} else if shard.Status == GC {
				shard.Status = Normal
			} else {
				break
			}
		}
	}

	return &OperationReply{Err: OK}
}

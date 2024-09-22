package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKv) applyTicker() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				// read from snapshot
				kv.mu.Lock()
				kv.readSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
				kv.mu.Unlock()
			} else if msg.CommandValid {
				// read from append log command
				kv.mu.Lock()
				// message has been served
				if msg.CommandIndex <= kv.lastApplied {
					break
				}
				kv.lastApplied = msg.CommandIndex

				// parse apply message from raft layer
				raftCommand := msg.Command.(RaftCommand)
				var opReply = &OperationReply{}

				if raftCommand.CommandType == ClientOperation {
					// client data change
					opReply = kv.handleClientOperation(raftCommand)
				} else {
					// config change
					opReply = kv.handleConfigChange(raftCommand)
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					// notify client
					notifyCh := kv.getNotifyChannel(msg.CommandIndex)
					notifyCh <- opReply
				}

				// do snapshot
				if kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState {
					kv.doSnapshot(msg.CommandIndex)
				}

				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKv) fetchConfigTicker() {
	for !kv.killed() {

		if _, isLeader := kv.rf.GetState(); isLeader {
			var needFetch = true
			kv.mu.Lock()
			for _, shard := range kv.shards {
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}
			var curConfigNum = kv.currentConfig.Num
			kv.mu.Unlock()

			if needFetch {
				// get a new config
				newConfig := kv.shardClerk.Query(curConfigNum + 1)
				if newConfig.Num == curConfigNum+1 {
					// apply to raft layer to let every other followers update the config
					kv.sendConfigToRaft(RaftCommand{
						CommandType: ConfigChange,
						Data:        newConfig,
					}, &OperationReply{})
				}
			}
		}

		time.Sleep(FetchConfigInterval)
	}
}

// task to run shard migrations between different group
func (kv *ShardKv) shardMigrationTicker() {
	for !kv.killed() {

		if _, isLeader := kv.rf.GetState(); isLeader {
			// deal with move in data
			kv.mu.Lock()

			groupShards := kv.getGroupShardsByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shards := range groupShards {
				// concurrently send request to each of server to get reply
				wg.Add(1)

				go func(prevGroupServers []string, configNum int, shardIds []int) {
					defer wg.Done()

					args := ShardOperationArgs{
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					shardOperationReply := ShardOperationReply{}

					for _, server := range prevGroupServers {
						clientEnd := kv.make_end(server)
						// To the move in group server's data before move in
						ok := clientEnd.Call("ShardKv.GetShardData", args, &shardOperationReply)
						if ok && shardOperationReply.Err == OK {
							// send to raft layer to synchronize data
							kv.sendConfigToRaft(RaftCommand{
								CommandType: ShardMigration,
								Data:        shardOperationReply,
							}, &OperationReply{})
						}
					}
					// this task happens config change and set shard status to Move in, data has not migrated,
					// so we should use the prev config to get original group servers
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shards)

				kv.mu.Unlock()
				wg.Wait()
			}
		}

		time.Sleep(FetchShardMigrationInterval)
	}
}

func (kv *ShardKv) shardGCTicker() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()

			groupShards := kv.getGroupShardsByStatus(GC)

			var wg sync.WaitGroup
			for gid, shards := range groupShards {
				wg.Add(1)

				go func(groupServers []string, configNum int, shardIds []int) {
					defer wg.Done()

					shardOperationArgs := ShardOperationArgs{
						ShardIds:  shardIds,
						ConfigNum: configNum,
					}
					reply := ShardOperationReply{}

					for _, server := range groupServers {
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKv.DeleteShardData", shardOperationArgs, &reply)
						if ok && reply.Err == OK {
							// set back to normal state if GC succeed
							kv.sendConfigToRaft(RaftCommand{ShardGC, shardOperationArgs}, &OperationReply{})
						}
					}

				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shards)
			}

			kv.mu.Unlock()
			wg.Wait()

		}

		time.Sleep(ShardGCInterval)
	}
}

func (kv *ShardKv) sendConfigToRaft(command RaftCommand, opReply *OperationReply) {
	index, _, isLeader := kv.rf.StartAppendCommandInLeader(command)

	if !isLeader {
		opReply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case msg := <-notifyCh:
		opReply.Value = msg.Value
		opReply.Err = msg.Err
	case <-time.After(TimeoutInterval):
		opReply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.deleteNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// group to shard (state machine) mapping from prev config
func (kv *ShardKv) getGroupShardsByStatus(status ShardStatus) map[int][]int {
	groupShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status != status {
			continue
		}

		// this task happens config change and set shard status to Move in, data has not migrated,
		// so we should use the prev config to get original group servers
		gid := kv.prevConfig.Shards[i]
		if gid != InvalidGroupId {
			if _, ok := groupShards[gid]; !ok {
				groupShards[gid] = make([]int, 0)
			}
			groupShards[gid] = append(groupShards[gid], i)
		}
	}

	return groupShards
}

// GetShardData rpc to get old group's shard data
func (kv *ShardKv) GetShardData(args ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// current group's data is not latest, not ready to copy
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrDataNotReady
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// copy state machine
	reply.ShardsData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardsData[shardId] = kv.shards[shardId].copyData()
	}

	// copy duplicate Table
	reply.DuplicateTable = make(map[int64]LastReply)
	for k, v := range kv.duplicateTable {
		reply.DuplicateTable[k] = v.copy()
	}

	reply.ConfigNum = args.ConfigNum
	reply.Err = OK
}

// DeleteShardData rpc to clean up shard's data
func (kv *ShardKv) DeleteShardData(args ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// current group's data is more updated, don't need to update
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OperationReply
	kv.sendConfigToRaft(RaftCommand{ShardGC, args}, &opReply)

	reply.Err = opReply.Err
}

func (kv *ShardKv) handleClientOperation(command RaftCommand) *OperationReply {
	var opReply = &OperationReply{}
	op := command.Data.(Operation)
	shard := key2shard(op.Key)

	if !kv.isGroupMatchWithAppropriateStatus(op.Key) {
		return opReply
	}

	switch op.OpType {
	case OpGet:
		opReply.Value, opReply.Err = kv.shards[shard].Get(op.Key)
	case OpPut:
		if kv.isDuplicateOp(op.ClientId, op.SeqId) {
			opReply = kv.duplicateTable[op.ClientId].Reply
		} else {
			opReply.Err = kv.shards[shard].Put(op.Key, op.Value)
			// cache command
			kv.duplicateTable[op.ClientId] = LastReply{
				SeqId: op.SeqId,
				Reply: opReply,
			}
		}
	case OpAppend:
		if kv.isDuplicateOp(op.ClientId, op.SeqId) {
			opReply = kv.duplicateTable[op.ClientId].Reply
		} else {
			opReply.Err = kv.shards[shard].Append(op.Key, op.Value)
			// cache command
			kv.duplicateTable[op.ClientId] = LastReply{
				SeqId: op.SeqId,
				Reply: opReply,
			}
		}
	default:
		panic("unknown operation type")
	}

	return opReply
}

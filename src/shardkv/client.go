package shardkv

import (
	"raft-kv/rpc"
	"raft-kv/shardctl"
	"time"
)

type Clerk struct {
	ctlClerk *shardctl.Clerk
	config   shardctl.Config // clerk's shard controller's config
	new_end  func(string) *rpc.ClientEnd

	leaderIds map[int]int // gid->leaderId

	clientId int64 // Mark a unique command
	seqId    int64
}

func NewClerk(ctlers []*rpc.ClientEnd, new_end func(string) *rpc.ClientEnd) *Clerk {
	ck := &Clerk{
		ctlClerk:  shardctl.NewClerk(ctlers),
		new_end:   new_end,
		leaderIds: make(map[int]int),
		clientId:  nrand(),
		seqId:     0,
	}
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			startLeaderId := ck.leaderIds[gid]

			for {
				server := ck.new_end(servers[ck.leaderIds[gid]])
				var reply GetReply

				ok := server.Call("ShardKV.Get", args, &reply)
				if ok && reply.Err == ErrKeyNotFound {
					return reply.Value
				}

				if ok && reply.Err == ErrWrongGroup {
					break
				}

				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(ck.leaderIds)
					if ck.leaderIds[gid] == startLeaderId {
						break
					}

					continue
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.ctlClerk.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

func (ck *Clerk) PutAppend(key string, value string, opType OperationType) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		OpType:   opType,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.leaderIds[gid]; !exist {
				// don't know leader, init a random leader
				ck.leaderIds[gid] = 0
			}
			startLeaderId := ck.leaderIds[gid]

			for {
				server := ck.new_end(servers[ck.leaderIds[gid]]) // try to call leader for the group to put append log
				var reply PutAppendReply

				ok := server.Call("ShardKV.PutAppend", args, &reply)
				if ok && reply.Err == ErrKeyNotFound {
					ck.seqId++
					return
				}

				if ok && reply.Err == ErrWrongGroup {
					break
				}

				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(ck.leaderIds)
					if ck.leaderIds[gid] == startLeaderId {
						break
					}

					continue
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.ctlClerk.Query(-1)
	}
}

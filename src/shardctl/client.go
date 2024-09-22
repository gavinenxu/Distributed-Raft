package shardctl

import (
	"raft-kv/rpc"
)

type Clerk struct {
	servers []*rpc.ClientEnd

	leaderId int
	clientId int64
	seqId    int64
}

func NewClerk(servers []*rpc.ClientEnd) *Clerk {
	clerk := &Clerk{
		servers:  servers,
		leaderId: 0,
		clientId: nrand(),
		seqId:    0,
	}

	return clerk
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := QueryReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{Servers: servers, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := MoveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

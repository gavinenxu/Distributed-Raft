package shardctl

import (
	"crypto/rand"
	"errors"
	"math/big"
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

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	SeqId    int64
}

type QueryReply struct {
	Err    error
	Config Config
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := QueryReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int64
}

type JoinReply struct {
	Err error
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{Servers: servers, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int64
}

type LeaveReply struct {
	Err error
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqId    int64
}

type MoveReply struct {
	Err error
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
	for {
		reply := MoveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func nrand() int64 {
	MAX := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, MAX)
	x := bigx.Int64()
	return x
}

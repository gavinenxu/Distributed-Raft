package kvraft

import (
	"crypto/rand"
	"errors"
	"math/big"
	"raft-kv/rpc"
)

type Clerk struct {
	servers  []*rpc.ClientEnd
	leaderId int

	clientId int64 // Mark a unique command
	seqId    int64
}

func NewClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:  servers,
		leaderId: 0,
		clientId: nrand(),
		seqId:    0,
	}
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Value
	}

}

func (ck *Clerk) putAppend(key string, value string, op OperationType) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, OpAppend)
}

func nrand() int64 {
	MAX := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, MAX)
	x := bigx.Int64()
	return x
}

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

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
	Err   error
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("Server.Get", args, reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Value
	}

}

type PutArgs struct {
	Key      string
	Value    string
	ClientId int64
	SeqId    int64
}

type PutReply struct {
	Err error
}

func (ck *Clerk) Put(key string, value string) {
	args := PutArgs{
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("Server.Put", args, reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Append(key string, value string) {
	args := PutArgs{
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("Server.Append", args, reply)
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

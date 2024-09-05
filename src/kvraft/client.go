package kvraft

import (
	"errors"
	"raft-kv/rpc"
)

type Clerk struct {
	servers  []*rpc.ClientEnd
	leaderId int
}

func NewClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:  servers,
		leaderId: 0,
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
	reply := &GetReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("Server.Get", args, reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Value
	}

}

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err error
}

func (ck *Clerk) Put(key string, value string) {
	args := PutArgs{Key: key, Value: value}
	reply := &GetReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("Server.Put", args, reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Append(key string, value string) {
	args := PutArgs{Key: key, Value: value}
	reply := &GetReply{}

	for {
		ok := ck.servers[ck.leaderId].Call("Server.Append", args, reply)
		if !ok || errors.Is(reply.Err, ErrWrongLeader) || errors.Is(reply.Err, ErrTimeout) {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

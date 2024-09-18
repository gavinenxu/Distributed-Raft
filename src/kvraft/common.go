package kvraft

import (
	"time"
)

type Err string

const (
	OK             Err = "ok"
	ErrKeyNotFound Err = "key not found"
	ErrWrongLeader Err = "wrong leader"
	ErrTimeout     Err = "timeout"
)

const TimeoutInterval = 500 * time.Millisecond

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
	Err   Err
}

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       OperationType
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type Operation struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OperationReply struct {
	Value string
	// need to pass via
	Err Err
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OperationReply
}

package kvraft

import (
	"errors"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrWrongLeader = errors.New("wrong leader")
	ErrTimeout     = errors.New("timeout")
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
	Err   error
}

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       OperationType
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err error
}

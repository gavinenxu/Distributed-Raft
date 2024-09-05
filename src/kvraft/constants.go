package kvraft

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrWrongLeader = errors.New("wrong leader")
	ErrTimeout     = errors.New("timeout")
)

type OperationType byte

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

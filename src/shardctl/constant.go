package shardctl

import "errors"

var (
	ErrWrongLeader = errors.New("wrong leader")
	ErrTimeout     = errors.New("timeout")
)

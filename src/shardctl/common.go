package shardctl

import (
	"crypto/rand"
	"math/big"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards The number of shards.
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func DefaultConfig() Config {
	return Config{
		Groups: make(map[int][]string),
	}
}

const TimeOutInterval = time.Duration(5) * time.Millisecond

type Err string

var (
	ErrWrongLeader Err = "wrong leader"
	ErrTimeout     Err = "timeout"
)

type OperationType byte

const (
	OpQuery OperationType = iota
	OpJoin
	OpLeave
	OpMove
)

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	SeqId    int64
}

type QueryReply struct {
	Err    Err
	Config Config
}

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int64
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int64
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqId    int64
}

type MoveReply struct {
	Err Err
}

// types for server

type Operation struct {
	OpType OperationType

	Num      int              // Query
	Servers  map[int][]string // Join
	GIDs     []int            // Leave
	Shard    int              // Move
	GID      int              // Move
	ClientId int64
	SeqId    int64
}

type OperationReply struct {
	CtlConfig Config
	Err       Err
}

type LastReply struct {
	seqId int64
	reply *OperationReply
}

func nrand() int64 {
	MAX := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, MAX)
	x := bigx.Int64()
	return x
}

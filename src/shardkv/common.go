package shardkv

import (
	"crypto/rand"
	"math/big"
	"raft-kv/shardctl"
	"time"
)

const InvalidGroupId = 0

const (
	TimeoutInterval             = 500 * time.Millisecond
	FetchConfigInterval         = 100 * time.Millisecond
	FetchShardMigrationInterval = 50 * time.Millisecond
	ShardGCInterval             = 50 * time.Millisecond
)

type Err string

var (
	OK              Err = "ok"
	ErrWrongLeader  Err = "wrong leader"
	ErrTimeout      Err = "timeout"
	ErrWrongGroup   Err = "wrong group"
	ErrKeyNotFound  Err = "key not found"
	ErrWrongConfig  Err = "wrong config"
	ErrDataNotReady Err = "data not ready"
)

type OperationType byte

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

// Operation communication with raft layer
type Operation struct {
	Key    string
	Value  string
	OpType OperationType

	ClientId int64
	SeqId    int64
}

type OperationReply struct {
	Value string
	Err   Err
}

type LastReply struct {
	SeqId int64
	Reply *OperationReply
}

func (lr *LastReply) copy() LastReply {
	return LastReply{
		SeqId: lr.SeqId,
		Reply: &OperationReply{
			Value: lr.Reply.Value,
			Err:   lr.Reply.Err,
		},
	}
}

type RaftCommandType uint8

const (
	ClientOperation RaftCommandType = iota
	ConfigChange
	ShardMigration
	ShardGC
)

// RaftCommand define different command type sent to raft
type RaftCommand struct {
	CommandType RaftCommandType
	Data        interface{}
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	MoveIn
	MoveOut
	GC
)

// ShardOperationArgs rpc arguments to get each shard's info
type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	ConfigNum      int
	ShardsData     map[int]map[string]string // shardId => state machine's data
	DuplicateTable map[int64]LastReply
	Err            Err
}

func nrand() int64 {
	MAX := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, MAX)
	x := bigx.Int64()
	return x
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctl.NShards
	return shard
}

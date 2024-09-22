package shardkv

import (
	"bytes"
	"raft-kv/encoding"
	"raft-kv/raft"
	"raft-kv/rpc"
	"raft-kv/shardctl"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKv struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMessage
	make_end func(string) *rpc.ClientEnd
	dead     int32
	gid      int
	ctrlers  []*rpc.ClientEnd

	maxRaftState int

	notifyChs      map[int]chan *OperationReply
	lastApplied    int
	shards         map[int]*StateMachine
	currentConfig  shardctl.Config // current shard controller's config
	prevConfig     shardctl.Config
	duplicateTable map[int64]LastReply // clientId -> seqId to check the duplicate log entry
	shardClerk     *shardctl.Clerk     // To pull the latest config from controller
}

func NewShardKv(clientEnds []*rpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*rpc.ClientEnd, make_end func(string) *rpc.ClientEnd) *ShardKv {
	encoding.Register(Operation{})
	encoding.Register(RaftCommand{})
	encoding.Register(shardctl.Config{})
	encoding.Register(ShardOperationArgs{})
	encoding.Register(ShardOperationReply{})

	kv := &ShardKv{
		mu:             sync.Mutex{},
		me:             me,
		applyCh:        make(chan raft.ApplyMessage),
		make_end:       make_end,
		dead:           0,
		gid:            gid,
		ctrlers:        ctrlers,
		maxRaftState:   maxRaftState,
		notifyChs:      make(map[int]chan *OperationReply),
		lastApplied:    0,
		shards:         make(map[int]*StateMachine),
		currentConfig:  shardctl.DefaultConfig(),
		prevConfig:     shardctl.DefaultConfig(),
		duplicateTable: make(map[int64]LastReply),
	}
	kv.applyCh = make(chan raft.ApplyMessage)
	kv.rf = raft.NewRaft(clientEnds, me, persister, kv.applyCh)
	kv.shardClerk = shardctl.NewClerk(ctrlers)

	go kv.applyTicker()
	go kv.fetchConfigTicker()
	go kv.shardMigrationTicker()
	go kv.shardGCTicker()

	return kv
}

// Get  log entry, then get result from notifyChannel through state machine
func (kv *ShardKv) Get(args GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if !kv.isGroupMatchWithAppropriateStatus(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// apply logs in raft first
	commandIndex, _, isLeader := kv.rf.StartAppendCommandInLeader(
		RaftCommand{
			CommandType: ClientOperation,
			Data: Operation{
				Key:    args.Key,
				OpType: OpGet,
			},
		})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(commandIndex)
	kv.mu.Unlock()

	select {
	case resp := <-notifyCh:
		reply.Value = resp.Value
		reply.Err = resp.Err
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.deleteNotifyChannel(commandIndex)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKv) PutAppend(args PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if !kv.isGroupMatchWithAppropriateStatus(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	if kv.isDuplicateOp(args.ClientId, args.SeqId) {
		last, _ := kv.duplicateTable[args.ClientId]
		reply.Err = last.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// apply logs in raft first
	commandIndex, _, isLeader := kv.rf.StartAppendCommandInLeader(
		RaftCommand{
			CommandType: ClientOperation,
			Data: Operation{
				Key:      args.Key,
				Value:    args.Value,
				OpType:   args.OpType,
				ClientId: args.ClientId,
				SeqId:    args.SeqId,
			},
		})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(commandIndex)
	kv.mu.Unlock()

	select {
	case resp := <-notifyCh:
		reply.Err = resp.Err
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.deleteNotifyChannel(commandIndex)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKv) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKv) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKv) getNotifyChannel(i int) chan *OperationReply {
	if _, ok := kv.notifyChs[i]; !ok {
		kv.notifyChs[i] = make(chan *OperationReply, 1)
	}
	return kv.notifyChs[i]
}

func (kv *ShardKv) deleteNotifyChannel(i int) {
	delete(kv.notifyChs, i)
}

func (kv *ShardKv) isGroupMatchWithAppropriateStatus(key string) bool {
	shard := key2shard(key)
	status := kv.shards[shard].Status
	return kv.currentConfig.Shards[shard] == kv.gid && (status == Normal || status == GC)
}

func (kv *ShardKv) isDuplicateOp(clientId, seqId int64) bool {
	reply, ok := kv.duplicateTable[clientId]
	return ok && reply.SeqId >= seqId
}

func (kv *ShardKv) doSnapshot(index int) {
	// kvserver to store state machine and duplicate table
	writer := new(bytes.Buffer)
	enc := encoding.NewEncoder(writer)
	_ = enc.Encode(kv.shards)
	_ = enc.Encode(kv.duplicateTable)
	_ = enc.Encode(kv.currentConfig)
	_ = enc.Encode(kv.prevConfig)

	kv.rf.Snapshot(index, writer.Bytes())
}

func (kv *ShardKv) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		for i := 0; i < shardctl.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewStateMachine()
			}
		}
		return
	}

	// read state machine and duplicate table from snapshot
	reader := bytes.NewBuffer(snapshot)
	dec := encoding.NewDecoder(reader)

	var nStateMachine map[int]*StateMachine
	if err := dec.Decode(&nStateMachine); err != nil {
		panic(err)
	}

	var duplicateTable map[int64]LastReply
	if err := dec.Decode(&duplicateTable); err != nil {
		panic(err)
	}

	var currentConfig shardctl.Config
	if err := dec.Decode(&currentConfig); err != nil {
		panic(err)
	}

	var prevConfig shardctl.Config
	if err := dec.Decode(&prevConfig); err != nil {
		panic(err)
	}

	kv.shards = nStateMachine
	kv.duplicateTable = duplicateTable
	kv.currentConfig = currentConfig
	kv.prevConfig = prevConfig
}

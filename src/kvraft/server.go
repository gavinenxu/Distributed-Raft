package kvraft

import (
	"raft-kv/encoding"
	"raft-kv/raft"
	"raft-kv/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// 1. KVServers receive application layer's command from "Get, Put, Append" methods,
// 2. it will write the log first through apply chan,
// then if it's leader it will listen to notify channel to get result from state machine before timeout
// 3. a background thread will listen to the apply channel which gets result from raft layer,
// if a log has been applied to raft, it will send back message to this thread, then we apply the changes to
// state machine, after that, send result to the tmp notify channel

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMessage
	dead    int32

	maxRaftState int

	notifyChs    map[int]chan *OperationReply
	stateMachine *MemoryKVStateMachine
	lastApplied  int

	duplicateTable map[int64]LastOperationInfo // clientId -> seqId to check the duplicate log entry
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
	Err   error
}

type LastOperationInfo struct {
	seqId int64
	reply *OperationReply
}

func NewKVServer(clientEnds []*rpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	encoding.Register(Operation{})

	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMessage),
		dead:         0,
		maxRaftState: maxRaftState,
		notifyChs:    make(map[int]chan *OperationReply),
		stateMachine: NewMemoryKVStateMachine(),
	}
	kv.applyCh = make(chan raft.ApplyMessage)
	kv.rf = raft.NewRaft(clientEnds, me, persister, kv.applyCh)

	go kv.applyTicker()

	return kv
}

// Get  log entry, then get result from notifyChannel through state machine
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// apply logs in raft first
	commandIndex, _, isLeader := kv.rf.StartAppendCommandInLeader(
		Operation{
			Key:    args.Key,
			OpType: OpGet,
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.isDuplicateOp(args.ClientId, args.SeqId) {
		last, _ := kv.duplicateTable[args.ClientId]
		reply.Err = last.reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	commandIndex, _, isLeader := kv.rf.StartAppendCommandInLeader(
		Operation{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   args.Op,
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
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

	defer func() {
		kv.mu.Lock()
		kv.deleteNotifyChannel(commandIndex)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getNotifyChannel(i int) chan *OperationReply {
	if _, ok := kv.notifyChs[i]; !ok {
		kv.notifyChs[i] = make(chan *OperationReply, 1)
	}
	return kv.notifyChs[i]
}

func (kv *KVServer) deleteNotifyChannel(i int) {
	delete(kv.notifyChs, i)
}

// background thread to accept msg from application layer through applyCh
// then send it to state machine to store the value
func (kv *KVServer) applyTicker() {
	for !kv.Killed() {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				break
			}

			kv.mu.Lock()

			// message has been served
			if msg.CommandIndex <= kv.lastApplied {
				break
			}
			kv.lastApplied = msg.CommandIndex

			op := msg.Command.(Operation)
			var opReply *OperationReply

			switch op.OpType {
			case OpGet:
				opReply.Value, opReply.Err = kv.stateMachine.Get(op.Key)
			case OpPut:
				if kv.isDuplicateOp(op.ClientId, op.SeqId) {
					opReply = kv.duplicateTable[op.ClientId].reply
				} else {
					opReply.Err = kv.stateMachine.Put(op.Key, op.Value)
					// cache command
					kv.duplicateTable[op.ClientId] = LastOperationInfo{
						seqId: op.SeqId,
						reply: opReply,
					}
				}
			case OpAppend:
				if kv.isDuplicateOp(op.ClientId, op.SeqId) {
					opReply = kv.duplicateTable[op.ClientId].reply
				} else {
					opReply.Err = kv.stateMachine.Append(op.Key, op.Value)
					// cache command
					kv.duplicateTable[op.ClientId] = LastOperationInfo{
						seqId: op.SeqId,
						reply: opReply,
					}
				}
			default:
				panic("unknown operation type")
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				// notify client
				notifyCh := kv.getNotifyChannel(msg.CommandIndex)
				notifyCh <- opReply
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) isDuplicateOp(clientId, seqId int64) bool {
	reply, ok := kv.duplicateTable[clientId]
	return ok && reply.seqId <= seqId
}

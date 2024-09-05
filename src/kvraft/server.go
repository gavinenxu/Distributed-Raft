package kvraft

import (
	"raft-kv/raft"
	"raft-kv/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// 1. Servers receive application layer's command from "Get, Put, Append" methods,
// 2. it will write the log first through apply chan,
// then if it's leader it will listen to notify channel to get result from state machine before timeout
// 3. a background thread will listen to the apply channel which gets result from raft layer,
// if a log has been applied to raft, it will send back message to this thread, then we apply the changes to
// state machine, after that, send result to the tmp notify channel

type Server struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMessage
	dead    int32

	maxRaftState int

	notifyChs    map[int]chan *OperationReply
	stateMachine *ClerkStateMachine
	lastApplied  int
}

type Operation struct {
	Key    string
	Value  string
	OpType OperationType
}

type OperationReply struct {
	Value string
	Err   error
}

func NewServer(clientEnds []*rpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *Server {
	//encoding.Register(Operation{})

	server := &Server{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMessage),
		dead:         0,
		maxRaftState: maxRaftState,
		notifyChs:    make(map[int]chan *OperationReply),
		stateMachine: NewClerkStateMachine(),
	}
	server.applyCh = make(chan raft.ApplyMessage)
	server.rf = raft.NewRaft(clientEnds, me, persister, server.applyCh)

	return server
}

func (server *Server) Get(args *GetArgs, reply *GetReply) {
	// apply logs in raft first
	commandIndex, _, isLeader := server.rf.StartAppendCommandInLeader(
		Operation{
			Key:    args.Key,
			OpType: OpGet,
		})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	server.mu.Lock()
	notifyCh := server.getNotifyChannel(commandIndex)
	server.mu.Unlock()

	select {
	case resp := <-notifyCh:
		reply.Value = resp.Value
		reply.Err = resp.Err
	case <-time.After(time.Duration(5) * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go func() {
		server.mu.Lock()
		server.deleteNotifyChannel(commandIndex)
		server.mu.Unlock()
	}()

}

func (server *Server) Put(args *PutArgs, reply *PutReply) {
	commandIndex, _, isLeader := server.rf.StartAppendCommandInLeader(
		Operation{
			Key:    args.Key,
			Value:  args.Value,
			OpType: OpPut,
		})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	server.mu.Lock()
	notifyCh := server.getNotifyChannel(commandIndex)
	server.mu.Unlock()

	select {
	case resp := <-notifyCh:
		reply.Err = resp.Err
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrTimeout
	}

	defer func() {
		server.mu.Lock()
		server.deleteNotifyChannel(commandIndex)
		server.mu.Unlock()
	}()
}

func (server *Server) Append(args *PutArgs, reply *PutReply) {
	commandIndex, _, isLeader := server.rf.StartAppendCommandInLeader(
		Operation{
			Key:    args.Key,
			Value:  args.Value,
			OpType: OpAppend,
		})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	server.mu.Lock()
	notifyCh := server.getNotifyChannel(commandIndex)
	server.mu.Unlock()

	select {
	case resp := <-notifyCh:
		reply.Err = resp.Err
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrTimeout
	}

	defer func() {
		server.mu.Lock()
		server.deleteNotifyChannel(commandIndex)
		server.mu.Unlock()
	}()
}

func (server *Server) Kill() {
	atomic.StoreInt32(&server.dead, 1)
	server.rf.Kill()
}

func (server *Server) Killed() bool {
	z := atomic.LoadInt32(&server.dead)
	return z == 1
}

func (server *Server) getNotifyChannel(i int) chan *OperationReply {
	if _, ok := server.notifyChs[i]; !ok {
		server.notifyChs[i] = make(chan *OperationReply, 1)
	}
	return server.notifyChs[i]
}

func (server *Server) deleteNotifyChannel(i int) {
	delete(server.notifyChs, i)
}

// background thread to accept msg from application layer through applyCh
// then send it to state machine to store the value
func (server *Server) applyTicker() {
	for !server.Killed() {
		select {
		case msg := <-server.applyCh:
			if !msg.CommandValid {
				break
			}

			server.mu.Lock()

			// message has been served
			if msg.CommandIndex <= server.lastApplied {
				break
			}
			server.lastApplied = msg.CommandIndex

			op := msg.Command.(Operation)
			var opReply *OperationReply

			switch op.OpType {
			case OpGet:
				opReply.Value, opReply.Err = server.stateMachine.Get(op.Key)
			case OpPut:
				opReply.Err = server.stateMachine.Put(op.Key, op.Value)
			case OpAppend:
				opReply.Err = server.stateMachine.Append(op.Key, op.Value)
			default:
				panic("unknown operation type")
			}

			if _, isLeader := server.rf.GetState(); isLeader {
				// notify client
				notifyCh := server.getNotifyChannel(msg.CommandIndex)
				notifyCh <- opReply
			}

			server.mu.Unlock()
		}
	}
}

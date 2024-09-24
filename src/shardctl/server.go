package shardctl

import (
	"raft-kv/encoding"
	"raft-kv/raft"
	"raft-kv/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMessage
	dead    int32

	notifyChs      map[int]chan *OperationReply
	stateMachine   *StateMachine
	lastApplied    int
	duplicateTable map[int64]*LastReply // clientId -> seqId to check the duplicate log entry
}

func NewShardController(clientEnds []*rpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	encoding.Register(Operation{})

	ctler := &ShardCtrler{
		mu:           sync.Mutex{},
		me:           me,
		applyCh:      make(chan raft.ApplyMessage),
		dead:         0,
		notifyChs:    make(map[int]chan *OperationReply),
		stateMachine: NewStateMachine(),
	}
	ctler.applyCh = make(chan raft.ApplyMessage)
	ctler.rf = raft.NewRaft(clientEnds, me, persister, ctler.applyCh)

	go ctler.applyTicker()

	return ctler
}

func (ctler *ShardCtrler) Query(args QueryArgs, reply *QueryReply) {
	queryReply := OperationReply{}
	ctler.command(Operation{
		OpType:   OpQuery,
		Num:      args.Num,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &queryReply)

	reply.Err = queryReply.Err
	reply.Config = queryReply.CtlConfig
}

func (ctler *ShardCtrler) Join(args JoinArgs, reply *JoinReply) {
	queryReply := OperationReply{}
	ctler.command(Operation{
		OpType:   OpJoin,
		Servers:  args.Servers,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &queryReply)

	reply.Err = queryReply.Err
}

func (ctler *ShardCtrler) Leave(args LeaveArgs, reply *LeaveReply) {
	queryReply := OperationReply{}
	ctler.command(Operation{
		OpType:   OpLeave,
		GIDs:     args.GIDs,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &queryReply)

	reply.Err = queryReply.Err
}

func (ctler *ShardCtrler) Move(args MoveArgs, reply *MoveReply) {
	queryReply := OperationReply{}
	ctler.command(Operation{
		OpType:   OpMove,
		Shard:    args.Shard,
		GID:      args.GID,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &queryReply)

	reply.Err = queryReply.Err
}

func (ctler *ShardCtrler) command(args Operation, reply *OperationReply) {
	if args.OpType != OpQuery {
		ctler.mu.Lock()
		if ctler.isDuplicateOp(args.ClientId, args.SeqId) {
			last, _ := ctler.duplicateTable[args.ClientId]
			reply.Err = last.reply.Err
			ctler.mu.Unlock()
			return
		}
		ctler.mu.Unlock()
	}

	commandIndex, _, isLeader := ctler.rf.StartAppendCommandInLeader(args)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ctler.mu.Lock()
	notifyCh := ctler.getNotifyChannel(commandIndex)
	ctler.mu.Unlock()

	select {
	case resp := <-notifyCh:
		if args.OpType == OpQuery {
			reply.CtlConfig = resp.CtlConfig
		}
		reply.Err = resp.Err
	case <-time.After(TimeOutInterval):
		reply.Err = ErrTimeout
	}

	go func() {
		ctler.mu.Lock()
		ctler.deleteNotifyChannel(commandIndex)
		ctler.mu.Unlock()
	}()
}

func (ctler *ShardCtrler) applyTicker() {
	for !ctler.killed() {
		select {
		case msg := <-ctler.applyCh:
			if !msg.CommandValid {
				break
			}

			ctler.mu.Lock()

			// message has been served
			if msg.CommandIndex <= ctler.lastApplied {
				break
			}
			ctler.lastApplied = msg.CommandIndex

			op := msg.Command.(Operation)
			var opReply = &OperationReply{}

			switch op.OpType {
			case OpQuery:
				conf, err := ctler.stateMachine.Query(op.Num)
				opReply.CtlConfig = conf
				opReply.Err = err
			case OpJoin:
				if ctler.isDuplicateOp(op.ClientId, op.SeqId) {
					opReply = ctler.duplicateTable[op.ClientId].reply
				} else {
					opReply.Err = ctler.stateMachine.Join(op.Servers)
				}
			case OpLeave:
				if ctler.isDuplicateOp(op.ClientId, op.SeqId) {
					opReply = ctler.duplicateTable[op.ClientId].reply
				} else {
					opReply.Err = ctler.stateMachine.Leave(op.GIDs)
				}
			case OpMove:
				if ctler.isDuplicateOp(op.ClientId, op.SeqId) {
					opReply = ctler.duplicateTable[op.ClientId].reply
				} else {
					opReply.Err = ctler.stateMachine.Move(op.Shard, op.GID)
				}
			default:
				panic("unknown operation type")
			}

			if _, isLeader := ctler.rf.GetState(); isLeader {
				// notify client
				notifyCh := ctler.getNotifyChannel(msg.CommandIndex)
				notifyCh <- opReply
			}

			ctler.mu.Unlock()
		}
	}
}

func (ctler *ShardCtrler) Kill() {
	atomic.StoreInt32(&ctler.dead, 1)
	ctler.rf.Kill()
}

func (ctler *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&ctler.dead)
	return z == 1
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (ctler *ShardCtrler) getNotifyChannel(i int) chan *OperationReply {
	if _, ok := ctler.notifyChs[i]; !ok {
		ctler.notifyChs[i] = make(chan *OperationReply, 1)
	}
	return ctler.notifyChs[i]
}

func (ctler *ShardCtrler) isDuplicateOp(clientId, seqId int64) bool {
	reply, ok := ctler.duplicateTable[clientId]
	return ok && reply.seqId >= seqId
}

func (ctler *ShardCtrler) deleteNotifyChannel(i int) {
	delete(ctler.notifyChs, i)
}

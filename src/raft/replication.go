package raft

import "time"

// AppendEntries Invoked by leader to replicate log entries
// And also used for heart beat

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// start a heart beat to all followers from leader
func (rf *Raft) replicateTicker(term int) {
	for !rf.killed() {
		if ok := rf.startReplicateLogEntries(rf.currentTerm); !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		SysLog(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}

	rf.becomeFollowerNoLock(args.Term)

	reply.Success = true
	rf.resetElectionTimerNoLock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startReplicateLogEntries(term int) bool {

	sendLogEntriesToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		if !ok || !reply.Success {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerNoLock(reply.Term)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextChangedNoLock(term, Leader) {
		SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort startReplicateLogEntries in T%d", rf.currentTerm)
		return false
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}

		go sendLogEntriesToPeer(i, args)
	}

	return true
}

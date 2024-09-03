package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Command      interface{}
	CommandValid bool
	Term         int
}

// AppendEntriesArgs Invoked by leader to replicate log entries
// And also used for heart beat
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // Get from leader's nextIndex
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int // follower to notify leader the term it should be rollback to in leader's nextIndex
	ConflictIndex int
}

func (aea AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev Log:T%d, (%d, %d], CommitIdx: %d",
		aea.LeaderId, aea.Term,
		aea.PrevLogTerm, aea.PrevLogIndex, aea.PrevLogIndex+len(aea.Entries),
		aea.LeaderCommit)
}

func (aea AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success: %v, ConflictTerm: [%d]T%d", aea.Term, aea.Success, aea.ConflictIndex, aea.ConflictTerm)
}

// start a log entry and heart beat to all followers from leader at the moment a raft is promoted
// Should pass term while raft is being selected as leader, which should be the same for the whole period of sending log entries
func (rf *Raft) replicateTicker(term int) {
	for !rf.killed() {
		if ok := rf.startReplicateLogEntries(term); !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}

// background thread to sync up the log entries for a raft server
func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// release current mutex lock, and wait other signal to wake up current thread
		rf.applyCond.Wait()

		SysLog(rf.me, rf.currentTerm, DInfo, "Receive new log [%d, %d]", rf.lastApplied, rf.commitIndex)

		// get new updated log entries
		logEntries := make([]LogEntry, 0)
		for i := rf.lastApplied; i <= rf.commitIndex; i++ {
			logEntries = append(logEntries, rf.log[i])
		}
		rf.mu.Unlock()

		for i, entry := range logEntries {
			// send to follower to update the log entries
			rf.applyCh <- ApplyMessage{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + i,
			}
		}

		rf.mu.Lock()
		SysLog(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(logEntries))
		rf.lastApplied += len(logEntries)
		rf.mu.Unlock()
	}
}

// AppendEntries peer receive append log entries rpc from leader, also used for heart beat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	SysLog(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		SysLog(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}

	rf.becomeFollowerNoLock(args.Term)

	// ensure this follower won't start an election during this interval
	defer func() {
		rf.resetElectionTimerNoLock()
		if !reply.Success {
			SysLog(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			SysLog(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.logTermString())
		}
	}()

	// check if log matched
	if args.PrevLogIndex >= len(rf.log) {
		// 1. in the case, leader's log is much longer than this follower's log
		// then this candidate's term is valid, because it's far behind leader
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = len(rf.log)

		SysLog(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log is too short, Len:%d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// 2. in the case, follower's log is longer, but with different term from leader's
		// then this follower should align with leader's log, and ignore all the logs which beyond leader's
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.getStartLogIndexForATerm(rf.log[args.PrevLogIndex].Term)

		SysLog(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log term not matched, [%d]T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	reply.Success = true

	// update follower's log
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	SysLog(rf.me, rf.currentTerm, DLog2, "Follower append logs: [%d, %d]", args.PrevLogIndex+1, len(rf.log))

	rf.persist()

	// leader asks to update commit index
	if args.LeaderCommit > rf.commitIndex {
		SysLog(rf.me, rf.currentTerm, DApply, "Follower update commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit

		// avoid the leader's commit index over current log end
		if rf.commitIndex >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		}
		// to wake up the follower's apply msg signal, this is the moment peer to sync up apply message
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Leader send replicate log to peers, leader could only become follower and return false to end its replicate ticker
func (rf *Raft) startReplicateLogEntries(term int) bool {

	sendLogEntriesToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		// To avoid race condition for return 'ok' while taking another PRC call
		if !ok {
			SysLog(rf.me, rf.currentTerm, DLog1, "-> S%d, sendLogEntriesToPeer Lost or crashed", peer)
			return
		}

		SysLog(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		if rf.contextChangedNoLock(term, Leader) {
			SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort sendLogEntriesToPeer in T%d", rf.currentTerm)
			return
		}

		if !reply.Success {
			// follower's term is larger than leader's team, abort
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerNoLock(reply.Term)
				return
			}

			// append log entry failed, then to find a possible start index in next
			prevNext := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				// 1. follower's log is far behind current leader's log, send follower's last log index
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				// 2. follower's log is larger but with different term
				startTermIndex := rf.getStartLogIndexForATerm(reply.ConflictTerm)
				if startTermIndex != InvalidLogIndex {
					// leader find specific candidate's term index
					rf.nextIndex[peer] = startTermIndex
				} else {
					// leader didn't find the candidate's term in log, get it from follower's conflict index
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}

			// avoid concurrent reply to move nextIndex forward
			if rf.nextIndex[peer] > prevNext {
				rf.nextIndex[peer] = prevNext
			}

			SysLog(rf.me, rf.currentTerm, DLog1, "-> S%d, Log not match Prev=[%d]T%d, Update Next Prev=[%d]%Td", peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term)

			SysLog(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.logTermString())

			return
		}

		// update match index after replicate log to peers
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		midLogIndex := rf.getMidMatchIndex()
		// update commit index after most of the peers have passed the leader's commit index and apply log's term equals to current leader's term
		if midLogIndex > rf.commitIndex && rf.log[midLogIndex].Term == rf.currentTerm {
			SysLog(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d -> %d", rf.commitIndex, midLogIndex)
			rf.commitIndex = midLogIndex
			// To signal leader apply msg ticker
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Replicate func might return first before RPC execution finish, so we could check the context while next tick trigger
	if rf.contextChangedNoLock(term, Leader) {
		SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort startReplicateLogEntries in T%d", rf.currentTerm)
		return false
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log) - 1
			rf.nextIndex[i] = len(rf.log)
			continue
		}

		// This is the place to use nextIndex, which is passed to follower to notify its log index
		prevIndex := rf.nextIndex[i] - 1
		prevTerm := rf.log[prevIndex].Term

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      append([]LogEntry(nil), rf.log[prevIndex+1:]...), // To copy the entries, otherwise it pass the reference to the inner thread, whose data is in critical section
			LeaderCommit: rf.commitIndex,
		}

		SysLog(rf.me, rf.currentTerm, DDebug, "-> S%d, Send log: %v", i, args.String())

		go sendLogEntriesToPeer(i, args)
	}

	return true
}

// This is the place to use match index, which is going to calc leader's commit match index
func (rf *Raft) getMidMatchIndex() int {
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	mid := (len(tmp) - 1) / 2

	SysLog(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, mid[%d]=%d", tmp, mid, tmp[mid])
	return tmp[mid]
}

func (rf *Raft) getStartLogIndexForATerm(term int) int {
	for i, entry := range rf.log {
		if entry.Term == term {
			return i
		} else if entry.Term > term {
			break
		}
	}
	return InvalidLogIndex
}

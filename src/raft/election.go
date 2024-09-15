package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// RequestVoteArgs Invoked by candidate to gather votes
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d T%d, Last Log:[%d]T%d", rva.CandidateId, rva.Term, rva.LastLogIndex, rva.LastLogTerm)
}

func (rvr *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, Vote Granted: %v", rvr.Term, rvr.VoteGranted)
}

// background thread from a raft server to start an election
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutNoLock() {
			// become a Candidate from Follower or Original Candidate
			rf.becomeCandidateNoLock()
			// since outside layer hold a lock, we async call a startElectionFromCandidate
			go rf.startElectionFromCandidate(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + rand.Int63n(300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// RequestVote RPC handler. Other candidate ask vote from current raft user,
// raft user might change context based on input
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	SysLog(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Reject vote: higher Term T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if rf.currentTerm < args.Term {
		// update term and current raft become a follower, reset votedFor to NotVoted
		rf.becomeFollowerNoLock(args.Term)
	} else if rf.votedFor != args.CandidateId {
		// Term equal, but want to vote for different candidate
		SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Reject vote: already vote S%d", args.CandidateId, rf.votedFor)
		return
	}

	// check candidate's log is more update to date
	if rf.isLogMoreUpdateToDateNoLock(args.LastLogTerm, args.LastLogIndex) {
		SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Reject vote: Candidate log less update-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	rf.persist()
	rf.resetElectionTimerNoLock()
	SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Vote granted", rf.votedFor)
}

// Send a RequestVote RPC from candidate to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../rpc/rpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// return whether Rpc call succeed or not
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElectionFromCandidate(term int) {
	// always vote for Candidate self
	votes := 1

	// To check if Candidate could get a vote from its peer
	requestVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			SysLog(rf.me, rf.currentTerm, DLog1, "-> S%d, requestVoteFromPeer Lost or crashed", peer)
			return
		}

		SysLog(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote reply: %v", peer, reply.String())

		// reply user Term is larger, to become follower for current Candidate
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerNoLock(reply.Term)
			return
		}

		if !reply.VoteGranted {
			return
		}

		// check raft is still a Candidate and term not changed while it's requesting
		if rf.contextChangedNoLock(term, Candidate) {
			SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort requestVoteFromPeer for T%d, Role:%s", rf.currentTerm, getRole(rf.role))
			return
		}

		votes++

		if votes > len(rf.peers)/2 {
			rf.becomeLeaderNoLock()
			go rf.replicateTicker(term)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextChangedNoLock(term, Candidate) {
		SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort startElectionFromCandidate in T%d", rf.currentTerm)
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: rf.log.LastIndex(),
			LastLogTerm:  rf.log.LastTerm(),
		}

		SysLog(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, args=%v", i, args.String())
		go requestVoteFromPeer(i, args)
	}
}

func (rf *Raft) resetElectionTimerNoLock() {
	rf.electionStart = time.Now()

	timeoutRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(timeoutRange))
}

func (rf *Raft) isElectionTimeoutNoLock() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) isLogMoreUpdateToDateNoLock(candidateTerm, candidateLogIndex int) bool {
	lastLogIndex, lastLogTerm := rf.log.LastIndex(), rf.log.LastTerm()

	SysLog(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastLogIndex, lastLogTerm, candidateLogIndex, candidateTerm)

	if lastLogTerm == candidateTerm {
		return lastLogIndex > candidateLogIndex
	} else {
		return lastLogTerm > candidateTerm
	}
}

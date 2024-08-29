package raft

// raft
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMessage
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMessage to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/encoding"
	"course/rpc"
)

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister *Persister       // Object to hold this peer's persisted state
	me        int              // this peer's index into peers[]
	dead      int32            // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role            Role
	currentTerm     int
	votedFor        int // -1 means vote for none
	electionStart   time.Time
	electionTimeout time.Duration // random
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMessage to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMessage contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type ApplyMessage struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// NewRaft the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func NewRaft(peers []*rpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMessage) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		role:        Follower,
		currentTerm: 0,
		votedFor:    NotVoted,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := encoding.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftState := w.Bytes()
	// rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := encoding.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// RequestVote Invoked by candidate to gather votes

type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler. Other candidate ask vote from current raft user
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Reject vote: higher Term T%d>T%d", rf.me, rf.currentTerm, args.Term)
		return
	}

	if rf.currentTerm < args.Term {
		// update term and current raft become a follower, reset votedFor to NotVoted
		rf.becomeFollowerNoLock(args.Term)
	} else if rf.votedFor != args.CandidateId {
		// Term equal, but want to vote for different candidate
		SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Reject vote: already vote S%d", rf.me, rf.votedFor)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerNoLock()
	SysLog(rf.me, rf.currentTerm, DVote, "-> S%d. Vote granted", rf.me)
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
		if !ok || !reply.VoteGranted {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		// raft user Term is larger, to become follower for current Candidate
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerNoLock(reply.Term)
			return
		}

		// check raft is still a Candidate and term not changed while it's requesting
		if rf.contextChangedNoLock(term, Candidate) {
			SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort requestVoteFromPeer in T%d", rf.currentTerm)
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
			Term:        term,
			CandidateId: rf.me,
		}

		go requestVoteFromPeer(i, args)
	}
}

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

	if rf.contextChangedNoLock(term, Candidate) {
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

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (PartB).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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

// start a heart beat to all followers from leader
func (rf *Raft) replicateTicker(term int) {
	for !rf.killed() {
		if ok := rf.startReplicateLogEntries(rf.currentTerm); !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}

func (rf *Raft) becomeFollowerNoLock(term int) bool {
	if rf.currentTerm > term {
		SysLog(rf.me, rf.currentTerm, DError, "Can't become follower, lower term is: %d, current term is: %d", term, rf.currentTerm)
		return false
	}

	SysLog(rf.me, rf.currentTerm, DLog, "%s->follower, for T%s->T%s", getRole(rf.role), rf.currentTerm, term)

	rf.role = Follower
	// reset vote while update term
	if rf.currentTerm < term {
		rf.votedFor = NotVoted
	}
	rf.currentTerm = term
	return true
}

func (rf *Raft) becomeCandidateNoLock() bool {
	if rf.role == Leader {
		SysLog(rf.me, rf.currentTerm, DError, "Leader can't become candidate")
		return false
	}

	SysLog(rf.me, rf.currentTerm, DLog, "%s->candidate, for T%s", getRole(rf.role), rf.currentTerm+1)

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me

	return true
}

func (rf *Raft) becomeLeaderNoLock() bool {
	if rf.role != Candidate {
		SysLog(rf.me, rf.currentTerm, DError, "Only candidate can become leader")
		return false
	}

	SysLog(rf.me, rf.currentTerm, DLog, "leader, for T%s", rf.currentTerm+1)

	rf.role = Leader

	return true
}

func (rf *Raft) resetElectionTimerNoLock() {
	rf.electionStart = time.Now()

	timeoutRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(timeoutRange))
}

func (rf *Raft) isElectionTimeoutNoLock() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// To check context if other raft send request to change current raft context, and it's approved by cur raft
func (rf *Raft) contextChangedNoLock(term int, role Role) bool {
	return rf.currentTerm != term || rf.role != role
}

package raft

// raft
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.StartAppendCommandInLeader(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMessage
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMessage to the service (or tester)
//   in the same server.
//

import (
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

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role Role
	// persist state on all servers
	currentTerm int
	votedFor    int        // -1 means vote for none
	log         []LogEntry // each entry contain command for state machine, and term when entry was received by leader (first index is 1, has a dummy head)

	// volatile state on leader, reinitialize after election
	nextIndex  []int // index of next log entry to send to that server, init = leader last log index + 1, which may not succeed on sync on the peer
	matchIndex []int // index of highest log entry known to be replicated, which means it succeed on sync

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed, init at 0
	lastApplied int // index of highest log entry applied to state machine, init at 0
	applyCond   *sync.Cond
	applyCh     chan ApplyMessage

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
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		role:        Follower,
		currentTerm: InitialTerm,
		votedFor:    NotVoted,
		log:         []LogEntry{},
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		commitIndex: InvalidLogIndex,
		lastApplied: InvalidLogIndex,
		applyCh:     applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	// append a dummy log entry
	rf.log = append(rf.log, LogEntry{Term: InvalidTerm})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	// apply log goroutine
	go rf.applyTicker()

	return rf
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// StartAppendCommandInLeader the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) StartAppendCommandInLeader(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return InvalidLogIndex, InvalidTerm, false
	}

	rf.log = append(rf.log, LogEntry{
		Command:      command,
		CommandValid: true,
		Term:         rf.currentTerm,
	})
	SysLog(rf.me, rf.currentTerm, DLeader, "Leader accept log: [%d]T%d", len(rf.log)-1, rf.currentTerm)

	rf.persist()

	return len(rf.log) - 1, rf.currentTerm, true
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

func (rf *Raft) becomeFollowerNoLock(term int) {
	if rf.currentTerm > term {
		SysLog(rf.me, rf.currentTerm, DError, "Can't become follower, lower term is: %d, current term is: %d", term, rf.currentTerm)
		return
	}

	SysLog(rf.me, rf.currentTerm, DInfo, "%s->follower, for T%d->T%d", getRole(rf.role), rf.currentTerm, term)

	rf.role = Follower
	shouldPersist := rf.currentTerm != term
	// reset vote while update term
	if rf.currentTerm < term {
		rf.votedFor = NotVoted
	}
	rf.currentTerm = term

	if shouldPersist {
		rf.persist()
	}
	return
}

func (rf *Raft) becomeCandidateNoLock() {
	if rf.role == Leader {
		SysLog(rf.me, rf.currentTerm, DError, "Leader can't become candidate")
		return
	}

	SysLog(rf.me, rf.currentTerm, DInfo, "%s->candidate, for T%d", getRole(rf.role), rf.currentTerm+1)

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me

	rf.persist()
	return
}

func (rf *Raft) becomeLeaderNoLock() {
	if rf.role != Candidate {
		SysLog(rf.me, rf.currentTerm, DError, "Only candidate can become leader")
		return
	}

	SysLog(rf.me, rf.currentTerm, DInfo, "Leader, for T%d", rf.currentTerm)

	rf.role = Leader

	// volatile state
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) // First leader's nextIndex will be 1
		rf.matchIndex[i] = InvalidLogIndex
	}

	return
}

// To check context if other raft send request to change current raft context, and it's approved by cur raft
func (rf *Raft) contextChangedNoLock(term int, role Role) bool {
	return rf.currentTerm != term || rf.role != role
}

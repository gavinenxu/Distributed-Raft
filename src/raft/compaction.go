package raft

import "fmt"

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.snapshotFromIndex(index, snapshot)
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term         int
	LeaderId     int
	LastLogIndex int
	LastLogTerm  int
	Snapshot     []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// InstallSnapshot rpc call from leader to follower to install snapshot in log
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	SysLog(rf.me, rf.currentTerm, DSnap, "<- S%d, Receive snapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		SysLog(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject snapshot", args.LeaderId)
		return
	}

	rf.becomeFollowerNoLock(args.Term)

	// ensure this follower won't start an election during this interval
	defer func() {
		rf.resetElectionTimerNoLock()
	}()

	if rf.log.snapLastIndex >= args.LastLogIndex {
		SysLog(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject snapshot, Already Installed: %d>%d", args.LeaderId, rf.log.snapLastIndex, args.LastLogIndex)
		return
	}

	// install snapshot in memory
	rf.log.installSnapshot(args.LastLogIndex, args.LastLogTerm, args.Snapshot)
	// persist it on disk
	rf.persist()
	// apply to application layer
	rf.snapshotApply = true

	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) installSnapshotOnPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// To avoid race condition for return 'ok' while taking another PRC call
	if !ok {
		SysLog(rf.me, rf.currentTerm, DLog1, "-> S%d, installSnapshotOnPeer Lost or crashed", peer)
		return
	}

	SysLog(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

	if rf.contextChangedNoLock(term, Leader) {
		SysLog(rf.me, rf.currentTerm, DVote, "Lost context, abort installSnapshotOnPeer in T%d", rf.currentTerm)
		return
	}

	// update match and next index
	if args.LastLogIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastLogIndex
		rf.nextIndex[peer] = args.LastLogIndex + 1
	}

	// no need to update commit index

}

package raft

import (
	"fmt"
	"raft-kv/encoding"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int

	snapshot []byte
	tailLog  []LogEntry // save a dummy logEntry {term: snapLastTerm} as the head
}

func NewRaftLog(snapLastIndex, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	raftLog := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}

	raftLog.tailLog = make([]LogEntry, 0, 1+len(entries))
	// set a dummy head for tail log, start from snap last term
	raftLog.tailLog = append(raftLog.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	raftLog.tailLog = append(raftLog.tailLog, entries...)

	return raftLog
}

// Size Total size of logical log not including dummy head
func (raftLog *RaftLog) size() int {
	return raftLog.snapLastIndex + len(raftLog.tailLog)
}

// LastIndex Last index of logical log not including dummy
func (raftLog *RaftLog) lastIndex() int {
	return raftLog.size() - 1
}

// LastTerm Last Term of logical log including dummy
func (raftLog *RaftLog) lastTerm() int {
	return raftLog.tailLog[len(raftLog.tailLog)-1].Term
}

// Encode raft log
func (raftLog *RaftLog) encode(enc *encoding.Encoder) error {
	if err := enc.Encode(raftLog.snapLastIndex); err != nil {
		return err
	}

	if err := enc.Encode(raftLog.snapLastTerm); err != nil {
		return err
	}

	if err := enc.Encode(raftLog.tailLog); err != nil {
		return err
	}

	return nil
}

// Decode raft log
func (raftLog *RaftLog) decode(dec *encoding.Decoder) error {
	var snapLastIndex int
	if err := dec.Decode(&snapLastIndex); err != nil {
		return err
	}
	raftLog.snapLastIndex = snapLastIndex

	var snapLastTerm int
	if err := dec.Decode(&snapLastTerm); err != nil {
		return err
	}
	raftLog.snapLastTerm = snapLastTerm

	var tailLog []LogEntry
	if err := dec.Decode(&tailLog); err != nil {
		return err
	}
	raftLog.tailLog = tailLog

	return nil
}

// Append new log entry to the tail
func (raftLog *RaftLog) append(entry LogEntry) {
	raftLog.tailLog = append(raftLog.tailLog, entry)
}

func (raftLog *RaftLog) getLogAtIndex(index int) LogEntry {
	idx := raftLog.logIndex(index)
	return raftLog.tailLog[idx]
}

func (raftLog *RaftLog) getStartIndexForATerm(term int) int {
	for i, entry := range raftLog.tailLog {
		if entry.Term == term {
			return i + raftLog.snapLastIndex
		}
		if entry.Term > term {
			break
		}
	}
	return InvalidLogIndex
}

func (raftLog *RaftLog) appendEntriesAfterIndex(index int, entries []LogEntry) {
	idx := raftLog.logIndex(index)
	// To exclude index in following tail log, avoid index out of range above
	raftLog.tailLog = append(raftLog.tailLog[:idx+1], entries...)
}

func (raftLog *RaftLog) getLogsFromIndex(index int) []LogEntry {
	idx := raftLog.logIndex(index)
	return raftLog.tailLog[idx:]
}

// SnapshotFromIndex leader to install itself snapshot from application layer
func (raftLog *RaftLog) snapshotFromIndex(index int, snapshot []byte) {
	idx := raftLog.logIndex(index)

	raftLog.snapshot = snapshot
	raftLog.snapLastIndex = index
	raftLog.snapLastTerm = raftLog.tailLog[idx].Term

	newLog := make([]LogEntry, 0, raftLog.size()-raftLog.snapLastIndex)
	// append the dummy log entry as head
	newLog = append(newLog, LogEntry{
		Term: raftLog.tailLog[idx].Term,
	})
	// take next index to build tail log
	newLog = append(newLog, raftLog.tailLog[idx+1:]...)

	raftLog.tailLog = newLog
}

// InstallSnapshot follower get snapshot from leader's log,
// and store all the leader's snapshot then init new log in follower
func (raftLog *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	raftLog.snapLastIndex = index
	raftLog.snapLastTerm = term
	raftLog.snapshot = snapshot

	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: term,
	})
	raftLog.tailLog = newLog
}

func (raftLog *RaftLog) readSnapshot() []byte {
	return raftLog.snapshot
}

func (raftLog *RaftLog) restoreSnapshot(snapshot []byte) {
	raftLog.snapshot = snapshot
}

// if it's out of range, return start index
func (raftLog *RaftLog) logIndex(logicIndex int) int {
	if logicIndex < raftLog.snapLastIndex || logicIndex > raftLog.lastIndex() {
		panic(fmt.Sprintf("index:%d is out of range [%d, %d]", logicIndex, raftLog.snapLastIndex, raftLog.lastIndex()))
	}
	return logicIndex - raftLog.snapLastIndex
}

// print out raft log's info
func (raftLog *RaftLog) String() string {
	var info string
	prevTerm := raftLog.snapLastTerm
	prevStart := 0
	for i := 1; i < len(raftLog.tailLog); i++ {
		if raftLog.tailLog[i].Term != prevTerm {
			info += fmt.Sprintf(" [%d, %d]T%d", prevStart+raftLog.snapLastIndex, i-1+raftLog.snapLastIndex, prevTerm)
			prevTerm = raftLog.tailLog[i].Term
			prevStart = i
		}
	}
	info += fmt.Sprintf(" [%d, %d]T%d", prevStart+raftLog.snapLastIndex, raftLog.lastIndex(), prevTerm)
	return info
}

package raft

import (
	"bytes"
	"fmt"
	"raft-kv/encoding"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	enc := encoding.NewEncoder(writer)
	_ = enc.Encode(rf.currentTerm)
	_ = enc.Encode(rf.votedFor)
	_ = rf.log.Encode(enc)
	raftState := writer.Bytes()
	rf.persister.Save(raftState, nil)

	SysLog(rf.me, rf.currentTerm, DPersist, "Write Persist: %v", rf.persistStateString())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	reader := bytes.NewBuffer(data)
	dec := encoding.NewDecoder(reader)
	var currentTerm int
	var votedFor int

	if err := dec.Decode(&currentTerm); err != nil {
		SysLog(rf.me, rf.currentTerm, DError, "failed to read current term error: %v", err)
		return
	}
	rf.currentTerm = currentTerm

	if err := dec.Decode(&votedFor); err != nil {
		SysLog(rf.me, rf.currentTerm, DError, "failed to read voted for error: %v", err)
		return
	}
	rf.votedFor = votedFor

	if err := rf.log.Decode(dec); err != nil {
		SysLog(rf.me, rf.currentTerm, DError, "failed to read log error: %v", err)
		return
	}

	SysLog(rf.me, rf.currentTerm, DPersist, "Read Persist: %v", rf.persistStateString())
}

func (rf *Raft) persistStateString() string {
	return fmt.Sprintf("T%d, Voted for: S%d, log length: %d", rf.currentTerm, rf.votedFor, rf.log.Size())
}

package raft

// background thread to sync up the log entries for a raft server
func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// release current mutex lock, and wait other signal to wake up current thread
		rf.applyCond.Wait()

		SysLog(rf.me, rf.currentTerm, DInfo, "Receive new log [%d, %d]", rf.lastApplied, rf.commitIndex)

		// get new updated log entries
		logEntries := make([]LogEntry, 0)

		snapshotApply := rf.snapshotApply
		if !snapshotApply {
			for i := rf.lastApplied; i <= rf.commitIndex; i++ {
				logEntries = append(logEntries, rf.log.getLogAtIndex(i))
			}
		}

		rf.mu.Unlock()

		if !snapshotApply {
			for i, entry := range logEntries {
				// send back to upper layer (the user of raft servers), then update state machine
				rf.applyCh <- ApplyMessage{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + i,
				}
			}
		} else {
			rf.applyCh <- ApplyMessage{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotTerm:  rf.log.snapLastTerm,
				SnapshotIndex: rf.log.snapLastIndex,
			}
		}

		rf.mu.Lock()

		if !snapshotApply {
			SysLog(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(logEntries))
			rf.lastApplied += len(logEntries)
		} else {
			SysLog(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", rf.log.snapLastIndex)
			rf.lastApplied = rf.log.snapLastIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapshotApply = false
		}

		rf.mu.Unlock()
	}
}

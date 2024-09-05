package raft

//
// support for Raft tester.
//

import (
	"bytes"
	"log"
	"math/rand"
	"raft-kv/encoding"
	"raft-kv/rpc"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"time"
)

func randString(n int) string {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	maxInt := big.NewInt(int64(1) << 62)
	bigX, _ := crand.Int(crand.Reader, maxInt)
	x := bigX.Int64()
	return x
}

type tester struct {
	mu          sync.Mutex
	t           *testing.T
	finished    int32
	net         *rpc.Network
	n           int
	rafts       []*Raft
	applyErr    []string // from apply channel readers
	connected   []bool   // whether each server is on the net
	saved       []*Persister
	endNames    [][]string            // the port file names each sends to
	logs        []map[int]interface{} // copy of each server's committed entries
	lastApplied []int
	start       time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0        time.Time // time at which raft_test.go called tr.begin()
	rpcS0     int       // rpcTotal() at start of test
	cmdS0     int       // number of agreements
	bytes0    int64
	maxIndex  int
	maxIndex0 int
}

var nCpuOnce sync.Once

// In application layer to set up net and raft servers, then connect raft servers
func newTester(t *testing.T, n int, unreliable bool, snapshot bool) *tester {
	nCpuOnce.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	tr := &tester{
		t:           t,
		net:         rpc.NewNetwork(),
		n:           n,
		applyErr:    make([]string, n),
		rafts:       make([]*Raft, n),
		connected:   make([]bool, n),
		saved:       make([]*Persister, n),
		endNames:    make([][]string, n),
		logs:        make([]map[int]interface{}, n),
		lastApplied: make([]int, n),
		start:       time.Now(),
	}

	tr.setUnreliable(unreliable)

	tr.net.LongDelays(true)

	applier := tr.applier
	if snapshot {
		applier = tr.applierSnap
	}
	// create a full set of Rafts.
	for i := 0; i < tr.n; i++ {
		tr.logs[i] = map[int]interface{}{}
		tr.startRaft(i, applier)
	}

	// connect everyone
	for i := 0; i < tr.n; i++ {
		tr.connect(i)
	}

	return tr
}

// shut down a Raft server but save its persistent state.
func (tr *tester) crash(i int) {
	tr.disconnect(i)
	tr.net.DeleteServer(i) // disable client connections to the server.

	tr.mu.Lock()
	defer tr.mu.Unlock()

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if tr.saved[i] != nil {
		tr.saved[i] = tr.saved[i].Copy()
	}

	rf := tr.rafts[i]
	if rf != nil {
		tr.mu.Unlock()
		rf.Kill()
		tr.mu.Lock()
		tr.rafts[i] = nil
	}

	if tr.saved[i] != nil {
		raftlog := tr.saved[i].ReadRaftState()
		snapshot := tr.saved[i].ReadSnapshot()
		tr.saved[i] = &Persister{}
		tr.saved[i].Save(raftlog, snapshot)
	}
}

func (tr *tester) checkLogs(i int, m ApplyMessage) (string, bool) {
	errMsg := ""
	v := m.Command
	for j := 0; j < len(tr.logs); j++ {
		if old, oldOk := tr.logs[j][m.CommandIndex]; oldOk && old != v {
			log.Printf("%v: log %v; server %v\n", i, tr.logs[i], tr.logs[j])
			// some server has already committed a different value for this entry!
			errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	_, prevOk := tr.logs[i][m.CommandIndex-1]
	tr.logs[i][m.CommandIndex] = v
	if m.CommandIndex > tr.maxIndex {
		tr.maxIndex = m.CommandIndex
	}
	return errMsg, prevOk
}

// applier reads message from apply ch and checks that they match the log
// contents
func (tr *tester) applier(i int, applyCh chan ApplyMessage) {
	for m := range applyCh {
		if m.CommandValid == false {
			// ignore other types of ApplyMessage
		} else {
			tr.mu.Lock()
			errMsg, prevOk := tr.checkLogs(i, m)
			tr.mu.Unlock()
			if m.CommandIndex > 1 && prevOk == false {
				errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
			}
			if errMsg != "" {
				tr.applyErr[i] = errMsg
				log.Fatalf("apply error: %v", errMsg)
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
		}
	}
}

// returns "" or error string
func (tr *tester) ingestSnap(i int, snapshot []byte, index int) string {
	if snapshot == nil {
		log.Fatalf("nil snapshot")
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := encoding.NewDecoder(r)
	var lastIncludedIndex int
	var xLog []interface{}
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xLog) != nil {
		log.Fatalf("snapshot decode error")
		return "snapshot Decode() error"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", i)
		return err
	}
	tr.logs[i] = map[int]interface{}{}
	for j := 0; j < len(xLog); j++ {
		tr.logs[i][j] = xLog[j]
	}
	tr.lastApplied[i] = lastIncludedIndex
	return ""
}

const SnapShotInterval = 10

// periodically snapshot raft state
func (tr *tester) applierSnap(i int, applyCh chan ApplyMessage) {
	tr.mu.Lock()
	rf := tr.rafts[i]
	tr.mu.Unlock()
	if rf == nil {
		return // ???
	}

	for m := range applyCh {
		errMsg := ""
		if m.SnapshotValid {
			tr.mu.Lock()
			errMsg = tr.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
			tr.mu.Unlock()
		} else if m.CommandValid {
			if m.CommandIndex != tr.lastApplied[i]+1 {
				errMsg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", i, tr.lastApplied[i]+1, m.CommandIndex)
			}

			if errMsg == "" {
				tr.mu.Lock()
				var prevOk bool
				errMsg, prevOk = tr.checkLogs(i, m)
				tr.mu.Unlock()
				if m.CommandIndex > 1 && prevOk == false {
					errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			}

			tr.mu.Lock()
			tr.lastApplied[i] = m.CommandIndex
			tr.mu.Unlock()

			if (m.CommandIndex+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := encoding.NewEncoder(w)
				_ = e.Encode(m.CommandIndex)
				var xLog []interface{}
				for j := 0; j <= m.CommandIndex; j++ {
					xLog = append(xLog, tr.logs[i][j])
				}
				_ = e.Encode(xLog)
				rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		} else {
			// Ignore other types of ApplyMessage.
		}
		if errMsg != "" {
			tr.applyErr[i] = errMsg
			log.Fatalf("apply error: %v", errMsg)
			// keep reading after error so that Raft doesn't block
			// holding locks...
		}
	}
}

// startRaft or re-start a Raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
func (tr *tester) startRaft(i int, applier func(int, chan ApplyMessage)) {
	tr.crash(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	tr.endNames[i] = make([]string, tr.n)
	for j := 0; j < tr.n; j++ {
		tr.endNames[i][j] = randString(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*rpc.ClientEnd, tr.n)
	for j := 0; j < tr.n; j++ {
		ends[j] = tr.net.AddClientEnd(tr.endNames[i][j])
		tr.net.Connect(tr.endNames[i][j], j)
	}

	tr.mu.Lock()

	tr.lastApplied[i] = 0

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if tr.saved[i] != nil {
		tr.saved[i] = tr.saved[i].Copy()

		snapshot := tr.saved[i].ReadSnapshot()
		if snapshot != nil && len(snapshot) > 0 {
			// mimic KV server and process snapshot now.
			// ideally Raft should send it up on applyCh...
			err := tr.ingestSnap(i, snapshot, -1)
			if err != "" {
				tr.t.Fatal(err)
			}
		}
	} else {
		tr.saved[i] = MakePersister()
	}

	tr.mu.Unlock()

	applyCh := make(chan ApplyMessage)

	rf := NewRaft(ends, i, tr.saved[i], applyCh)

	tr.mu.Lock()
	tr.rafts[i] = rf
	tr.mu.Unlock()

	go applier(i, applyCh)

	svc := rpc.NewService(rf)
	srv := rpc.NewServer(i)
	srv.AddService(svc)
	tr.net.AddServer(srv)
}

func (tr *tester) checkTimeout() {
	// enforce a two minutes real-time limit on each test
	if !tr.t.Failed() && time.Since(tr.start) > 120*time.Second {
		tr.t.Fatal("test took longer than 120 seconds")
	}
}

func (tr *tester) checkFinished() bool {
	z := atomic.LoadInt32(&tr.finished)
	return z != 0
}

func (tr *tester) cleanup() {
	atomic.StoreInt32(&tr.finished, 1)
	for i := 0; i < len(tr.rafts); i++ {
		if tr.rafts[i] != nil {
			tr.rafts[i].Kill()
		}
	}
	tr.net.Cleanup()
	tr.checkTimeout()
}

// attach server i to the net.
func (tr *tester) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	tr.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < tr.n; j++ {
		if tr.connected[j] {
			endName := tr.endNames[i][j]
			tr.net.Enable(endName, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < tr.n; j++ {
		if tr.connected[j] {
			endName := tr.endNames[j][i]
			tr.net.Enable(endName, true)
		}
	}
}

// detach server i from the net.
func (tr *tester) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	tr.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < tr.n; j++ {
		if tr.endNames[i] != nil {
			endName := tr.endNames[i][j]
			tr.net.Enable(endName, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < tr.n; j++ {
		if tr.endNames[j] != nil {
			endName := tr.endNames[j][i]
			tr.net.Enable(endName, false)
		}
	}
}

func (tr *tester) rpcCount(server int) int {
	return tr.net.GetCount(server)
}

func (tr *tester) rpcTotal() int {
	return tr.net.GetTotalCount()
}

func (tr *tester) setUnreliable(isUnreliable bool) {
	tr.net.Reliable(!isUnreliable)
}

func (tr *tester) bytesTotal() int64 {
	return tr.net.GetTotalBytes()
}

func (tr *tester) setLongReordering(isLongReordering bool) {
	tr.net.LongReordering(isLongReordering)
}

// check that one of the connected servers thinks
// it is the leader, and that no other connected
// server thinks otherwise.
//
// try a few times in case re-elections are needed.
func (tr *tester) checkOneLeader() int {
	for i := 0; i < 10; i++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < tr.n; i++ {
			if tr.connected[i] {
				if term, leader := tr.rafts[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				tr.t.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	tr.t.Fatalf("expected one leader, got none")
	return -1
}

// check that everyone agrees on the term.
func (tr *tester) checkTerms() int {
	term := -1
	for i := 0; i < tr.n; i++ {
		if tr.connected[i] {
			xterm, _ := tr.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				tr.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

// check that none of the connected servers
// thinks it is the leader.
func (tr *tester) checkNoLeader() {
	for i := 0; i < tr.n; i++ {
		if tr.connected[i] {
			_, isLeader := tr.rafts[i].GetState()
			if isLeader {
				tr.t.Fatalf("expected no leader among connected servers, but %v claims to be leader", i)
			}
		}
	}
}

// how many servers think a log entry is committed?
func (tr *tester) nCommitted(index int) (int, interface{}) {
	count := 0
	var cmd interface{} = nil
	for i := 0; i < len(tr.rafts); i++ {
		if tr.applyErr[i] != "" {
			tr.t.Fatal(tr.applyErr[i])
		}

		tr.mu.Lock()
		cmd1, ok := tr.logs[i][index]
		tr.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				tr.t.Fatalf("committed values do not match: index %v, %v, %v",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// wait for at least n servers to commit.
// but don't wait forever.
func (tr *tester) wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for i := 0; i < 30; i++ {
		nd, _ := tr.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range tr.rafts {
				if t, _ := r.GetState(); t > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := tr.nCommitted(index)
	if nd < n {
		tr.t.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, may submit the command multiple
// times, in case a leader fails just after StartAppendCommandInLeader().
// if retry==false, calls StartAppendCommandInLeader() only once, in order
// to simplify the early Lab 2B tests.
func (tr *tester) one(cmd interface{}, expectedServers int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 && tr.checkFinished() == false {
		// try all the servers, maybe one is the leader.
		index := -1
		for si := 0; si < tr.n; si++ {
			starts = (starts + 1) % tr.n
			var rf *Raft
			tr.mu.Lock()
			if tr.connected[starts] {
				rf = tr.rafts[starts]
			}
			tr.mu.Unlock()
			if rf != nil {
				index1, _, ok := rf.StartAppendCommandInLeader(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := tr.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				tr.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if tr.checkFinished() == false {
		tr.t.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return -1
}

// start a Test.
// print the Test message.
// e.g. tr.begin("Test (2B): RPC counts aren't too high")
func (tr *tester) begin(description string) {
	fmt.Printf("%s ...\n", description)
	tr.t0 = time.Now()
	tr.rpcS0 = tr.rpcTotal()
	tr.bytes0 = tr.bytesTotal()
	tr.cmdS0 = 0
	tr.maxIndex0 = tr.maxIndex
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (tr *tester) end() {
	tr.checkTimeout()
	if tr.t.Failed() == false {
		tr.mu.Lock()
		t := time.Since(tr.t0).Seconds()      // real time
		nPeers := tr.n                        // number of Raft peers
		nRpc := tr.rpcTotal() - tr.rpcS0      // number of RPC sends
		nBytes := tr.bytesTotal() - tr.bytes0 // number of bytes
		nCmds := tr.maxIndex - tr.maxIndex0   // number of Raft agreements reported
		tr.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  total time: %4.1f,  peers: %d, rpc sent: %d, total bytes sent: %d, total raft agreements: %d\n", t, nPeers, nRpc, nBytes, nCmds)
	}
}

// Maximum log size across all servers
func (tr *tester) logSize() int {
	logSize := 0
	for i := 0; i < tr.n; i++ {
		n := tr.saved[i].RaftStateSize()
		if n > logSize {
			logSize = n
		}
	}
	return logSize
}

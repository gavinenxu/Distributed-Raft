package raft

//
// Raft tests.
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElectionPartA(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test: initial election")

	// is a leader elected?
	tr.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := tr.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := tr.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	tr.checkOneLeader()

	tr.end()
}

func TestReElectionPartA(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test: election after network failure")

	leader1 := tr.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	tr.disconnect(leader1)
	tr.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	tr.connect(leader1)
	leader2 := tr.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	tr.disconnect(leader2)
	tr.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	tr.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	tr.connect((leader2 + 1) % servers)
	tr.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	tr.connect(leader2)
	tr.checkOneLeader()

	tr.end()
}

func TestManyElectionsPartA(t *testing.T) {
	servers := 7
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test: multiple elections")

	tr.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		tr.disconnect(i1)
		tr.disconnect(i2)
		tr.disconnect(i3)

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		tr.checkOneLeader()

		tr.connect(i1)
		tr.connect(i2)
		tr.connect(i3)
	}

	tr.checkOneLeader()

	tr.end()
}

func TestBasicAgreePartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := tr.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before StartAppendCommandInLeader()")
		}

		xindex := tr.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	tr.end()
}

// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
func TestRPCBytesPartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): RPC byte count")

	tr.one(99, servers, false)
	bytes0 := tr.bytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := randString(5000)
		xindex := tr.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := tr.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	tr.end()
}

// test just failure of followers.
func TestFollowerFailurePartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): test progressive failure of followers")

	tr.one(101, servers, false)

	// disconnect one follower from the network.
	leader1 := tr.checkOneLeader()
	tr.disconnect((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	tr.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	tr.one(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := tr.checkOneLeader()
	tr.disconnect((leader2 + 1) % servers)
	tr.disconnect((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := tr.rafts[leader2].StartAppendCommandInLeader(104)
	if ok != true {
		t.Fatalf("leader rejected StartAppendCommandInLeader()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := tr.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	tr.end()
}

// test just failure of leaders.
func TestLeaderFailurePartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): test failure of leaders")

	tr.one(101, servers, false)

	// disconnect the first leader.
	leader1 := tr.checkOneLeader()
	tr.disconnect(leader1)

	// the remaining followers should elect
	// a new leader.
	tr.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	tr.one(103, servers-1, false)

	// disconnect the new leader.
	leader2 := tr.checkOneLeader()
	tr.disconnect(leader2)

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		tr.rafts[i].StartAppendCommandInLeader(104)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := tr.nCommitted(4)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	tr.end()
}

// test that a follower participates after
// disconnect and re-connect.
func TestFailAgreePartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): agreement after follower reconnects")

	tr.one(101, servers, false)

	// disconnect one follower from the network.
	leader := tr.checkOneLeader()
	tr.disconnect((leader + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	tr.one(102, servers-1, false)
	tr.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	tr.one(104, servers-1, false)
	tr.one(105, servers-1, false)

	// re-connect
	tr.connect((leader + 1) % servers)

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	tr.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	tr.one(107, servers, true)

	tr.end()
}

func TestFailNoAgreePartB(t *testing.T) {
	servers := 5
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): no agreement if too many followers disconnect")

	tr.one(10, servers, false)

	// 3 of 5 followers disconnect
	leader := tr.checkOneLeader()
	tr.disconnect((leader + 1) % servers)
	tr.disconnect((leader + 2) % servers)
	tr.disconnect((leader + 3) % servers)

	index, _, ok := tr.rafts[leader].StartAppendCommandInLeader(20)
	if ok != true {
		t.Fatalf("leader rejected StartAppendCommandInLeader()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := tr.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	tr.connect((leader + 1) % servers)
	tr.connect((leader + 2) % servers)
	tr.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := tr.checkOneLeader()
	index2, _, ok2 := tr.rafts[leader2].StartAppendCommandInLeader(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected StartAppendCommandInLeader()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	tr.one(1000, servers, true)

	tr.end()
}

func TestConcurrentStartsPartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): concurrent StartAppendCommandInLeader()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := tr.checkOneLeader()
		_, term, ok := tr.rafts[leader].StartAppendCommandInLeader(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := tr.rafts[leader].StartAppendCommandInLeader(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := tr.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := tr.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all StartAppendCommandInLeader()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	tr.end()
}

func TestRejoinPartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): rejoin of partitioned leader")

	tr.one(101, servers, true)

	// leader network failure
	leader1 := tr.checkOneLeader()
	tr.disconnect(leader1)

	// make old leader try to agree on some entries
	tr.rafts[leader1].StartAppendCommandInLeader(102)
	tr.rafts[leader1].StartAppendCommandInLeader(103)
	tr.rafts[leader1].StartAppendCommandInLeader(104)

	// new leader commits, also for index=2
	tr.one(103, 2, true)

	// new leader network failure
	leader2 := tr.checkOneLeader()
	tr.disconnect(leader2)

	// old leader connected again
	tr.connect(leader1)

	tr.one(104, 2, true)

	// all together now
	tr.connect(leader2)

	tr.one(105, servers, true)

	tr.end()
}

func TestBackupPartB(t *testing.T) {
	servers := 5
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): leader backs up quickly over incorrect follower logs")

	tr.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := tr.checkOneLeader()
	tr.disconnect((leader1 + 2) % servers)
	tr.disconnect((leader1 + 3) % servers)
	tr.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		tr.rafts[leader1].StartAppendCommandInLeader(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	tr.disconnect((leader1 + 0) % servers)
	tr.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	tr.connect((leader1 + 2) % servers)
	tr.connect((leader1 + 3) % servers)
	tr.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		tr.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := tr.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	tr.disconnect(other)

	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		tr.rafts[leader2].StartAppendCommandInLeader(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		tr.disconnect(i)
	}
	tr.connect((leader1 + 0) % servers)
	tr.connect((leader1 + 1) % servers)
	tr.connect(other)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		tr.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		tr.connect(i)
	}
	tr.one(rand.Int(), servers, true)

	tr.end()
}

func TestCountPartB(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartB): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += tr.rpcCount(j)
		}
		return
	}

	leader := tr.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = tr.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := tr.rafts[leader].StartAppendCommandInLeader(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := tr.rafts[leader].StartAppendCommandInLeader(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("StartAppendCommandInLeader() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := tr.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := tr.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += tr.rpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += tr.rpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	tr.end()
}

func TestPersist1PartC(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartC): basic persistence")

	tr.one(11, servers, true)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		tr.startRaft(i, tr.applier)
	}
	for i := 0; i < servers; i++ {
		tr.disconnect(i)
		tr.connect(i)
	}

	tr.one(12, servers, true)

	leader1 := tr.checkOneLeader()
	tr.disconnect(leader1)
	tr.startRaft(leader1, tr.applier)
	tr.connect(leader1)

	tr.one(13, servers, true)

	leader2 := tr.checkOneLeader()
	tr.disconnect(leader2)
	tr.one(14, servers-1, true)
	tr.startRaft(leader2, tr.applier)
	tr.connect(leader2)

	tr.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (tr.checkOneLeader() + 1) % servers
	tr.disconnect(i3)
	tr.one(15, servers-1, true)
	tr.startRaft(i3, tr.applier)
	tr.connect(i3)

	tr.one(16, servers, true)

	tr.end()
}

func TestPersist2PartC(t *testing.T) {
	servers := 5
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartC): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		tr.one(10+index, servers, true)
		index++

		leader1 := tr.checkOneLeader()

		tr.disconnect((leader1 + 1) % servers)
		tr.disconnect((leader1 + 2) % servers)

		tr.one(10+index, servers-2, true)
		index++

		tr.disconnect((leader1 + 0) % servers)
		tr.disconnect((leader1 + 3) % servers)
		tr.disconnect((leader1 + 4) % servers)

		tr.startRaft((leader1+1)%servers, tr.applier)
		tr.startRaft((leader1+2)%servers, tr.applier)
		tr.connect((leader1 + 1) % servers)
		tr.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		tr.startRaft((leader1+3)%servers, tr.applier)
		tr.connect((leader1 + 3) % servers)

		tr.one(10+index, servers-2, true)
		index++

		tr.connect((leader1 + 4) % servers)
		tr.connect((leader1 + 0) % servers)
	}

	tr.one(1000, servers, true)

	tr.end()
}

func TestPersist3PartC(t *testing.T) {
	servers := 3
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartC): partitioned leader and one follower crash, leader restarts")

	tr.one(101, 3, true)

	leader := tr.checkOneLeader()
	tr.disconnect((leader + 2) % servers)

	tr.one(102, 2, true)

	tr.crash((leader + 0) % servers)
	tr.crash((leader + 1) % servers)
	tr.connect((leader + 2) % servers)
	tr.startRaft((leader+0)%servers, tr.applier)
	tr.connect((leader + 0) % servers)

	tr.one(103, 2, true)

	tr.startRaft((leader+1)%servers, tr.applier)
	tr.connect((leader + 1) % servers)

	tr.one(104, servers, true)

	tr.end()
}

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
func TestFigure8PartC(t *testing.T) {
	servers := 5
	tr := newTester(t, servers, false, false)
	defer tr.cleanup()

	tr.begin("Test (PartC): Figure 8")

	tr.one(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if tr.rafts[i] != nil {
				_, _, ok := tr.rafts[i].StartAppendCommandInLeader(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			tr.crash(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if tr.rafts[s] == nil {
				tr.startRaft(s, tr.applier)
				tr.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if tr.rafts[i] == nil {
			tr.startRaft(i, tr.applier)
			tr.connect(i)
		}
	}

	tr.one(rand.Int(), servers, true)

	tr.end()
}

func TestUnreliableAgreePartC(t *testing.T) {
	servers := 5
	tr := newTester(t, servers, true, false)
	defer tr.cleanup()

	tr.begin("Test (PartC): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				tr.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		tr.one(iters, 1, true)
	}

	tr.setUnreliable(false)

	wg.Wait()

	tr.one(100, servers, true)

	tr.end()
}

func TestFigure8UnreliablePartC(t *testing.T) {
	servers := 5
	tr := newTester(t, servers, true, false)
	defer tr.cleanup()

	tr.begin("Test (PartC): Figure 8 (unreliable)")

	tr.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			tr.setLongReordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := tr.rafts[i].StartAppendCommandInLeader(rand.Int() % 10000)
			if ok && tr.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			tr.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if tr.connected[s] == false {
				tr.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if tr.connected[i] == false {
			tr.connect(i)
		}
	}

	tr.one(rand.Int()%10000, servers, true)

	tr.end()
}

func internalChurn(t *testing.T, unreliable bool) {

	servers := 5
	tr := newTester(t, servers, unreliable, false)
	defer tr.cleanup()

	if unreliable {
		tr.begin("Test (PartC): unreliable churn")
	} else {
		tr.begin("Test (PartC): churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				tr.mu.Lock()
				rf := tr.rafts[i]
				tr.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.StartAppendCommandInLeader(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := tr.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							tr.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			tr.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if tr.rafts[i] == nil {
				tr.startRaft(i, tr.applier)
			}
			tr.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if tr.rafts[i] != nil {
				tr.crash(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	tr.setUnreliable(false)
	for i := 0; i < servers; i++ {
		if tr.rafts[i] == nil {
			tr.startRaft(i, tr.applier)
		}
		tr.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := tr.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := tr.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			tr.t.Fatalf("didn't find a value")
		}
	}

	tr.end()
}

//func TestReliableChurnPartC(t *testing.T) {
//	internalChurn(t, false)
//}
//
//func TestUnreliableChurnPartC(t *testing.T) {
//	internalChurn(t, true)
//}

const MAXLOGSIZE = 2000

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 30
	servers := 3
	tr := newTester(t, servers, !reliable, true)
	defer tr.cleanup()

	tr.begin(name)

	tr.one(rand.Int(), servers, true)
	leader1 := tr.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			tr.disconnect(victim)
			tr.one(rand.Int(), servers-1, true)
		}
		if crash {
			tr.crash(victim)
			tr.one(rand.Int(), servers-1, true)
		}

		// perhaps send enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			tr.rafts[sender].StartAppendCommandInLeader(rand.Int())
		}

		// let applier threads catch up with the StartAppendCommandInLeader()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			// TestSnapshotBasicPartD().
			tr.one(rand.Int(), servers, true)
		} else {
			tr.one(rand.Int(), servers-1, true)
		}

		if tr.logSize() >= MAXLOGSIZE {
			tr.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			tr.connect(victim)
			tr.one(rand.Int(), servers, true)
			leader1 = tr.checkOneLeader()
		}
		if crash {
			tr.startRaft(victim, tr.applierSnap)
			tr.connect(victim)
			tr.one(rand.Int(), servers, true)
			leader1 = tr.checkOneLeader()
		}
	}
	tr.end()
}

//func TestSnapshotBasicPartD(t *testing.T) {
//	snapcommon(t, "Test (PartD): snapshots basic", false, true, false)
//}
//
//func TestSnapshotInstallPartD(t *testing.T) {
//	snapcommon(t, "Test (PartD): install snapshots (disconnect)", true, true, false)
//}
//
//func TestSnapshotInstallUnreliablePartD(t *testing.T) {
//	snapcommon(t, "Test (PartD): install snapshots (disconnect+unreliable)",
//		true, false, false)
//}
//
//func TestSnapshotInstallCrashPartD(t *testing.T) {
//	snapcommon(t, "Test (PartD): install snapshots (crash)", false, true, true)
//}
//
//func TestSnapshotInstallUnCrashPartD(t *testing.T) {
//	snapcommon(t, "Test (PartD): install snapshots (unreliable+crash)", false, false, true)
//}
//
//// do the servers persist the snapshots, and
//// restart using snapshot along with the
//// tail of the log?
//func TestSnapshotAllCrashPartD(t *testing.T) {
//	servers := 3
//	iters := 5
//	tr := newTester(t, servers, false, true)
//	defer tr.cleanup()
//
//	tr.begin("Test (PartD): crash and restart all servers")
//
//	tr.one(rand.Int(), servers, true)
//
//	for i := 0; i < iters; i++ {
//		// perhaps enough to get a snapshot
//		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
//		for i := 0; i < nn; i++ {
//			tr.one(rand.Int(), servers, true)
//		}
//
//		index1 := tr.one(rand.Int(), servers, true)
//
//		// crash all
//		for i := 0; i < servers; i++ {
//			tr.crash(i)
//		}
//
//		// revive all
//		for i := 0; i < servers; i++ {
//			tr.startRaft(i, tr.applierSnap)
//			tr.connect(i)
//		}
//
//		index2 := tr.one(rand.Int(), servers, true)
//		if index2 < index1+1 {
//			t.Fatalf("index decreased from %v to %v", index1, index2)
//		}
//	}
//	tr.end()
//}
//
//// do servers correctly initialize their in-memory copy of the snapshot, making
//// sure that future writes to persistent state don't lose state?
//func TestSnapshotInitPartD(t *testing.T) {
//	servers := 3
//	tr := newTester(t, servers, false, true)
//	defer tr.cleanup()
//
//	tr.begin("Test (PartD): snapshot initialization after crash")
//	tr.one(rand.Int(), servers, true)
//
//	// enough ops to make a snapshot
//	nn := SnapShotInterval + 1
//	for i := 0; i < nn; i++ {
//		tr.one(rand.Int(), servers, true)
//	}
//
//	// crash all
//	for i := 0; i < servers; i++ {
//		tr.crash(i)
//	}
//
//	// revive all
//	for i := 0; i < servers; i++ {
//		tr.startRaft(i, tr.applierSnap)
//		tr.connect(i)
//	}
//
//	// a single op, to get something to be written back to persistent storage.
//	tr.one(rand.Int(), servers, true)
//
//	// crash all
//	for i := 0; i < servers; i++ {
//		tr.crash(i)
//	}
//
//	// revive all
//	for i := 0; i < servers; i++ {
//		tr.startRaft(i, tr.applierSnap)
//		tr.connect(i)
//	}
//
//	// do another op to trigger potential bug
//	tr.one(rand.Int(), servers, true)
//	tr.end()
//}

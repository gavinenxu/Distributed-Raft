package rpc

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)
import "runtime"

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer("server99")
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	{
		reply := ""
		ok := clientEnd.Call("JunkServer.Handler2", 111, &reply)
		assert.True(t, ok)
		assert.Equal(t, "handler2-111", reply)
		assert.Equal(t, 111, js.log2[0])
	}

	{
		reply := 0
		ok := clientEnd.Call("JunkServer.Handler1", "9099", &reply)
		assert.True(t, ok)
		assert.Equal(t, 9099, reply)
		assert.Equal(t, "9099", js.log1[0])
	}
}

func TestTypes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer("server99")
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	{
		var args JunkArgs
		var reply JunkReply
		// args must match type (pointer or not) of handler.
		ok := clientEnd.Call("JunkServer.Handler4", &args, &reply)
		assert.True(t, ok)
		assert.Equal(t, "pointer", reply.X)
	}

	{
		var args JunkArgs
		var reply JunkReply
		// args must match type (pointer or not) of handler.
		ok := clientEnd.Call("JunkServer.Handler5", args, &reply)
		assert.True(t, ok)
		assert.Equal(t, "no pointer", reply.X)
	}
}

// does net.Enable(endname, false) really disconnect a client?
func TestDisconnect(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer("server99")
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)

	{
		reply := ""
		ok := clientEnd.Call("JunkServer.Handler2", 111, &reply)
		assert.False(t, ok)
		assert.Equal(t, "", reply)
	}

	net.Enable(clientEnd.name, true)

	{
		reply := 0
		ok := clientEnd.Call("JunkServer.Handler1", "9099", &reply)
		assert.True(t, ok)
		assert.Equal(t, 9099, reply)
	}
}

// test net.GetCount()
func TestCounts(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer(99)
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	num := 17
	for i := 0; i < num; i++ {
		reply := ""
		ok := clientEnd.Call("JunkServer.Handler2", i, &reply)
		assert.True(t, ok)
		wanted := "handler2-" + strconv.Itoa(i)
		assert.Equal(t, wanted, reply)
	}

	n := net.GetCount(server.name)
	assert.Equal(t, num, n)
}

// test net.GetTotalBytes()
func TestBytes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer(99)
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	num := 17
	for i := 0; i < num; i++ {
		args := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
		args = args + args
		args = args + args
		reply := 0
		ok := clientEnd.Call("JunkServer.Handler6", args, &reply)
		assert.True(t, ok)
		wanted := len(args)
		assert.Equal(t, wanted, reply)
	}

	n := net.GetTotalBytes()
	if n < 4828 || n > 6000 {
		t.Fatalf("wrong GetTotalBytes() %v, expected about 5000\n", n)
	}

	for i := 0; i < num; i++ {
		args := 107
		reply := ""
		ok := clientEnd.Call("JunkServer.Handler7", args, &reply)
		assert.True(t, ok)
		wanted := args
		assert.Equal(t, wanted, len(reply))
	}

	nn := net.GetTotalBytes() - n
	if nn < 1800 || nn > 2500 {
		t.Fatalf("wrong GetTotalBytes() %v, expected about 2000\n", nn)
	}
}

// test RPCs from concurrent ClientEnds
func TestConcurrentMany(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer(1000)
	server.AddService(service)
	net.AddServer(server)

	ch := make(chan int)

	numClients := 20
	numRpcs := 10
	for ii := 0; ii < numClients; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			client := net.AddClientEnd(i)
			net.Connect(i, server.name)
			net.Enable(i, true)

			for j := 0; j < numRpcs; j++ {
				arg := i*100 + j
				reply := ""
				ok := client.Call("JunkServer.Handler2", arg, &reply)
				assert.True(t, ok)
				wanted := "handler2-" + strconv.Itoa(arg)
				assert.Equal(t, wanted, reply)
				n += 1
			}
		}(ii)
	}

	total := 0
	for ii := 0; ii < numClients; ii++ {
		x := <-ch
		total += x
	}
	assert.Equal(t, numClients*numRpcs, total)

	n := net.GetCount(server.name)
	assert.Equal(t, total, n)
}

// test unreliable
func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer(1000)
	server.AddService(service)
	net.AddServer(server)

	net.Reliable(false)

	ch := make(chan int)

	numClients := 300
	for ii := 0; ii < numClients; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			client := net.AddClientEnd(i)
			net.Connect(i, server.name)
			net.Enable(i, true)

			arg := i * 100
			reply := ""
			ok := client.Call("JunkServer.Handler2", arg, &reply)
			if ok {
				wanted := "handler2-" + strconv.Itoa(arg)
				assert.Equal(t, wanted, reply)
				n += 1
			}
		}(ii)
	}

	total := 0
	for ii := 0; ii < numClients; ii++ {
		x := <-ch
		total += x
	}

	if total == numClients || total == 0 {
		t.Fatalf("all RPCs succeeded despite unreliable")
	}
}

// test concurrent RPCs from a single ClientEnd
func TestConcurrentOne(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer(1000)
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("c")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	ch := make(chan int)

	numRpcs := 20
	for ii := 0; ii < numRpcs; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			arg := 100 + i
			reply := ""
			ok := clientEnd.Call("JunkServer.Handler2", arg, &reply)
			assert.True(t, ok)
			wanted := "handler2-" + strconv.Itoa(arg)
			assert.Equal(t, wanted, reply)
			n += 1
		}(ii)
	}

	total := 0
	for ii := 0; ii < numRpcs; ii++ {
		x := <-ch
		total += x
	}

	assert.Equal(t, numRpcs, total)

	js.mu.Lock()
	defer js.mu.Unlock()
	assert.Equal(t, numRpcs, len(js.log2))

	n := net.GetCount(server.name)
	assert.Equal(t, total, n)
}

// regression: an RPC that's delayed during Enabled=false
// should not delay subsequent RPCs (e.g. after Enabled=true).
func TestRegression1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer(1000)
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("c")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)

	// start some RPCs while the ClientEnd is disabled.
	// they'll be delayed.
	net.Enable(clientEnd.name, false)
	ch := make(chan bool)
	numRpcs := 20
	for ii := 0; ii < numRpcs; ii++ {
		go func(i int) {
			ok := false
			defer func() { ch <- ok }()

			arg := 100 + i
			reply := ""
			// this call ought to return false.
			clientEnd.Call("JunkServer.Handler2", arg, &reply)
			ok = true
		}(ii)
	}

	time.Sleep(100 * time.Millisecond)

	// now enable the ClientEnd and check that an RPC completes quickly.
	t0 := time.Now()
	net.Enable("c", true)
	{
		arg := 99
		reply := ""
		ok := clientEnd.Call("JunkServer.Handler2", arg, &reply)
		assert.True(t, ok)
		wanted := "handler2-" + strconv.Itoa(arg)
		assert.Equal(t, wanted, reply)
	}
	dur := time.Since(t0).Seconds()

	if dur > 0.03 {
		t.Fatalf("RPC took too long (%v) after Enable", dur)
	}

	for ii := 0; ii < numRpcs; ii++ {
		<-ch
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	assert.Equal(t, 1, len(js.log2))

	n := net.GetCount(server.name)
	assert.Equal(t, 1, n)
}

// if an RPC is stuck in a server, and the server
// is killed with DeleteServer(), does the RPC
// get un-stuck?
func TestKilled(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer("server99")
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	doneCh := make(chan bool)
	go func() {
		reply := 0
		// take 1 second
		ok := clientEnd.Call("JunkServer.Handler3", 99, &reply)
		doneCh <- ok
	}()

	select {
	case <-doneCh:
		t.Fatalf("Handler3 should not have returned yet")
	case <-time.After(100 * time.Millisecond):
	}

	time.Sleep(1000 * time.Millisecond)

	net.DeleteServer(server.name)

	select {
	case x := <-doneCh:
		assert.True(t, x)
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Handler3 should return after DeleteServer()")
	}
}

func TestBenchmark(t *testing.T) {
	runtime.GOMAXPROCS(4)

	net := NewNetwork()
	defer net.Cleanup()

	js := &JunkServer{}
	service := NewService(js)

	// net add server
	server := NewServer("server99")
	server.AddService(service)
	net.AddServer(server)

	// net add client
	clientEnd := net.AddClientEnd("end1-99")

	// net connect client and server
	net.Connect(clientEnd.name, server.name)
	net.Enable(clientEnd.name, true)

	t0 := time.Now()
	n := 100000
	for iters := 0; iters < n; iters++ {
		reply := ""
		clientEnd.Call("JunkServer.Handler2", 111, &reply)
		assert.Equal(t, "handler2-111", reply)
	}
	fmt.Printf("%v for %v\n", time.Since(t0), n)
	// march 2016, rtm laptop, 22 microseconds per RPC
}

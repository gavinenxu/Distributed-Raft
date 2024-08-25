package rpc

//
// channel-based RPC, for 6.5840 labs.
//
// simulates a network that can lose requests, lose replies,
// delay messages, and entirely disconnect particular hosts.
//
// adapted from Go net/rpc/server.go.
//
// sends encoding-encoded values to ensure that RPCs
// don't include references to program objects.
//
// net := NewNetwork() -- holds network, clients, servers.
// end := net.NewClientEnd(endName) -- create a client end-point, to talk to one server.
// net.AddServer(servername, server) -- adds a named server to network.
// net.DeleteServer(servername) -- eliminate the named server.
// net.Connect(endName, servername) -- connect a client to a server.
// net.Enable(endName, enabled) -- enable/disable a client.
// net.Reliable(bool) -- false means drop/delay messages
//
// end.Call("Raft.AppendEntries", &args, &reply) -- send an RPC, wait for reply.
// the "Raft" is the name of the server struct to be called.
// the "AppendEntries" is the name of the method to be called.
// Call() returns true to indicate that the server executed the request
// and the reply is valid.
// Call() returns false if the network lost the request or reply
// or the server is down.
// It is OK to have multiple Call()s in progress at the same time on the
// same ClientEnd.
// Concurrent calls to Call() may be delivered to the server out of order,
// since the network may re-order messages.
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.
// the server RPC handler function must declare its args and reply arguments
// as pointers, so that their types exactly match the types of the arguments
// to Call().
//
// srv := NewServer()
// srv.AddService(svc) -- a server can have multiple services, e.g. Raft and k/v
//   pass srv to net.AddServer()
//
// svc := NewService(receiverObject) -- obj's methods will handle RPCs
//   much like Go's rpcs.Register()
//   pass svc to srv.AddService()
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// <---------------------------Network-------------------------------->

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool                        // pause a long time on send on disabled connection
	longReordering bool                        // sometimes delay replies a long time
	clientEnds     map[interface{}]*ClientEnd  // ends, by name
	enabled        map[interface{}]bool        // by end name
	servers        map[interface{}]*Server     // servers, by name
	connections    map[interface{}]interface{} // endName -> servername
	endCh          chan requestMessage
	done           chan struct{} // closed when Network is cleaned up
	count          int32         // total RPC count, for statistics
	bytes          int64         // total bytes send, for statistics
}

func NewNetwork() *Network {
	network := &Network{
		reliable:    true,
		clientEnds:  make(map[interface{}]*ClientEnd),
		enabled:     make(map[interface{}]bool),
		servers:     make(map[interface{}]*Server),
		connections: make(map[interface{}]interface{}),
		endCh:       make(chan requestMessage),
		done:        make(chan struct{}),
	}

	// single goroutine to handle all ClientEnd.Call()s
	go func() {
		for {
			select {
			case reqMsg := <-network.endCh:
				atomic.AddInt32(&network.count, 1)
				atomic.AddInt64(&network.bytes, int64(len(reqMsg.args)))
				go network.processReq(reqMsg)
			case <-network.done:
				return
			}
		}
	}()

	return network
}

// AddClientEnd create a client end-point.
// start the thread that listens and delivers.
func (nw *Network) AddClientEnd(endName interface{}) *ClientEnd {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	if _, ok := nw.clientEnds[endName]; ok {
		log.Fatalf("NewClientEnd: %v already exists\n", endName)
	}

	clientEnd := &ClientEnd{
		name: endName,
		ch:   nw.endCh,
		done: nw.done,
	}

	nw.clientEnds[endName] = clientEnd
	nw.enabled[endName] = false
	nw.connections[endName] = nil

	return clientEnd
}

func (nw *Network) Cleanup() {
	close(nw.done)
}

func (nw *Network) Reliable(isReliable bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.reliable = isReliable
}

func (nw *Network) LongReordering(isLongReordering bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.longReordering = isLongReordering
}

func (nw *Network) LongDelays(isLongDelays bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.longDelays = isLongDelays
}

func (nw *Network) AddServer(server *Server) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.servers[server.name] = server
}

func (nw *Network) DeleteServer(servername interface{}) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.servers[servername] = nil
}

// Connect a ClientEnd to a server.
// a ClientEnd can only be connected once in its lifetime.
func (nw *Network) Connect(endName interface{}, servername interface{}) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.connections[endName] = servername
}

// Enable enable/disable a ClientEnd.
func (nw *Network) Enable(endName interface{}, enabled bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.enabled[endName] = enabled
}

// GetCount get a server's count of incoming RPCs.
func (nw *Network) GetCount(servername interface{}) int {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	server := nw.servers[servername]
	return server.GetCount()
}

func (nw *Network) GetTotalCount() int {
	x := atomic.LoadInt32(&nw.count)
	return int(x)
}

func (nw *Network) GetTotalBytes() int64 {
	x := atomic.LoadInt64(&nw.bytes)
	return x
}

func (nw *Network) readEndNameInfo(endName interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longReordering bool,
) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	enabled = nw.enabled[endName]
	servername = nw.connections[endName]
	if servername != nil {
		server = nw.servers[servername]
	}
	reliable = nw.reliable
	longReordering = nw.longReordering
	return
}

func (nw *Network) isServerDead(endName interface{}, servername interface{}, server *Server) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	if !nw.enabled[endName] || servername != server.name {
		return true
	}
	return false
}

func (nw *Network) processReq(req requestMessage) {
	enabled, servername, server, reliable, longReordering := nw.readEndNameInfo(req.clientEndName)

	if !enabled || servername == nil || server == nil {
		// simulate no reply and eventual timeout.
		ms := 0
		if nw.longDelays {
			// let Raft tests check that leader doesn't send
			// RPCs synchronously.
			ms = rand.Intn(7000)
		} else {
			// many kv tests require the client to try each
			// server in fairly rapid succession.
			ms = rand.Intn(100)
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMessage{false, nil}
		})
		return
	}

	if !reliable {
		// short delay
		ms := rand.Intn(27)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if (rand.Intn(1000)) < 100 {
			// drop the request, return as if timeout
			req.replyCh <- replyMessage{false, nil}
			return
		}
	}

	// execute the request (call the RPC handler).
	// in a separate thread so that we can periodically check
	// if the server has been killed and the RPC should get a
	// failure reply.
	msgCh := make(chan replyMessage)
	go func() {
		serverReplyMsg := server.dispatch(req)
		msgCh <- serverReplyMsg
	}()

	// wait for handler to return,
	// but stop waiting if DeleteServer() has been called,
	// and return an error.
	var reply replyMessage
	replyOK := false
	serverDead := false
	for !replyOK && !serverDead {
		select {
		case reply = <-msgCh:
			replyOK = true
		case <-time.After(100 * time.Millisecond):
			serverDead = nw.isServerDead(req.clientEndName, servername, server)
			if serverDead {
				go func() {
					<-msgCh // drain channel to let the goroutine created earlier terminate
				}()
			}
		}
	}

	// do not reply if DeleteServer() has been called, i.e.
	// the server has been killed. this is needed to avoid
	// situation in which a client gets a positive reply
	// to an Append, but the server persisted the update
	// into the old Persister. tester.go is careful to call
	// DeleteServer() before superseding the Persister.
	serverDead = nw.isServerDead(req.clientEndName, servername, server)

	if !replyOK || serverDead {
		// server was killed while we were waiting; return error.
		req.replyCh <- replyMessage{false, nil}
	} else if !reliable && (rand.Intn(1000)) < 100 {
		// drop the reply, return as if timeout
		req.replyCh <- replyMessage{false, nil}
	} else if longReordering && rand.Intn(900) < 600 {
		// delay the response for a while
		ms := 200 + rand.Intn(1+rand.Intn(2000))
		// Russ points out that this timer arrangement will decrease
		// the number of goroutines, so that the race
		// detector is less likely to get upset.
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			atomic.AddInt64(&nw.bytes, int64(len(reply.msg)))
			req.replyCh <- reply
		})
	} else {
		atomic.AddInt64(&nw.bytes, int64(len(reply.msg)))
		req.replyCh <- reply
	}
}

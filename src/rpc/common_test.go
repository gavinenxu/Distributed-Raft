package rpc

import (
	"strconv"
	"sync"
	"time"
)

type JunkArgs struct {
	X int
}
type JunkReply struct {
	X string
}

type JunkServer struct {
	mu   sync.Mutex
	log1 []string
	log2 []int
}

func (js *JunkServer) Handler1(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
}

func (js *JunkServer) Handler2(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log2 = append(js.log2, args)
	*reply = "handler2-" + strconv.Itoa(args)
}

func (js *JunkServer) Handler3(args int, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	time.Sleep(1 * time.Second)
	*reply = -args
}

// args is a pointer
func (js *JunkServer) Handler4(args *JunkArgs, reply *JunkReply) {
	reply.X = "pointer"
}

// args is a not pointer
func (js *JunkServer) Handler5(args JunkArgs, reply *JunkReply) {
	reply.X = "no pointer"
}

func (js *JunkServer) Handler6(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	*reply = len(args)
}

func (js *JunkServer) Handler7(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	*reply = ""
	for i := 0; i < args; i++ {
		*reply = *reply + "y"
	}
}

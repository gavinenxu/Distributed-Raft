package rpc

import (
	"bytes"
	"log"
	"raft-kv/encoding"
	"reflect"
)

type ClientEnd struct {
	name interface{}         // this end-point's name
	ch   chan requestMessage // copy of Network.endCh
	done chan struct{}       // closed when Network is cleaned up
}

// Call send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
// reply should be pointer to get the value
func (client *ClientEnd) Call(serviceMethod string, args interface{}, reply interface{}) bool {
	req := requestMessage{
		clientEndName: client.name,
		serviceMethod: serviceMethod,
		argsType:      reflect.TypeOf(args),
		replyCh:       make(chan replyMessage),
	}

	// encoding request arguments
	writer := new(bytes.Buffer)
	enc := encoding.NewEncoder(writer)
	if err := enc.Encode(args); err != nil {
		panic(err)
	}
	req.args = writer.Bytes()

	// send the request to network channel
	select {
	case client.ch <- req:
		// the request has been sent.
	case <-client.done:
		// entire Network has been destroyed.
		return false
	}

	// wait for the reply.
	replyMsg := <-req.replyCh
	if replyMsg.ok {
		reader := bytes.NewBuffer(replyMsg.msg)
		dec := encoding.NewDecoder(reader)
		if err := dec.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

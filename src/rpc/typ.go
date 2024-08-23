package rpc

import "reflect"

type requestMessage struct {
	clientEndName interface{} // name of sending ClientEnd
	serviceMethod string      // e.g. "Raft.AppendEntries"
	argsType      reflect.Type
	args          []byte
	replyCh       chan replyMessage
}

type replyMessage struct {
	ok  bool
	msg []byte
}

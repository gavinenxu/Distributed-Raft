package rpc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClientEnd_Call(t *testing.T) {
	reqCh := make(chan requestMessage)
	client := &ClientEnd{
		name: "end1",
		ch:   reqCh,
		done: make(chan struct{}),
	}

	go func() {
		reqMsg := <-reqCh
		reqMsg.replyCh <- replyMessage{true, []byte{6, 12, 0, 3, 49, 49, 49}}

	}()

	reply := ""
	ok := client.Call("", "", &reply)

	assert.True(t, ok)
	assert.Equal(t, "111", reply)
}

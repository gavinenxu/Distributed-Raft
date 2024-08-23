package rpc

import (
	"bytes"
	"course/encoding"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestServerDispatch(t *testing.T) {
	js := &JunkServer{}
	service := NewService(js)

	server := NewServer("server1")
	server.AddService(service)

	{
		method := "JunkServer.Handler1"
		args := "111"

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := server.dispatch(req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res int
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, 111, res)
	}

	{
		method := "JunkServer.Handler2"
		args := 111

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := server.dispatch(req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res string
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, "handler2-111", res)
	}

	{
		method := "JunkServer.Handler4"
		args := &JunkArgs{}

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := server.dispatch(req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res JunkReply
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, "pointer", res.X)
	}
}

package rpc

import (
	"bytes"
	"course/encoding"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestServiceDispatch(t *testing.T) {
	js := &JunkServer{}
	service := NewService(js)
	assert.NotNil(t, service)
	typ := reflect.TypeOf(js)
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		r := service.methods[method.Name]
		assert.NotNil(t, r)
	}

	{
		method := "Handler1"
		args := "111"

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := service.dispatch(method, req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res int
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, 111, res)
	}

	{
		method := "Handler2"
		args := 111

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := service.dispatch(method, req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res string
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, "handler2-111", res)
	}

	{
		method := "Handler4"
		args := &JunkArgs{}

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := service.dispatch(method, req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res JunkReply
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, "pointer", res.X)
	}

	{
		method := "Handler5"
		args := JunkArgs{}

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := service.dispatch(method, req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res JunkReply
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, "no pointer", res.X)
	}

	{
		method := "Handler7"
		args := 5

		req := requestMessage{
			serviceMethod: method,
		}
		req.argsType = reflect.TypeOf(args)

		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.Encode(args)
		req.args = writer.Bytes()

		reply := service.dispatch(method, req)
		assert.True(t, reply.ok)
		assert.NotNil(t, reply.msg)

		var res string
		reader := bytes.NewBuffer(reply.msg)
		dec := encoding.NewDecoder(reader)

		_ = dec.Decode(&res)
		assert.Equal(t, "yyyyy", res)
	}
}

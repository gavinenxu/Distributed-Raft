package rpc

import (
	"bytes"
	"log"
	"raft-kv/encoding"
	"reflect"
)

// Service an object with methods that can be called via RPC.
// a single server may have more than one Service.
type Service struct {
	name     string
	receiver reflect.Value
	typ      reflect.Type
	methods  map[string]reflect.Method
}

func NewService(receiver interface{}) *Service {
	service := &Service{
		typ:      reflect.TypeOf(receiver),
		receiver: reflect.ValueOf(receiver),
		methods:  make(map[string]reflect.Method),
	}
	service.name = reflect.Indirect(service.receiver).Type().Name()

	for i := 0; i < service.typ.NumMethod(); i++ {
		method := service.typ.Method(i)
		mType := method.Type
		mName := method.Name

		//fmt.Printf("%v pp %v ni %v 1k %v 2k %v no %v\n",
		//	mname, method.PkgPath, mtype.NumIn(), mtype.In(1).Kind(), mtype.In(2).Kind(), mtype.NumOut())

		if method.PkgPath != "" || // capitalized?
			mType.NumIn() != 3 ||
			//mtype.In(1).Kind() != reflect.Ptr ||
			mType.In(2).Kind() != reflect.Ptr ||
			mType.NumOut() != 0 {
			// the method is not suitable for a handler
			//fmt.Printf("bad method: %v\n", mname)
		} else {
			// the method looks like a handler
			service.methods[mName] = method
		}
	}

	return service
}

func (svc *Service) dispatch(methodName string, req requestMessage) replyMessage {
	if method, ok := svc.methods[methodName]; ok {
		// prepare space into which to read the argument.
		// the Value's type will be a pointer to req.argsType.
		args := reflect.New(req.argsType)

		// decode the argument.
		reader := bytes.NewBuffer(req.args)
		dec := encoding.NewDecoder(reader)
		_ = dec.Decode(args.Interface())

		// allocate space for the reply.
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyVal := reflect.New(replyType)

		// call the method.
		function := method.Func
		function.Call([]reflect.Value{svc.receiver, args.Elem(), replyVal})

		// encode the reply.
		writer := new(bytes.Buffer)
		enc := encoding.NewEncoder(writer)
		_ = enc.EncodeValue(replyVal)

		return replyMessage{true, writer.Bytes()}
	} else {
		choices := make([]string, 0)
		for c, _ := range svc.methods {
			choices = append(choices, c)
		}
		log.Fatalf("rpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methodName, req.serviceMethod, choices)
		return replyMessage{false, nil}
	}
}

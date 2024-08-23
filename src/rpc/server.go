package rpc

import (
	"log"
	"strings"
	"sync"
)

// Server a server is a collection of services, all sharing
// the same rpc dispatcher. so that e.g. both a Raft
// and a k/v server can listen to the same rpc endpoint.
type Server struct {
	name     interface{}
	mu       sync.Mutex
	services map[string]*Service
	count    int // incoming RPCs
}

func NewServer(name interface{}) *Server {
	return &Server{
		name:     name,
		services: make(map[string]*Service),
	}
}

func (server *Server) AddService(service *Service) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.services[service.name] = service
}

func (server *Server) dispatch(req requestMessage) replyMessage {
	server.mu.Lock()

	server.count += 1

	// split Raft.AppendEntries into service and method
	dotIdx := strings.LastIndex(req.serviceMethod, ".")
	serviceName := req.serviceMethod[:dotIdx]
	methodName := req.serviceMethod[dotIdx+1:]

	service, ok := server.services[serviceName]

	server.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	}

	choices := make([]string, 0)
	for c, _ := range server.services {
		choices = append(choices, c)
	}
	log.Fatalf("rpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
		serviceName, serviceName, methodName, choices)
	return replyMessage{false, nil}
}

func (server *Server) GetCount() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.count
}

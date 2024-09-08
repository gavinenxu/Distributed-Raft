package shardctl

type StateMachine struct {
	Map map[string]string
}

func NewStateMachine() *StateMachine {
	return &StateMachine{Map: make(map[string]string)}
}

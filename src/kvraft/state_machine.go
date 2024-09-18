package kvraft

type MemoryKVStateMachine struct {
	Map map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{Map: make(map[string]string)}
}

func (cm *MemoryKVStateMachine) Put(key string, value string) Err {
	cm.Map[key] = value
	return OK
}

func (cm *MemoryKVStateMachine) Append(key string, value string) Err {
	cm.Map[key] += value
	return OK
}

func (cm *MemoryKVStateMachine) Get(key string) (string, Err) {
	if val, ok := cm.Map[key]; ok {
		return val, OK
	}
	return "", ErrKeyNotFound
}

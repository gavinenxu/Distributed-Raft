package kvraft

type MemoryKVStateMachine struct {
	Map map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{Map: make(map[string]string)}
}

func (cm *MemoryKVStateMachine) Put(key string, value string) error {
	cm.Map[key] = value
	return nil
}

func (cm *MemoryKVStateMachine) Append(key string, value string) error {
	if _, ok := cm.Map[key]; !ok {
		return ErrKeyNotFound
	}
	cm.Map[key] += value
	return nil
}

func (cm *MemoryKVStateMachine) Get(key string) (string, error) {
	if val, ok := cm.Map[key]; ok {
		return val, nil
	}

	return "", ErrKeyNotFound
}

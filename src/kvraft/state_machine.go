package kvraft

type ClerkStateMachine struct {
	Map map[string]string
}

func NewClerkStateMachine() *ClerkStateMachine {
	return &ClerkStateMachine{Map: make(map[string]string)}
}

func (cm *ClerkStateMachine) Put(key string, value string) error {
	cm.Map[key] = value
	return nil
}

func (cm *ClerkStateMachine) Append(key string, value string) error {
	if _, ok := cm.Map[key]; !ok {
		return ErrKeyNotFound
	}
	cm.Map[key] += value
	return nil
}

func (cm *ClerkStateMachine) Get(key string) (string, error) {
	if val, ok := cm.Map[key]; ok {
		return val, nil
	}

	return "", ErrKeyNotFound
}

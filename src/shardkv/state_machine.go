package shardkv

type StateMachine struct {
	Map    map[string]string
	Status ShardStatus
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		Map:    make(map[string]string),
		Status: Normal,
	}
}

func (cm *StateMachine) Put(key string, value string) Err {
	cm.Map[key] = value
	return OK
}

func (cm *StateMachine) Append(key string, value string) Err {
	cm.Map[key] += value
	return OK
}

func (cm *StateMachine) Get(key string) (string, Err) {
	if val, ok := cm.Map[key]; ok {
		return val, OK
	}

	return "", ErrKeyNotFound
}

func (cm *StateMachine) copyData() map[string]string {
	data := make(map[string]string)
	for k, v := range cm.Map {
		data[k] = v
	}
	return data
}

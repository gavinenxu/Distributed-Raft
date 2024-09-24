package shardctl

import "sort"

type StateMachine struct {
	Configs []Config // The groups of config that applied
}

func NewStateMachine() *StateMachine {
	sm := &StateMachine{Configs: make([]Config, 1)}
	sm.Configs[0] = DefaultConfig()
	return sm
}

func (sm *StateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(sm.Configs) {
		return sm.Configs[len(sm.Configs)-1], OK
	}

	return sm.Configs[num], OK
}

// Join the input groups own different group id with config's groups
func (sm *StateMachine) Join(groups map[int][]string) Err {
	if _, ok := groups[InvalidGroupId]; ok {
		return ErrInvalidGroup
	}

	if len(groups) == 0 {
		return OK
	}

	num := len(sm.Configs)
	lastConfig := sm.Configs[num-1]
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}

	// copy groups
	for gid, servers := range lastConfig.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	// join with input group
	var validGid int
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
		validGid = gid
	}

	// mapping from group to shards
	groupShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		// create group to shards mapping table based on input groups
		groupShards[gid] = make([]int, 0)
	}

	for shard, gid := range lastConfig.Shards {
		// initial value could be invalid group, we assign it to other valid group
		if gid == InvalidGroupId {
			gid = validGid
		}
		groupShards[gid] = append(groupShards[gid], shard)
	}

	// balance group shards, to remove item from largest size of group to smallest size of group
	for {
		minGid, maxGid := minMaxShardsGroup(groupShards)

		if len(groupShards[maxGid])-len(groupShards[minGid]) <= 1 {
			break
		}

		lastShard := groupShards[maxGid][len(groupShards[maxGid])-1]
		groupShards[minGid] = append(groupShards[minGid], lastShard)
		groupShards[maxGid] = groupShards[maxGid][:len(groupShards[maxGid])-1]
	}

	var newShards [NShards]int
	for gid, shards := range groupShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards

	sm.Configs = append(sm.Configs, newConfig)
	return OK
}

func (sm *StateMachine) Leave(gids []int) Err {
	lastConfig := sm.Configs[len(sm.Configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: make(map[int][]string),
	}

	// copy groups
	for gid, servers := range lastConfig.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	// mapping from group to shards
	groupShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		groupShards[gid] = make([]int, 0)
	}
	for shard, gid := range lastConfig.Shards {
		groupShards[gid] = append(groupShards[gid], shard)
	}

	// delete the input group's and also cache the shards for these groups
	var unassignedShards []int
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}

		if _, ok := groupShards[gid]; ok {
			unassignedShards = append(unassignedShards, groupShards[gid]...)
			delete(groupShards, gid)
		}
	}

	// init shards or reassign groups
	var newShard [NShards]int
	if len(newConfig.Groups) > 0 {
		for _, shard := range unassignedShards {
			minGid, _ := minMaxShardsGroup(groupShards)
			groupShards[minGid] = append(groupShards[minGid], shard)
		}

		for gid, shards := range groupShards {
			for _, shard := range shards {
				newShard[shard] = gid
			}
		}
	}

	newConfig.Shards = newShard
	sm.Configs = append(sm.Configs, newConfig)
	return OK
}

func (sm *StateMachine) Move(shard, gid int) Err {
	lastConfig := sm.Configs[len(sm.Configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}

	// copy groups
	for gid, servers := range lastConfig.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	newConfig.Shards[shard] = gid

	sm.Configs = append(sm.Configs, newConfig)
	return OK
}

func minMaxShardsGroup(groupShards map[int][]int) (int, int) {
	var gids []int
	for gid := range groupShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minGidShards := -1, NShards+1
	maxGid, maxGidShards := -1, -1

	for _, gid := range gids {
		if len(groupShards[gid]) < minGidShards {
			minGid, minGidShards = gid, len(groupShards[gid])
		}

		if len(groupShards[gid]) > maxGidShards {
			maxGid, maxGidShards = gid, len(groupShards[gid])
		}
	}

	return minGid, maxGid
}

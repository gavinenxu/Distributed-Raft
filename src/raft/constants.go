package raft

import "time"

const NotVoted = -1

type Role byte

const (
	Leader Role = iota
	Candidate
	Follower
)

const (
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 400 * time.Millisecond
	replicateInterval  = 70 * time.Millisecond
)

const (
	InvalidLogIndex = 0
	InvalidTerm     = 0
	InitialTerm     = InvalidTerm + 1
)

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
)

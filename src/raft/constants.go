package raft

const NotVoted = -1

type Role byte

const (
	Leader Role = iota
	Candidate
	Follower
)

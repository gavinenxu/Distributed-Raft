package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DError logTopic = "ERROR" // level = 3
	DWarn  logTopic = "WARN"  // level = 2
	DInfo  logTopic = "INFO"  // level = 1
	DDebug logTopic = "DEBUG" // level = 0

	// level 1 topics
	DClient  logTopic = "CLIENT"
	DCommit  logTopic = "COMMIT"
	DDrop    logTopic = "DROP"
	DLeader  logTopic = "LEADER"
	DLog     logTopic = "SEND"    // sending log
	DLog2    logTopic = "RECEIVE" // receiving log
	DPersist logTopic = "PERSIST"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMER"
	DTrace   logTopic = "TRACE"
	DVote    logTopic = "VOTE"
	DApply   logTopic = "APPLY"
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DError:
		return 3
	case DWarn:
		return 2
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}

func getEnvLevel() int {
	v := os.Getenv("VERBOSE")
	level := getTopicLevel(DError) + 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var logStart time.Time
var logLevel int

func init() {
	logLevel = getEnvLevel()
	logStart = time.Now()

	// do not print verbose date
	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime))
}

func SysLog(peerId int, term int, topic logTopic, format string, a ...interface{}) {
	topicLevel := getTopicLevel(topic)
	if topicLevel >= logLevel {
		curTime := time.Since(logStart).Microseconds() / 100
		prefix := fmt.Sprintf("%06d T%04d %v S%d ", curTime, term, string(topic), peerId)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func getRole(role Role) string {
	switch role {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "unknown role"
	}
}

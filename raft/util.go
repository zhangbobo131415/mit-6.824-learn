package raft

import "log"

// Debugging
const (
	Debug         = 1
	ElectionState = false
	HEARTBEAT     = false
	TIMER         = false
	TERMSTATE     = false
)

func DEBUG(state bool, format string, a ...interface{}) (n int, err error) {
	if state {
		return DPrintf(format, a...)
	}
	return 0, nil
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

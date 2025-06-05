package timers

/*
Info:
- ELM = entire log message
- sent from leader to follower to help reconcile follower log
- this timer is for the follower to check for ELMs on server that would be sent by a leader
Relies on:
- leader to send ELM in check LEM
- follower periodically checking for ELM
- follower starting this timer when finds an error between self log and leader log
- leader or candidate stopping this timer when becomes a candidate or leader from follower
*/

// don't clear the ELMs bc of case where follower -> cand -> follower and still needs to check

/*
start timer
- yes

reset timer
- yes
- for callback

timer callback
- follower copies the log from message and puts it into own log
- make a method of Log to help with this
*/

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/log"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	CHECK_ENTIRE_LOG_TIME = 5 // in milliseconds
)

/* Package level variable */
var CheckEntireLogTimer *time.Timer

func StartCheckEntireLogTimer(server *rpc.Client, selfNode *node.Node) {
	CheckEntireLogTimer = time.AfterFunc(
		time.Millisecond*time.Duration(CHECK_ENTIRE_LOG_TIME),
		// time.Second*time.Duration(CHECK_ENTIRE_LOG_TIME),
		func() { CheckEntireLogTimerCallback(server, selfNode) },
	)
}

func ResetCheckEntireLogTimer() {
	CheckEntireLogTimer.Reset(time.Millisecond * time.Duration(CHECK_ENTIRE_LOG_TIME))
	// CheckEntireLogTimer.Reset(time.Second * time.Duration(CHECK_ENTIRE_LOG_TIME))
}

func CheckEntireLogTimerCallback(server *rpc.Client, selfNode *node.Node) {
	elm := msgs.ReadEntireLogMessage(server, selfNode.ID)
	if elm.Exists {
		/* Replace self (follower) log w log in message (leader's log) */
		log.Selflog.ReplaceCommitted(elm.Entries)
		fmt.Println("updated self entire log from leader")

		/* Immediately check for that log entry that started this error process */
		CheckAppendEntriesTimer.Stop()
		CheckAppendEntriesTimerCallback(server, selfNode)
	} else {
		/* This timer is only started if the node is expecting this message,
		so if didn't receive it yet, need to check again */
		ResetCheckEntireLogTimer()
	}
}

/*
update roles.go to stop this timer when changing from follower to other role
*/

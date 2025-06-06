package timers

/*
Info:
- LEM = log entry message
- sent from follower to leader (or leader to self) for leader to add attached entry to log
- this timer is for the leader to check for LEMs on server that would be added
Relies on:
- nodes to send LEMs
- leader periodically checking for LEM
- leader starting this timer when becomes a leader
- follower stopping this timer when becomes a follower from leader
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
	CHECK_LOG_ENT_TIME = 1 // in milliseconds
)

/* Package level variable */
var CheckLogEntryTimer *time.Timer

func StartCheckLogEntryTimer(server *rpc.Client, selfNode *node.Node) {
	CheckLogEntryTimer = time.AfterFunc(
		time.Millisecond*time.Duration(CHECK_LOG_ENT_TIME),
		// time.Second*time.Duration(CHECK_LOG_ENT_TIME),
		func() { CheckLogEntryTimerCallback(server, selfNode) },
	)
}

func ResetCheckLogEntryTimer() {
	CheckLogEntryTimer.Reset(time.Millisecond * time.Duration(CHECK_LOG_ENT_TIME))
	// CheckLogEntryTimer.Reset(time.Second * time.Duration(CHECK_LOG_ENT_TIME))
}

func CheckLogEntryTimerCallback(server *rpc.Client, selfNode *node.Node) {
	readAgain := true
	for readAgain {
		lem := msgs.ReadLogEntryMessage(server, selfNode.ID)
		if lem.Exists {
			// log.Selflog.AddToPending(lem.Entry)
			newWaitingEntry := log.Selflog.StartAppendEntryProcess(lem.Entry)
			fmt.Println("leader starting add log entry process - from server struct")
			if newWaitingEntry {
				/* Update the latest entry in LogMatchCounter struct */
				lmm := msgs.LogMatchMessage{
					SourceID:    selfNode.ID,
					LatestEntry: lem.Entry,
				}
				msgs.SendLMResetEntry(server, lmm)
				/* Start count log match timer */
				ResetCountLogMatchesTimer()
			}
		}
		readAgain = lem.MorePresent
		if readAgain {
			fmt.Println("CheckLogEntryTimerCallback(): reading LEM again")
		}
	}
}

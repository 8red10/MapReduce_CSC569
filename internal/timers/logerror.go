package timers

/*
Info:
- LEM = log error message
- sent from follower to leader to indicate follower wants help fixing their log
- this timer is for the leader to check for LEMs on server that would be sent by a follower
Relies on:
- follower to send LEM in check AEM
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
	CHECK_LOG_ERR_TIME = 5 // in milliseconds
)

/* Package level variable */
var CheckLogErrorTimer *time.Timer

func StartCheckLogErrorTimer(server *rpc.Client, selfNode *node.Node) {
	CheckLogErrorTimer = time.AfterFunc(
		time.Millisecond*time.Duration(CHECK_LOG_ERR_TIME),
		// time.Second*time.Duration(CHECK_LOG_ERR_TIME),
		func() { CheckLogErrorTimerCallback(server, selfNode) },
	)
}

func ResetCheckLogErrorTimer() {
	CheckLogErrorTimer.Reset(time.Millisecond * time.Duration(CHECK_LOG_ERR_TIME))
	// CheckLogErrorTimer.Reset(time.Second * time.Duration(CHECK_LOG_ERR_TIME))
}

func CheckLogErrorTimerCallback(server *rpc.Client, selfNode *node.Node) {
	readAgain := true
	for readAgain {
		lem := msgs.ReadLogErrorMessage(server, selfNode.ID)
		if lem.Exists {
			elm := msgs.EntireLogMessage{
				TargetID: lem.SourceID,
				SourceID: selfNode.ID,
				Entries:  log.Selflog.GetCommittedCopy(),
				Exists:   true,
			}
			msgs.SendEntireLogMessage(server, elm)
		}
		readAgain = lem.MorePresent
		if readAgain {
			fmt.Println("CheckLogErrorTimerCallback(): reading LEM again")
		}
	}

	ResetCheckLogErrorTimer()
}

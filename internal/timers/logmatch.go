package timers

/*
Info:
- message sent from follower to leader
- only leader needs a timer to check
- will commit current entry to log if have enough approval
- timer started via this callback in becomeleader

Relies on:
- follower sending message after receives good AEM
- leader calling this callback when becomes leader
- follower stopping this timer when becomes follower
- there is a way that the latest entry trying to be committed is being kept track of
- there is a pending queue of entries to add to the log if the latest entry is still trying to be added
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
	COUNT_LOG_MATCH_TIME = 5 // in milliseconds
)

/* Package level variables */
var CountLogMatchesTimer *time.Timer

/* Creates the timer */
func StartCountLogMatchesTimer(server *rpc.Client, selfNode *node.Node) {
	CountLogMatchesTimer = time.AfterFunc(
		time.Millisecond*time.Duration(COUNT_LOG_MATCH_TIME),
		// time.Second*time.Duration(3*COUNT_LOG_MATCH_TIME/10),
		func() { CountLogMatchesTimerCallback(server, selfNode) },
	)
}

/* Resets the timer */
func ResetCountLogMatchesTimer() {
	CountLogMatchesTimer.Reset(time.Millisecond * time.Duration(COUNT_LOG_MATCH_TIME))
	// CheckLogErrorTimer.Reset(time.Second * time.Duration(COUNT_LOG_MATCH_TIME))
}

/* Count the amount of approval for the current log entry - timer started via this callback in becomeleader */
func CountLogMatchesTimerCallback(server *rpc.Client, selfNode *node.Node) {
	fmt.Println("counting log matches...")
	matchCount := msgs.CountLogMatches(server, selfNode.ID, log.Selflog.Waitingentry)
	fmt.Printf("got %d matches: ", matchCount)
	countAgain := true
	if matchCount > node.NUM_NODES/2 {
		/* Case 1: leader got majority approval for entry, commit to log if haven't already committed */
		countAgain = log.Selflog.CommitWaitingEntry()
		lmm := msgs.LogMatchMessage{
			SourceID:    selfNode.ID,
			LatestEntry: log.Selflog.GetWaitingEntry(),
		}
		msgs.SendLMResetEntry(server, lmm)
		fmt.Println("committing current entry")
	} else {
		/* Case 2: leader didn't get majority approval yet, wait for next callback */
		fmt.Println("not committing current entry")
	}

	/* try to count log matches again if there is a waiting entry (either entry is the same one or a new one exists) */
	if countAgain {
		StartCountLogMatchesTimer(server, selfNode)
	}
}

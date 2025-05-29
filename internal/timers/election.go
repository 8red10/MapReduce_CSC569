package timers

import (
	"math/rand"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	ELECTION_TIME_MAX = 300 // max time before follower becomes candidate, in milliseconds
	ELECTION_TIME_MIN = 150 // min time before follower becomes candidate, in milliseconds
)

/* Package level variables */
var ElectionTimer *time.Timer

/* Calculate time before needing to hold an election */
func calcElectionTime(min int, max int) int {
	return rand.Intn(max-min) + min
}

/* Creates the timer */
func StartElectionTimer(server *rpc.Client, selfNode *node.Node) {
	ElectionTimer = time.AfterFunc(
		time.Millisecond*time.Duration(calcElectionTime(ELECTION_TIME_MIN, ELECTION_TIME_MAX)),
		// time.Second*time.Duration(calcElectionTime(ELECTION_TIME_MIN/10, ELECTION_TIME_MAX/10)),
		func() { electionTimerCallback(server, selfNode) })
}

/*
Relies on the timer already being created.
*/
func ResetElectionTimer() {
	ElectionTimer.Reset(time.Millisecond * time.Duration(calcElectionTime(ELECTION_TIME_MIN, ELECTION_TIME_MAX)))
	// electionTimer.Reset(time.Second * time.Duration(calcElectionTime(ELECTION_TIME_MIN/5, ELECTION_TIME_MAX/5)))
	// electionTimer.Reset(time.Second * time.Duration(30))
}

/*
Called when the election timer expires, essentially becomes a candidate (only scenario to become a candidate).
Part of follower and candidate roles.
*/
func electionTimerCallback(server *rpc.Client, selfNode *node.Node) {
	BecomeCandidate(server, selfNode)
}

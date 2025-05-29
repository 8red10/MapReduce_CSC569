package timers

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	COUNT_VOTES_TIME = 50 // delay between candidate sending vote req and counting votes, in milliseconds
)

/* Package level variables */
var CountVotesTimer *time.Timer

/* Creates the timer */
func StartCountVotesTimer(server *rpc.Client, selfNode *node.Node) {
	CountVotesTimer = time.AfterFunc(
		time.Millisecond*time.Duration(COUNT_VOTES_TIME),
		// time.Second*time.Duration(3*COUNT_VOTES_TIME/10),
		func() { CountVotesTimerCallback(server, selfNode) },
	)
}

/*
Relies on the timer already being created.
*/
func ResetCountVotesTimer() {
	CountVotesTimer.Reset(time.Millisecond * time.Duration(COUNT_VOTES_TIME))
}

/*
Count the votes addressed to self during this term - part of candidate role.
Can become a leader (only scenario to become a leader).
*/
func CountVotesTimerCallback(server *rpc.Client, selfNode *node.Node) {

	fmt.Print("counting votes... ")
	voteCount := msgs.CountVotes(server, selfNode.ID, selfNode.GetTerm())
	fmt.Printf("got %d votes: ", voteCount)
	if voteCount > node.NUM_NODES/2 { // shared.NUM_NODES/2+1
		/* Case 1: candidate received vote from majority of nodes, become leader */
		BecomeLeader(server, selfNode)
	} else {
		/* Case 2: candidate didn't receive majority vote, wait for next election timeout */
		fmt.Println("not becoming leader")
	}
}

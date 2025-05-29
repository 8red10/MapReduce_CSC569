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

/*
Creates a timer that can now be reset instead of initially started.
Is so that this timer can be passed to a function instead of being returned from one.
*/
func CreateCountVotesTimer(server *rpc.Client, selfNode *node.Node, electionTimer *time.Timer) *time.Timer {
	countVotesTimer := time.AfterFunc(
		time.Millisecond*time.Duration(COUNT_VOTES_TIME),
		// time.Second*time.Duration(3*COUNT_VOTES_TIME/10),
		func() { CountVotesTimerCallback(server, selfNode, electionTimer) },
	)
	countVotesTimer.Stop()
	return countVotesTimer
}

func StartCountVotesTimer(server *rpc.Client, selfNode *node.Node, electionTimer *time.Timer) *time.Timer {
	return time.AfterFunc(
		time.Millisecond*time.Duration(COUNT_VOTES_TIME),
		// time.Second*time.Duration(3*COUNT_VOTES_TIME/10),
		func() { CountVotesTimerCallback(server, selfNode, electionTimer) },
	)
}

/*
Relies on the timer already being created.
*/
func ResetCountVotesTimer(countVotesTimer *time.Timer) {
	countVotesTimer.Reset(time.Millisecond * time.Duration(COUNT_VOTES_TIME))
}

/*
Count the votes addressed to self during this term - part of candidate role.
Can become a leader (only scenario to become a leader).
*/
func CountVotesTimerCallback(server *rpc.Client, selfNode *node.Node, electionTimer *time.Timer) {

	fmt.Print("counting votes... ")
	voteCount := msgs.CountVotes(server, selfNode.ID, selfNode.GetTerm())
	fmt.Printf("got %d votes: ", voteCount)
	if voteCount > node.NUM_NODES/2 { // shared.NUM_NODES/2+1
		/* Case 1: candidate received vote from majority of nodes, become leader */
		BecomeLeader(server, selfNode, electionTimer)
	} else {
		/* Case 2: candidate didn't receive majority vote, wait for next election timeout */
		fmt.Println("not becoming leader")
	}
}

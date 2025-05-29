package timers

import (
	"fmt"
	"net/rpc"

	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	FOLLOWER       = 0
	CANDIDATE      = 1
	LEADER         = 2
	DEBUG_MESSAGES = false
)

/* Change role to follower */
func BecomeFollower(selfNode *node.Node, newTerm int) {

	fmt.Println("becoming follower... ")

	if selfNode.GetRole() == LEADER {
		SendAppendEntriesTimer.Stop() // only need to stop aem if coming from leader
	}
	if role := selfNode.GetRole(); role == CANDIDATE {
		CountVotesTimer.Stop() // only need to stop count votes if coming from candidate
	}
	ResetElectionTimer()
	selfNode.UpdateRoleTo(FOLLOWER)
	selfNode.UpdateTermTo(newTerm)
}

/* Change role to candidate */
func BecomeCandidate(server *rpc.Client, selfNode *node.Node) {

	if DEBUG_MESSAGES {
		fmt.Println("want to become candidate (election timer done), now checking VR and AEM...")
	}
	receivedGoodVR := CheckVoteRequestTimerCallback(server, selfNode)    // receiving a good vr means to reset election timer
	receivedGoodAEM := CheckAppendEntriesTimerCallback(server, selfNode) // receiveing a good aem means to reset election timer
	if !receivedGoodVR && !receivedGoodAEM {

		fmt.Printf("becoming candidate, from term %d ", selfNode.GetTerm())
		selfNode.UpdateRoleTo(CANDIDATE)
		selfNode.UpdateTermTo(selfNode.GetTerm() + 1)
		fmt.Printf("to term %d\n", selfNode.GetTerm())

		vm := msgs.VoteMessage{TargetID: selfNode.ID, Term: selfNode.GetTerm()}
		msgs.SendVoteMessage(server, vm)
		for i := range node.NUM_NODES {
			id := i + 1
			if id != selfNode.ID {
				vr := msgs.VoteRequest{TargetID: id, Term: selfNode.GetTerm(), SourceID: selfNode.ID, Exists: true} // need .exists
				msgs.SendVoteRequest(server, vr)
			}
		}

		// TODO - make a StartCountVotesTimer function?, then can use that here and return the timer
		// then return the timer created at the end of this (BecomeCandidate) function

		// CountVotesTimer = time.AfterFunc(
		// 	time.Millisecond*time.Duration(COUNT_VOTES_TIME),
		// 	// time.Second*time.Duration(3*COUNT_VOTES_TIME/10),
		// 	func() { CountVotesTimerCallback(server, selfNode) },
		// )

		StartCountVotesTimer(server, selfNode)

		ResetElectionTimer()
	} else if receivedGoodVR {
		if DEBUG_MESSAGES {
			fmt.Printf("received good vr, not becoming a candidate, my role: %d\n", selfNode.GetRole())
		}
	} else if receivedGoodAEM {
		if DEBUG_MESSAGES {
			fmt.Printf("received good aem, not becoming a candidate, my role: %d\n", selfNode.GetRole())
		}
	}
}

/* Change role to leader */
func BecomeLeader(server *rpc.Client, selfNode *node.Node) {
	fmt.Println("becoming leader...")
	ElectionTimer.Stop() // auto starts this timer in the beginning so don't need to protect this stop
	selfNode.UpdateRoleTo(LEADER)
	go SendAppendEntriesTimerCallback(server, selfNode)
}

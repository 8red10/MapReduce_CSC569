package timers

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/log"
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

	fmt.Print("becoming follower... ")

	if selfNode.GetRole() == LEADER {
		SendAppendEntriesTimer.Stop() // only need to stop sending aem if transitioning role from leader
		CountLogMatchesTimer.Stop()   // only need to stop counting log matches if coming from leader
		CheckLogErrorTimer.Stop()     // only need to stop if coming from leader
		CheckLogEntryTimer.Stop()     // only need to stop if coming from leader
		log.Selflog.ClearPending()    // don't carry the pending to follower status
	}
	if role := selfNode.GetRole(); role == CANDIDATE {
		CountVotesTimer.Stop() // only need to stop count votes if coming from candidate
	}
	ResetElectionTimer()
	selfNode.UpdateRoleTo(FOLLOWER)
	selfNode.UpdateTermTo(newTerm)
	if DEBUG_MESSAGES {
		fmt.Printf("becoming follower(): updated term to %d\n", newTerm)
		fmt.Println("becoming follower2... ")
	} else {
		fmt.Printf("for term %d\n", selfNode.GetTerm())
	}
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
	fmt.Printf("becoming leader... for term %d\n", selfNode.GetTerm())
	ElectionTimer.Stop()       // auto starts this timer in the beginning so don't need to protect this stop
	CheckEntireLogTimer.Stop() // auto starts (then stops) this timer in the beginning so don't need to protect this stop
	// StartCountLogMatchesTimer(server, selfNode) // should only start when an entry is being proposed
	StartCheckLogErrorTimer(server, selfNode)
	selfNode.UpdateRoleTo(LEADER)
	if DEBUG_MESSAGES {
		fmt.Printf(" at term %d", selfNode.GetTerm())
		fmt.Println("becoming leader2...")
	}
	go SendAppendEntriesTimerCallback(server, selfNode)

	/* TESTING = after 50 ms, add to self log - see what happens */
	entry := log.NewLogEntry(
		true,
		log.NewMapReduceData(-1),
	)
	time.AfterFunc(
		time.Millisecond*time.Duration(50), // after 50 ms
		func() { LeaderStartAddLogEntryProcess(server, selfNode, entry) },
	)
	time.AfterFunc(
		time.Millisecond*time.Duration(100), // after 100 ms
		func() { LeaderStartAddLogEntryProcess(server, selfNode, entry) },
	)
}

/* leader function, add entry to log */
func LeaderStartAddLogEntryProcess(server *rpc.Client, selfNode *node.Node, entry log.LogEntry) {
	if selfNode.GetRole() == LEADER && entry.Exists {
		newWaitingEntry := log.Selflog.StartAppendEntryProcess(entry)
		fmt.Println("leader starting add log entry process - forced")
		if newWaitingEntry {
			/* Update the latest entry in LogMatchCounter struct */
			lmm := msgs.LogMatchMessage{
				SourceID:    selfNode.ID,
				LatestEntry: entry,
			}
			msgs.SendLMResetEntry(server, lmm)
			/* Start count log match timer */
			ResetCountLogMatchesTimer()
		}
	}
}

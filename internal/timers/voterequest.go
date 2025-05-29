package timers

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	CHECK_VOTE_REQUEST_TIME = 5 // delay between client (also candidate) checking for vote requests, in milliseconds
)

/* Package level variables */
var CheckVoteRequestTimer *time.Timer

/* Start timer to check for vote requests */
func StartVoteRequestTimer(server *rpc.Client, selfNode *node.Node) {
	CheckVoteRequestTimer = time.AfterFunc(
		time.Millisecond*time.Duration(CHECK_VOTE_REQUEST_TIME),
		// time.Second*time.Duration(CHECK_VOTE_REQUEST_TIME),
		func() { CheckVoteRequestTimerCallback(server, selfNode) },
	)
}

/* Reset timer */
func ResetVoteRequestTimer() {
	CheckVoteRequestTimer.Reset(time.Millisecond * time.Duration(CHECK_VOTE_REQUEST_TIME))
	// checkVoteRequestTimer.Reset(time.Second * time.Duration(CHECK_VOTE_REQUEST_TIME))
}

/* Process vote request for self node */
func CheckVoteRequestTimerCallback(server *rpc.Client, selfNode *node.Node) bool {
	if DEBUG_MESSAGES {
		fmt.Print("Checking VR... ")
	}
	receivedGoodVR := false
	if vr := msgs.ReadVoteRequest(server, selfNode.ID); vr.Exists {

		if DEBUG_MESSAGES {
			fmt.Printf("got VR from %d during term %d\n", vr.SourceID, vr.Term)
		}
		if vr.Term > selfNode.GetTerm() { // TODO - consolidate the contents of this if statement ?
			receivedGoodVR = true

			/* Case 1: vote request term > self term, check role */
			switch selfNode.GetRole() {

			case LEADER:
				selfNode.UpdateTermTo(vr.Term)

			case CANDIDATE:
				selfNode.UpdateTermTo(vr.Term)
				vm := msgs.VoteMessage{TargetID: vr.SourceID, Term: selfNode.GetTerm()}
				msgs.SendVoteMessage(server, vm)
				ResetElectionTimer()

			case FOLLOWER:
				selfNode.UpdateTermTo(vr.Term)
				vm := msgs.VoteMessage{TargetID: vr.SourceID, Term: selfNode.GetTerm()}
				msgs.SendVoteMessage(server, vm)
				ResetElectionTimer()

			default:
				fmt.Println("ERROR: checkVRTC(): bad role")
			}
			/* Case 2: vote request term <= self term, do nothing bc already voted during this term */

		} else {
			if DEBUG_MESSAGES {
				fmt.Println("VR received from a previous term.")
			}
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Println("no VR received.")
		}
	}
	ResetCheckAppendEntriesTimer()

	return receivedGoodVR
}

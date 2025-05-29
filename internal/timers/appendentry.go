package timers

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	CHECK_LEADER_HEARTBEAT_TIME = 5 // delay between non-leader checking for leader heartbeats, in milliseconds
	LEADER_HEARTBEAT_TIME       = 5 // delay between leader hearbeats, in milliseconds
)

/* Package level variables */
var CheckAppendEntriesTimer *time.Timer
var SendAppendEntriesTimer *time.Timer

/* Returns the check aem time - timer to check for leader heartbeats */
func StartCheckAppendEntriesTimer(server *rpc.Client, selfNode *node.Node) {
	CheckAppendEntriesTimer = time.AfterFunc(
		time.Millisecond*time.Duration(CHECK_LEADER_HEARTBEAT_TIME),
		// time.Second*time.Duration(CHECK_LEADER_HEARTBEAT_TIME),
		func() { CheckAppendEntriesTimerCallback(server, selfNode) },
	)
}

/* Resets timer */
func ResetCheckAppendEntriesTimer() {
	CheckAppendEntriesTimer.Reset(time.Millisecond * time.Duration(CHECK_LEADER_HEARTBEAT_TIME))
	// checkAppendEntriesTimer.Reset(time.Second * time.Duration(CHECK_LEADER_HEARTBEAT_TIME))
}

/* Process leader heartbeats addressed to self node - part of follower, candidate, and leader role */
func CheckAppendEntriesTimerCallback(server *rpc.Client, selfNode *node.Node) bool {
	if DEBUG_MESSAGES {
		fmt.Print("Checking AEM...")
	}
	receivedGoodAEM := false // good aem means that we don't need to have this node become a candidate

	if msg := msgs.ReadAppendEntryMessage(server, selfNode.ID); msg.Exists { // TODO - consolidate the contents of this if block ?

		if DEBUG_MESSAGES {
			fmt.Printf("got AEM during term %d\n", msg.Term)
		}
		if selfNode.GetRole() == LEADER {
			/* Case 1: self is leader */
			if msg.Term > selfNode.GetTerm() {
				/* Case 1a: other leader w greater term, become follower */
				BecomeFollower(selfNode, msg.Term)
				receivedGoodAEM = true

			} else if msg.Term == selfNode.GetTerm() {
				/* Case 1b: other leader w same term, error occurred w voting */
				fmt.Printf("ERROR: bad voting - detected another leader w same term: %d\n", selfNode.GetTerm())
			}
			/* Case 1c: other leader w lesser term, do nothing and other leader should notice and become follower */
			// bc other leader will receive aem from this node and recognize a higher term and demote itself

		} else if selfNode.GetRole() == CANDIDATE {
			/* Case 2: self is candidate */
			BecomeFollower(selfNode, msg.Term)
			receivedGoodAEM = true
		} else {
			/* Case 3: self is follower */
			if msg.Term > selfNode.GetTerm() {
				/* Case 3a: leader w greater term, stay follower, update term and reset election timer */
				ResetElectionTimer()
				selfNode.UpdateTermTo(msg.Term)
				receivedGoodAEM = true

			} else if msg.Term == selfNode.GetTerm() {
				/* Case 3b: leader w same term, stay follower, reset election timer */
				ResetElectionTimer()
				receivedGoodAEM = true
			}
			/* Case 3c: leader term < self term, do nothing and leader should eventually become follower */
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Println("no AEM received.")
		}
	}
	ResetCheckAppendEntriesTimer()

	return receivedGoodAEM
}

/* Send leader heartbeat to all other nodes - part of leader role - called upon role transition */
func SendAppendEntriesTimerCallback(server *rpc.Client, selfNode *node.Node) {

	/* Send leader heartbeat to all other nodes */
	for i := range node.NUM_NODES {
		id := i + 1
		if id != selfNode.ID {
			aem := msgs.AppendEntryMessage{TargetID: id, Term: selfNode.GetTerm(), Exists: true} // need exists
			go msgs.SendAppendEntryMessage(server, aem)
		}
	}

	/* Start timer to send leader heartbeat again */
	SendAppendEntriesTimer = time.AfterFunc(
		time.Millisecond*time.Duration(LEADER_HEARTBEAT_TIME),
		// time.Second*time.Duration(LEADER_HEARTBEAT_TIME),
		func() { SendAppendEntriesTimerCallback(server, selfNode) },
	)
}

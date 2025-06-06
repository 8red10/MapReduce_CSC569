package timers

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	CHECK_LEADER_HEARTBEAT_TIME = 20 // delay between non-leader checking for leader heartbeats, in milliseconds
	LEADER_HEARTBEAT_TIME       = 20 // delay between leader hearbeats, in milliseconds
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
				sendResponsetoAEM(server, msg)

			} else if msg.Term == selfNode.GetTerm() {
				/* Case 3b: leader w same term, stay follower, reset election timer */
				ResetElectionTimer()
				receivedGoodAEM = true
				sendResponsetoAEM(server, msg)
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

/* compares entries in received aem with self log - send appropriate response */
func sendResponsetoAEM(server *rpc.Client, aem msgs.AppendEntryMessage) {
	leaderPrevEntry := aem.PreviousEntry
	leaderNewEntry := aem.NewEntry
	followerPrevEntry := logs.Selflog.GetLastCommitted()

	if DEBUG_MESSAGES {
		fmt.Printf("FPE=(%t %d %d), LNE=(%t %d %d), LPE=(%t %d %d)\n",
			followerPrevEntry.Exists, followerPrevEntry.GetIndex(), followerPrevEntry.GetTerm(),
			leaderNewEntry.Exists, leaderNewEntry.GetIndex(), leaderNewEntry.GetTerm(),
			leaderPrevEntry.Exists, leaderPrevEntry.GetIndex(), leaderPrevEntry.GetTerm(),
		)
	}

	if leaderNewEntry.Exists && !followerPrevEntry.MatchesAndBothExist(leaderNewEntry) {
		/* Case 1: leader trying to update log and follower not appended new entry yet */
		if followerPrevEntry.MatchesAndBothExist(leaderPrevEntry) {
			/* Case 1a: follower log up to date */
			fmt.Println("self (fol.) log adding leader's new proposed entry bc prev log match")
			indicateLogMatch(server, aem)
		} else if !followerPrevEntry.Exists && !leaderPrevEntry.Exists {
			/* Case 1b: follower and leader don't have anything committed yet */
			fmt.Println("self (fol.) log adding leader new proposed entry bc neither committed")
			indicateLogMatch(server, aem)
		} else {
			/* Case 1c: log mismatch */
			fmt.Println("self (fol.) log mismatch leader's, sending log error")
			indicateLogError(server, aem)
		}
	} else if !leaderNewEntry.Exists && leaderPrevEntry.Exists && !followerPrevEntry.MatchesAndBothExist(leaderPrevEntry) {
		/* Case 2: leader not trying to add new, leader has existing entries, but FPE doesn't match LPE */
		fmt.Println("self (fol.) log mismatch leader's, LNE false, LPE true, is log err")
		indicateLogError(server, aem)
	} else {
		if DEBUG_MESSAGES {
			fmt.Println("self (follower) log up to date w leader's log")
		}
	}
}

/* sends log match message and adds new entry to follower log */
func indicateLogMatch(server *rpc.Client, aem msgs.AppendEntryMessage) {
	msg := msgs.LogMatchMessage{
		TargetID:    aem.SourceID,
		SourceID:    aem.TargetID,
		LatestEntry: aem.NewEntry,
	}
	msgs.SendLogMatchMessage(server, msg)

	logs.Selflog.StartAppendEntryProcess(aem.NewEntry)
	logs.Selflog.CommitWaitingEntry()

	logs.Selflog.PrintLog()
}

/* sends log error message */
func indicateLogError(server *rpc.Client, aem msgs.AppendEntryMessage) {
	msg := msgs.LogErrorMessage{
		TargetID:    aem.SourceID,
		SourceID:    aem.TargetID,
		Exists:      true,
		MorePresent: false, // to be changed by server if necessary
	}
	msgs.SendLogErrorMessage(server, msg)
	ResetCheckEntireLogTimer()
}

// /* compares entries in received aem with self log - send appropriate response */
// func compareEntries(server *rpc.Client, aem msgs.AppendEntryMessage) {
// 	leaderPrevEntry := aem.PreviousEntry
// 	leaderNewEntry := aem.NewEntry
// 	followerPrevEntry := log.Selflog.GetLastCommitted()
// 	if leaderNewEntry.Exists {
// 		/* Case 1: leader is trying to append an entry, check if follower log up to date */
// 		if !leaderPrevEntry.Exists && !followerPrevEntry.Exists {
// 			/* Case 1a: leader and follower don't have prev entries, indicate match and add entry to follower log*/
// 			indicateLogMatch(server, aem)
// 		} else if leaderPrevEntry.Exists && followerPrevEntry.Exists {
// 			/* Case 1b: leader and follower have prev entries, compare them */
// 			if leaderPrevEntry.MatchesAndBothExist(followerPrevEntry) {
// 				/* Case 1ba: leader and follower entries match, send log match msg and add entry to follower log */
// 				indicateLogMatch(server, aem)
// 			} else {
// 				/* Case 1bb: leader and follower entries don't match, send log error msg */
// 				indicateLogError(server, aem)
// 			}
// 		} else if leaderPrevEntry.Exists {
// 			/* Case 1c: follower doesn't have an entry in log, follower not up to date */
// 			indicateLogError(server, aem)
// 		} else {
// 			/* Case 1d: leader doesn't have an entry in log but follower does, this shouldn't happen, do nothing */
// 			fmt.Println("Weird scenario: leader no log but follower have log in sendResponsetoAEM()")
// 			// possibly happens bc leader elected doesn't have the most up to date logs
// 			// also would happen when follower appended but leader still waiting on approval before committing
// 		}
// 	}
// 	/* Case 2: new entry doesn't exist, do nothing */
// }

/* Send leader heartbeat to all other nodes - part of leader role - called upon role transition */
func SendAppendEntriesTimerCallback(server *rpc.Client, selfNode *node.Node) {

	/* Send leader heartbeat to all other nodes */
	for i := range node.NUM_NODES {
		id := i + 1
		if id != selfNode.ID {
			aem := msgs.AppendEntryMessage{
				TargetID:      id,
				SourceID:      selfNode.ID,
				Term:          selfNode.GetTerm(),
				Exists:        true, // need exists
				PreviousEntry: logs.Selflog.GetLastCommitted(),
				NewEntry:      logs.Selflog.GetWaitingEntry(),
			}
			// fmt.Printf("SAETC w LNE %t %d %d, LPE %t %d %d\n",
			// 	aem.PreviousEntry.Exists, aem.PreviousEntry.Index, aem.PreviousEntry.Term,
			// 	aem.NewEntry.Exists, aem.NewEntry.Index, aem.NewEntry.Term,
			// )
			// fmt.Printf("SAETC w LNE %d \n", aem.PreviousEntry.Term) //%d %d, LPE %t %d %d\n",
			// // aem.PreviousEntry.Exists, aem.PreviousEntry.Index, aem.PreviousEntry.Term,
			// // aem.NewEntry.Exists, aem.NewEntry.Index, aem.NewEntry.Term,
			// // )
			// fmt.Println()
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

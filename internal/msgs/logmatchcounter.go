package msgs

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

/* message - indicates approval for AppendEntry proposal */
type LogMatchMessage struct {
	TargetID int // id of leader, to send message to - TODO - check if we actually need this
	SourceID int // id of node sending message
	// need the new entry here for validation and reset of LogMatchCounter ?
	// need some sort of validation here bc don't want late LogMatchCounter to interfere w current
	// Index int // latest index that the follower's log matches up to leader log
	LatestEntry logs.LogEntry // entry being approved by follower
}

/* server struct - counts follower approvals for leader's AppendEntry proposal */
type LogMatchCounter struct {
	mu *sync.RWMutex // enables thread-safe operations
	// Index int           // latest index in log being matched
	LatestEntry logs.LogEntry // entry being approved by followers
	Mailbox     map[int]bool  // map[sourceID]source_node_approval_of_this_entry
}

func NewLogMatchCounter() *LogMatchCounter {
	return &LogMatchCounter{
		mu: new(sync.RWMutex),
		LatestEntry: logs.NewLogEntry(
			false,
			logs.NewMapReduceData(-1),
		),
		Mailbox: make(map[int]bool),
	}
}

/*
Reset the latest entry in LogMatchCounter for new AppendEntry proposals - RPC ok.

not sure where to apply this function yet - it will be in the count approval function // TODO

For use by the leader who wishes to start another AppendEntry proposal
Also automatically adds approval for the leader who is resetting latest entry.
payload = LogMatchMessage containing message to propose by putting into LogMatchCounter.
reply = success flag (true = added ,false = didn't add).
*/
func (lm *LogMatchCounter) ResetLatestEntry(payload LogMatchMessage, reply *bool) error {
	if payload.SourceID < 1 || payload.SourceID > node.NUM_NODES {
		*reply = false
		return errors.New("LogMatchCounter.ResetLatestEntry(): Source ID out of range")
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.LatestEntry = payload.LatestEntry
	for i := range node.NUM_NODES {
		lm.Mailbox[i+1] = false
	}
	lm.Mailbox[payload.SourceID] = true
	*reply = true

	return nil
}

/*
Add approval for the indicated entry - RPC ok.
Part of the sendLogMatchMessage client operation.
Only adds approval if the latest entry in LogMatchCounter matches the messages's latest entry.
Adds approval with source ID so follower nodes can only approve once.
payload = LogMatchMessage with info on how to approve.
reply = success flag (true = added, false = didn't add).
*/
func (lm *LogMatchCounter) Add(payload LogMatchMessage, reply *bool) error {
	if payload.SourceID < 1 || payload.SourceID > node.NUM_NODES {
		*reply = false
		return errors.New("LogMatchCounter.Add(): Source ID out of range")
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if payload.LatestEntry.MatchesAndBothExist(lm.LatestEntry) {
		lm.Mailbox[payload.SourceID] = true
		*reply = true
	} else {
		*reply = false
	}

	return nil
}

/*
Get the approval count for the message in LogMatchCounter - RPC ok.
Doesn't reset approvals for majority count.
Relies on leader reseting the counter when seeking approval for the next entry to append.
sourceID = id of leader that is counting the approval messages.
reply = approval count in LogMatchCounter.
*/
func (lm *LogMatchCounter) Listen(payload LogMatchMessage, reply *int) error {
	if payload.SourceID < 1 || payload.SourceID > node.NUM_NODES {
		*reply = 0
		return errors.New("LogMatchCounter.Listen(): Source ID out of range")
	}

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if lm.LatestEntry.MatchesAndBothExist(payload.LatestEntry) {
		count := 0
		for i := range node.NUM_NODES {
			if lm.Mailbox[i+1] {
				count += 1
			}
		}
		*reply = count
	} else {
		*reply = 0
		return errors.New("LogMatchCounter.Listen(): payload entry doesn't match")
	}

	return nil
}

/*
Send follower approval to leader saying log matches
*/
func SendLogMatchMessage(server *rpc.Client, msg LogMatchMessage) {
	var added bool
	if err := server.Call("LogMatchCounter.Add", msg, &added); err != nil {
		fmt.Println("ERROR: LogMatchCounter.Add():", err)
	} else if added {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: LogMatchMessage sent to node %d\n", msg.TargetID)
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: LogMatchMessage NOT sent to node %d\n", msg.TargetID)
		} else {
			fmt.Printf("LMM NOT sent to node %d = LMC entry already approved before self vote\n", msg.TargetID) // added more info on the error
		}
	}
}

/* uses RPC to reset latest entry on in the log match counter struct */
func SendLMResetEntry(server *rpc.Client, lmm LogMatchMessage) {
	var added bool
	if err := server.Call("LogMatchCounter.ResetLatestEntry", lmm, &added); err != nil {
		fmt.Println("ERROR: LogMatchCounter.ResetLatestEntry():", err)
	} else if added {
		if DEBUG_MESSAGES {
			fmt.Println("OK: reset entry in LogMatchCounter")
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Println("ERROR: entry NOT reset in LogMatchCounter")
		} else {
			fmt.Println("ERROR: entry NOT reset in LogMatchCounter")
		}
	}
}

/*
Count approval for the proposed log entry to append.
Part of leader role.
server = RPC connection to server.
sourceID = self node ID.
entry = entry to count approval for.
*/
func CountLogMatches(server *rpc.Client, sourceID int, entry logs.LogEntry) int {

	lmm := LogMatchMessage{SourceID: sourceID, LatestEntry: entry}
	count := 0
	if err := server.Call("LogMatchCounter.Listen", lmm, &count); err != nil {
		fmt.Println("ERROR: LogMatchCounter.Listen():", err)
	}
	return count
}

package msgs

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

/* message - leader sends as a heartbeat to assert leadership to followers and for followers to add to their logs */
type AppendEntryMessage struct {
	TargetID      int           // id of node to send message to
	SourceID      int           // id of leader sending the AppendEntry proposal
	Term          int           // current term of leader
	Exists        bool          // when reading: true if a AppendEntryMessage exists in server struct, false otherwise
	PreviousEntry logs.LogEntry // previously committed log entry for consistency checks
	NewEntry      logs.LogEntry // entry being proposed to be committed
}

/* server struct - holds all append entry messages for followers to read */
type AppendEntryMessages struct {
	mu      *sync.RWMutex              // enables concurrent thread operations
	Term    int                        // current term number
	Mailbox map[int]AppendEntryMessage // map[targetID]leader_heartbeat
}

/* Returns a pointer to a new AppendEntryMessages */
func NewAppendEntryMessages() *AppendEntryMessages {
	return &AppendEntryMessages{
		mu:      new(sync.RWMutex),
		Term:    0,
		Mailbox: make(map[int]AppendEntryMessage),
	}
}

/*
Add leader heartbeat message to server struct - RPC ok.
Part of sendAppendEntryMessage client operation.
Updates AppendEntryMessages term number with newest term received.
Relies on leader to properly initialize AppendEntryMessage.
payload - heartbeat of leader.
reply - success flag (true = added, false = not added)
*/
func (ae *AppendEntryMessages) Add(payload AppendEntryMessage, reply *bool) error {

	/* Check ID within range */
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = false
		return errors.New("AppendEntryMessages.Add(): target ID out of range")
	}

	/* Add leader heartbeat */
	ae.mu.Lock()
	defer ae.mu.Unlock()
	if ae.Term < payload.Term {
		/* Case 1: server term < leader term, clear all existing messages and add current leader message and update server term */
		for i := range node.NUM_NODES {
			delete(ae.Mailbox, i+1)
		}
		ae.Mailbox[payload.TargetID] = payload
		ae.Term = payload.Term
		*reply = true
	} else if ae.Term == payload.Term {
		/* Case 2: server term == leader term, add current leader message (replacing if necessary) */
		ae.Mailbox[payload.TargetID] = payload
		*reply = true
	} else {
		/* Case 3: server term > leader term, don't add leader message */
		*reply = false
	}

	/* Indicate success */
	return nil
}

/*
Get the leader heartbeat directed at self node - RPC ok.
Part of readAppendEntryMessage client operation.
Relies on client to check the message term number upon receipt.
Client should discard message if leader term < self term.
Client should process message if leader term >= self term.
sourceID - node id of client checking for AppendEntryMessage.
reply - AppendEntryMessage struct on server at source id.
*/
func (ae *AppendEntryMessages) Listen(sourceID int, reply *AppendEntryMessage) error {

	/* Check ID within range */
	if sourceID < 1 || sourceID > node.NUM_NODES {
		*reply = AppendEntryMessage{Exists: false}
		return errors.New("AppendEntryMessages.Listen(): source ID out of range")
	}

	/* Get leader heartbeat */
	ae.mu.Lock()
	defer ae.mu.Unlock()
	if mes, exists := ae.Mailbox[sourceID]; exists {
		*reply = mes
		delete(ae.Mailbox, sourceID)
	} else {
		*reply = AppendEntryMessage{Exists: false}
	}

	/* Indicate success */
	return nil
}

/*
Send leader heartbeat to target node
*/
func SendAppendEntryMessage(server *rpc.Client, msg AppendEntryMessage) {
	var appendAdded bool
	if err := server.Call("AppendEntryMessages.Add", msg, &appendAdded); err != nil {
		fmt.Println("ERROR: AppendEntryMessages.Add():", err)
	} else if appendAdded {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: AppendEntryMessage sent to node %d\n", msg.TargetID)
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: AppendEntryMessage NOT sent to node %d\n", msg.TargetID)
		} else {
			fmt.Printf("AppendEntryMessage NOT sent to node %d during term %d\n", msg.TargetID, msg.Term)
		}
	}
}

/*
Check server for a leader heartbeat addressed to self node and return it.
Part of follower, candidate, leader roles.
server - RPC connection to server.
sourceID - self node ID.
*/
func ReadAppendEntryMessage(server *rpc.Client, sourceID int) AppendEntryMessage {

	aem := AppendEntryMessage{Exists: false}
	if err := server.Call("AppendEntryMessages.Listen", sourceID, &aem); err != nil {
		fmt.Println("ERROR: AppendEntryMessages.Listen():", err)
	}
	return aem
}

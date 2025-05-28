package msgs

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/log"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

/* message - for passing leader's entire log to fix follower log */
type EntireLogMessage struct {
	TargetID int            // id of follower to send whole log to
	SourceID int            // id of leader
	Entries  []log.LogEntry // whole log of the leader - use GetDeepCopy to fill this attribute
	Exists   bool           // when reading: true if message exists on server, false otherwise
}

/* server struct - holds all entire log messages for followers to read */
type EntireLogMessages struct {
	mu      *sync.RWMutex            // enables thread-safe operations
	Mailbox map[int]EntireLogMessage // log message for follower to replicate from
}

func NewEntireLogMessages() *EntireLogMessages {
	return &EntireLogMessages{
		mu:      new(sync.RWMutex),
		Mailbox: make(map[int]EntireLogMessage),
	}
}

/*
Adds message to struct on server - RPC ok.
Part of client sendEntireLogMessage operation.
payload = message to add.
reply = success flag (true = added, false = didn't add).
*/
func (e *EntireLogMessages) Add(payload EntireLogMessage, reply *bool) error {
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = false
		return errors.New("EntireLogMessages.Add(): Target ID out of range")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.Mailbox[payload.TargetID] = payload
	*reply = true

	return nil
}

/*
Gets message from struct on server - RPC ok.
Removes message from struct upon receipt.
sourceID = id to use to check for a message.
reply = message.
*/
func (e *EntireLogMessages) Listen(sourceID int, reply *EntireLogMessage) error {
	if sourceID < 1 || sourceID > node.NUM_NODES {
		*reply = EntireLogMessage{Exists: false}
		return errors.New("EntireLogMessages.Listen(): Source ID out of range")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if mes, exists := e.Mailbox[sourceID]; exists {
		*reply = mes
		delete(e.Mailbox, sourceID)
	} else {
		*reply = EntireLogMessage{Exists: false}
	}

	return nil
}

/*
Send whole log to follower to help reconcile log
*/
func SendEntireLogMessage(server *rpc.Client, msg EntireLogMessage) {
	var added bool
	if err := server.Call("EntireLogMessages.Add", msg, &added); err != nil {
		fmt.Println("ERROR: EntireLogMessages.Add():", err)
	} else if added {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: EntireLogMessage sent to node %d\n", msg.TargetID)
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: EntireLogMessage NOT sent to node %d\n", msg.TargetID)
		} else {
			fmt.Printf("EntireLogMessage NOT sent to node %d\n", msg.TargetID)
		}
	}
}

/*
Check server for entire log messsage addressed to self node and return it.
Part of follower role.
server = RPC server connection.
sourceID = self node ID.
*/
func ReadEntireLogMessage(server *rpc.Client, sourceID int) EntireLogMessage {

	elm := EntireLogMessage{Exists: false}
	if err := server.Call("EntireLogMessages.Listen", sourceID, &elm); err != nil {
		fmt.Println("ERROR: EntireLogMessages.Listen():", err)
	}
	return elm
}

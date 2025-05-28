package msgs

import (
	"errors"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/node"
)

/* message - indicates error between self log and leader log */
type LogErrorMessage struct {
	TargetID    int  // id of leader
	SourceID    int  // id of follower sending this message
	Exists      bool // when reading: true if error message exists in server struct, false otherwise
	MorePresent bool // when reading: true if more messages besides this one exist in server struct, false otherwise
}

/* server struct - holds error messages for leader to read */
type LogErrorMessages struct {
	mu      *sync.RWMutex           // enables thread-safe operations
	Mailbox map[int]LogErrorMessage // map[sourceID]error_message
}

func NewLogErrorMessages() *LogErrorMessages {
	return &LogErrorMessages{
		mu:      new(sync.RWMutex),
		Mailbox: make(map[int]LogErrorMessage),
	}
}

/*
Adds log error message to server struct - RPC ok.
Part of sendLogErrorMessage client operation.
payload = LogErrorMessage to send.
reply = success flag (true = added, false = didn't add).
*/
func (l *LogErrorMessages) Add(payload LogErrorMessage, reply *bool) error {
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = false
		return errors.New("LogErrorMessages.Add(): Target ID out of range")
	}
	if payload.SourceID < 1 || payload.SourceID > node.NUM_NODES {
		*reply = false
		return errors.New("LogErrorMessages.Add(): Source ID out of range")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// TODO - possibly add functionality to check the term of the leader and store it
	// then would be able to reset these messages when a new leader is elected

	l.Mailbox[payload.SourceID] = payload
	*reply = true

	return nil
}

/*
Gets a LogErrorMessage addressed to the source ID indicated - RPC ok.
Removes message from struct upon receipt.
Relies on leader being able to call this function faster than nodes will error (CHECK_ERROR < (SEND_APPEND / NUM_NODES). // TODO - check this
sourceID = id of leader checking for error messages.
reply = error message for leader.
*/
func (l *LogErrorMessages) Listen(sourceID int, reply *LogErrorMessage) error {
	if sourceID < 1 || sourceID > node.NUM_NODES {
		*reply = LogErrorMessage{Exists: false, MorePresent: false}
		return errors.New("LogErrorMessages.Listen(): Source ID out of range")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	foundMessage := false
	foundAnother := false
	for i := range node.NUM_NODES {
		if mes, exists := l.Mailbox[i+1]; exists {
			if !foundMessage {
				*reply = mes
				delete(l.Mailbox, i+1)
				foundMessage = true
			} else if !foundAnother {
				foundAnother = true
			}
		}
	}
	reply.MorePresent = foundAnother

	return nil
}

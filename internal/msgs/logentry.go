package msgs

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

/* message - holds a log entry for leader to add to log */
type LogEntryMessage struct {
	Entry       logs.LogEntry // entry for leader to add to log
	Exists      bool          // when reading: true if error message exists in server struct, false otherwise
	MorePresent bool          // when reading: true if more messages besides this one exist in server struct, false otherwise
}

/* server struct - holds messages for leader to read - read via sourceID */
type LogEntryMessages struct {
	mu       *sync.RWMutex     // enables thread-safe operations
	Messages []LogEntryMessage // slice holding messages for leader to retreive and add to log
}

func NewLogEntryMessages() *LogEntryMessages {
	return &LogEntryMessages{
		mu:       new(sync.RWMutex),
		Messages: make([]LogEntryMessage, 0, 10),
	}
}

/*
Adds log entry message to server struct - RPC ok.
Part of sendLogEntryMessage client operation.
payload = LogEntryMessage to send.
reply = success flag (true = added, false = didn't add).
*/
func (l *LogEntryMessages) Add(payload LogEntryMessage, reply *bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if payload.Exists && payload.Entry.Exists {
		l.Messages = append(l.Messages, payload)
		*reply = true
	} else {
		*reply = false
	}

	return nil
}

/*
Gets the next LogEntryMessage in the struct - RPC ok.
Removes message from struct upon receipt.
reply = LogEntryMessage for leader to parse for the LogEntry to add to the log.
*/
func (l *LogEntryMessages) Listen(sourceID int, reply *LogEntryMessage) error {
	emptyLEM := LogEntryMessage{Exists: false, MorePresent: false}
	if sourceID < 1 || sourceID > node.NUM_NODES {
		*reply = emptyLEM
		return errors.New("LogEntryMessages.Listen(): Source ID out of range")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if msgsLen := len(l.Messages); msgsLen > 0 {
		lem := l.Messages[0]
		l.Messages = l.Messages[1:]
		if msgsLen > 1 {
			lem.MorePresent = true
		}
		*reply = lem
	} else {
		*reply = emptyLEM
	}

	return nil
}

/* Start the process of sending MR state in a log entry to the leader for them to add it to the log */
func StartAddStateToLog(server *rpc.Client, st logs.State) {
	msg := LogEntryMessage{
		Entry:  logs.NewLogEntry(true, st),
		Exists: true,
	}
	SendLogEntryMessage(server, msg)
}

/*
Send log entry message to leader - basically asks the leader to add the attached entry to the log
*/
func SendLogEntryMessage(server *rpc.Client, msg LogEntryMessage) {
	var added bool
	if err := server.Call("LogEntryMessages.Add", msg, &added); err != nil {
		fmt.Println("ERROR: LogEntryMessages.Add():", err)
	} else if added {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: LogEntryMessage added to struct\n")
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: LogEntryMessage NOT added to struct\n")
		} else {
			fmt.Printf("LogEntryMessage NOT added to struct\n")
		}
	}
}

/*
Check server for log entry messages - returns any message bc want to handle multiple nodes adding entry to log.
Followers and leader can add via this method.
Relies on leader being able to check for messages faster than nodes accumulate entries.
Relies on leader checking the MorePresent field of this message and calling this function again if true.
Part of leader role.
server = RPC server connection.
sourceID = self node ID - doesn't do anything really.
*/
func ReadLogEntryMessage(server *rpc.Client, sourceID int) LogEntryMessage {

	lem := LogEntryMessage{Exists: false}
	if err := server.Call("LogEntryMessages.Listen", sourceID, &lem); err != nil {
		fmt.Println("ERROR: LogEntryMessages.Listen():", err)
	}
	return lem
}

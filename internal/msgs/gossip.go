package msgs

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	T_FAIL         = 12 // in seconds, should be = 3 * Y_TIME
	T_CLEANUP      = 6  // in seconds, should be = T_FAIL
	DEBUG_MESSAGES = false
)

/* GossipMessage struct represents a new gossip message request to a client (request = member list) */
type GossipMessage struct {
	Init     bool               // initialize Requests.Pending entry with this table (true = yes, false = no)
	TargetID int                // request target node ID = ID of the request's target node
	Table    memlist.MemberList // source node member list
}

/* Requests struct represents pending message requests, only will be on server, holds most up to date version of member list */
type Requests struct {
	mu      *sync.RWMutex              // enables thread-safe reads and writes to Requests.Pending
	Pending map[int]memlist.MemberList // map [target node ID] (target node member list to combine with)
}

/* Returns a new instance of a Requests (returns as a pointer) */
func NewRequests() *Requests {
	return &Requests{
		mu:      new(sync.RWMutex),
		Pending: make(map[int]memlist.MemberList),
	}
}

/*
Adds a new message request to the pending list - method of Requests - RPC ok.
Part of the sendMessage operation of client.
Combines target node's table existing in Requests with table part of message being sent.
payload.ID = target node ID (updates this node's member list).
payload.Table = target node table (updates this table).
reply = update flag of combining tables (true if updated entry in Requests, false otherwise = does nothing)
*/
func (req *Requests) Add(payload GossipMessage, reply *bool) error {

	/* Check ID is within range */
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = false
		return errors.New("Requests.Add(): ID out of range")
	}

	/* Initialize entry if appropriate (ensures only self initializes its entry) */
	req.mu.Lock()
	defer req.mu.Unlock()
	if payload.Init {
		req.Pending[payload.TargetID] = payload.Table
		*reply = true
	} else {
		/* Check if Requests entry initialized */
		table, exists := req.Pending[payload.TargetID]
		if exists {
			/* Combine the target node's table with payload table, and mark dead nodes */
			req.Pending[payload.TargetID] = *combineTables(&table, &payload.Table)
			t := req.Pending[payload.TargetID].Members[payload.TargetID].Time // reference time t = local time at target node
			for id, n := range req.Pending[payload.TargetID].Members {        // loop through all nodes in target node's member list
				if (t - n.Time) >= (T_FAIL + T_CLEANUP) {
					delete(req.Pending[payload.TargetID].Members, id) // delete current node if not updated in T_FAIL + T_CLEANUP
				} else if (t - n.Time) >= T_FAIL {
					n.Alive = false                               // mark current node dead if not updated in T_FAIL
					req.Pending[payload.TargetID].Members[id] = n // save node info to target node's member list
				}
			}
			*reply = true
		} else {
			/* Case where another node sending a message to a Requests entry that isn't initialized yet */
			*reply = false
		}
	}

	/* Indicate success */
	return nil
}

/*
Receives communication from neighboring nodes - method of Requests - RPC ok.
Part of the readMessages operation of client.
ID = source node ID.
reply = most updated version of source node member list.
*/
func (req *Requests) Listen(ID int, reply *memlist.MemberList) error {

	/* Check ID is within range */
	if ID < 1 || ID > node.NUM_NODES {
		return errors.New("Requests.Listen(): ID out of range")
	}

	/* Retrieve source node's member list */
	req.mu.RLock()
	defer req.mu.RUnlock()
	if table, exists := req.Pending[ID]; exists {
		*reply = table
	} else {
		return errors.New("Requests.Listen(): member list doesn't exist for ID")
	}

	/* Indicate success */
	return nil
}

/*
Combines the info of two member lists and returns the updated member list - not exported.
Part of the Requests.Add() method that is part of the sendMessage operation.
table1 = copy of source node member list.
table2 = copy of table received via message.
*/
func combineTables(table1 *memlist.MemberList, table2 *memlist.MemberList) *memlist.MemberList {

	/* Loop through all possible node IDs */
	for i := range node.NUM_NODES {
		/* Get each table info for this ID */
		id := i + 1
		node1, exists1 := table1.Members[id]
		node2, exists2 := table2.Members[id]

		if exists1 && exists2 {
			/* Case 1: both tables have a node with this ID = decide whether to update table 1 node info */
			if node2.Alive && (node2.Hbcounter > node1.Hbcounter) {
				/* Case 1a: node 2 is alive and has a higher heartbeat value than node 1 = update node 1 */
				node2.Time = node.CalcTime()
				table1.Members[id] = node2
			}
			/* Case 1b: node 2 dead = do nothing */

		} else if exists2 && node2.Alive {
			/* Case 2: only table 2 has a node with this ID = add to table 1 if node is alive */
			node2.Time = node.CalcTime()
			table1.Members[id] = node2
		}
		/* Case 3: only table 1 has a node with this ID = do nothing */
		/* Case 4: neither table has a node with this ID = do nothing */
	}

	/* Return the updated source node table */
	return table1
}

/*
Sends gossip message to target node
*/
func SendGossipMessage(server *rpc.Client, msg GossipMessage) {
	var tablesCombined bool
	if err := server.Call("Requests.Add", msg, &tablesCombined); err != nil {
		fmt.Println("ERROR: Requests.Add()", err)
	} else if tablesCombined {
		if DEBUG_MESSAGES {
			fmt.Printf("Success: memberlist sent to ID: %d\n", msg.TargetID)
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Printf("Bypassed: node %d memberlist not initialized in Requests\n", msg.TargetID)
		}
	}
}

/*
Read the gossip message from the server.
Part of client updateSelfInfo operation.
In actuality, gets the most updated version of this node's member list from the server.
server - RPC connection to server.
sourceID - self node id.
memberlist - current member list to return on error
*/
func ReadGossipMessage(server *rpc.Client, sourceID int, memberlist memlist.MemberList) memlist.MemberList {

	/* Retrieve most up to date version of self member list */
	table := memlist.NewMemberList()
	if err := server.Call("Requests.Listen", sourceID, table); err != nil {
		fmt.Println("ERROR: Requests.Listen():", err)
		return memberlist // if error, then return initial memberlist
	}
	/* Return member list updated with other nodes' messages */
	return *table
}

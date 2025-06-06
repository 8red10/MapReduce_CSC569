package msgs

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/node"
)

type VoteMessage struct {
	TargetID int // id of node to vote for
	Term     int // local term of node = term of client sending the vote
}

type VoteCounter struct {
	mu        *sync.RWMutex // enables multiple threads to write concurrently
	Term      int           // current term number
	VoteCount map[int]int   // map[targetID]vote_count - holds the vote count for each node at the current term
}

/* Returns a pointer to a new VoteCounter */
func NewVoteCounter() *VoteCounter {
	vc := &VoteCounter{
		mu:        new(sync.RWMutex),
		Term:      0,
		VoteCount: make(map[int]int),
	}
	for i := range node.NUM_NODES {
		vc.VoteCount[i+1] = 0
	}
	return vc
}

/*
Add to specified vote count if appropriate - RPC ok.
Part of the sendVoteMessage client operation.
Only increments votes for the latest term number.
Resets vote counts for new term numbers.
payload = VoteMessage from voting client to requesting candidate.
reply = success flag (true = added, false = didn't add).
*/
func (vc *VoteCounter) Add(payload VoteMessage, reply *bool) error {

	/* Check ID within range */
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = false
		return errors.New("VoteCounter.Add(): Target ID out of range")
	}

	/* Add to vote count */
	vc.mu.Lock()
	defer vc.mu.Unlock()
	if vc.Term < payload.Term {
		/* Case 1: server term < client term, reset votes and add client vote and update server term */
		for i := range node.NUM_NODES {
			vc.VoteCount[i+1] = 0
		}
		vc.VoteCount[payload.TargetID] = 1
		vc.Term = payload.Term
		*reply = true
	} else if vc.Term == payload.Term {
		/* Case 2: server term == client term, add client vote */
		vc.VoteCount[payload.TargetID] += 1
		*reply = true
	} else {
		/* Case 3: server term > client term, don't add vote */
		*reply = false
	}

	/* Indicate success */
	return nil
}

/*
Get the vote count directed at self node - RPC ok.
Part of the countVotes client operation.
Relies on clients sending votes to update server term number and clear previous term election votes.
Only returns count if client term matches with server term.
payload = client ID and term.
reply = vote count for specified ID.
*/
func (vc *VoteCounter) Listen(payload VoteMessage, reply *int) error {

	/* Check ID within range */
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = 0
		return errors.New("VoteCounter.Listen(): source ID out of range")
	}

	/* Check for vote count at ID */
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	if vc.Term == payload.Term {
		*reply = vc.VoteCount[payload.TargetID]
	} else {
		*reply = 0
	}

	/* Indicate success */
	return nil
}

/*
Send vote message to target node
*/
func SendVoteMessage(server *rpc.Client, msg VoteMessage) {
	var voteAdded bool
	if err := server.Call("VoteCounter.Add", msg, &voteAdded); err != nil {
		fmt.Println("ERROR: VoteCounter.Add():", err)
	} else if voteAdded {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: VoteMessage sent to node %d during term %d\n", msg.TargetID, msg.Term)
			// } else {
			fmt.Printf("VoteMessage sent to node %d during term %d\n", msg.TargetID, msg.Term)
		}
	} else {
		if DEBUG_MESSAGES {
			fmt.Printf("OK: VoteMessage NOT sent to node %d\n", msg.TargetID)
		} else {
			fmt.Printf("VoteMessage NOT sent to node %d during term %d\n", msg.TargetID, msg.Term)
		}
	}
}

/*
Count votes on server addressed to self node and return count.
Part of candidate role.
server - RPC connection to server.
sourceID - self node ID.
sourceTerm - self node term.
*/
func CountVotes(server *rpc.Client, sourceID int, sourceTerm int) int {

	vm := VoteMessage{TargetID: sourceID, Term: sourceTerm}
	count := 0
	if err := server.Call("VoteCounter.Listen", vm, &count); err != nil {
		fmt.Println("ERROR: VoteCounter.Listen():", err)
	}
	return count
}

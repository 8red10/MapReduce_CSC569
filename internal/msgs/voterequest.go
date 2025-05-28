package msgs

import (
	"errors"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/node"
)

type VoteRequest struct {
	TargetID int  // id of node to send request to
	Term     int  // local term of candidate = term requesting the vote
	SourceID int  // id of candidate sending request
	Exists   bool // when reading: true if a vote request exists in server struct, false otherwise
}

type VoteRequests struct {
	mu      *sync.RWMutex       // enables multiple threads to write concurrently
	Term    int                 // latest vote request term number
	Mailbox map[int]VoteRequest // map[targetID]VoteRequest - holds the first VoteRequest of the newest term
}

/* Returns a pointer to a new VoteRequests */
func NewVoteRequests() *VoteRequests {
	return &VoteRequests{
		mu:      new(sync.RWMutex),
		Term:    0,
		Mailbox: make(map[int]VoteRequest),
	}
}

/*
Adds a new VoteRequest to the VoteRequests struct - RPC ok.
Part of the sendVoteRequest operation of client.
Doesn't remove existing VoteRequest entries with lesser term number.
Relies on client to compare term numbers upon receipt and only respond to a vote request w term > client term.
payload = VoteRequest from requesting candidate.
reply = success flag (true = added, false = didn't add).
*/
func (vr *VoteRequests) Add(payload VoteRequest, reply *bool) error {

	/* Check IDs are within range */
	if payload.TargetID < 1 || payload.TargetID > node.NUM_NODES {
		*reply = false
		return errors.New("VoteRequests.Add(): Target ID out of range")
	}
	if payload.SourceID < 1 || payload.SourceID > node.NUM_NODES {
		*reply = false
		return errors.New("VoteRequests.Add(): Source ID out of range")
	}

	/* Add payload to struct */
	vr.mu.Lock()
	defer vr.mu.Unlock()
	if vr.Term < payload.Term {
		/* Case 1: server term < candidate term, add request and update server term */
		vr.Mailbox[payload.TargetID] = payload
		vr.Term = payload.Term
		*reply = true
	} else if vr.Term == payload.Term {
		/* Case 2: server term == candidate term, check for existing request */
		if req, exists := vr.Mailbox[payload.TargetID]; !exists || req.Term < payload.Term {
			/* Case 2a: request doesn't exist on server or existing request has lower term number than payload */
			vr.Mailbox[payload.TargetID] = payload
			*reply = true
		} else {
			/* Case 2b: request on server exists and has term greater than or equal to payload request */
			*reply = false
		}
	} else {
		/* Case 3: server term > candidate term, late request = don't add */
		*reply = false
	}

	/* Indicate success */
	return nil
}

/*
Check for a VoteRequest directed at self node - RPC ok.
Part of the readVoteRequest operation of client.
Doesn't remove existing VoteRequest entries, just uses the idea that the first VoteRequest will update self term number.
Relies on client to use self term number to disregard request or update self term number and send vote.
sourceID - node ID of client checking for a vote request.
reply - vote request struct on server at sourceID.
*/
func (vr *VoteRequests) Listen(sourceID int, reply *VoteRequest) error {

	/* Check ID is within range */
	if sourceID < 1 || sourceID > node.NUM_NODES {
		*reply = VoteRequest{Exists: false}
		return errors.New("VoteRequests.Listen(): ID out of range")
	}

	/* Check for VoteRequest at ID */
	vr.mu.RLock()
	defer vr.mu.RUnlock()
	if req, exists := vr.Mailbox[sourceID]; exists {
		*reply = req
	} else {
		*reply = VoteRequest{Exists: false}
	}

	/* Inidicate success */
	return nil
}

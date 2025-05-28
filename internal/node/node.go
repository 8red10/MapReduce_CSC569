package node

import (
	"math/rand"
	"sync"
	"time"
)

const (
	NUM_NODES = 8
)

/* Node struct represents a computing node */
type Node struct {
	ID        int           // ID number
	Hbcounter int           // heartbeat counter
	Time      int64         // local time at last heartbeat counter update
	Alive     bool          // marks node as alive or in grace period (w/in T_cleanup)
	mu        *sync.RWMutex // enables thread-safe operations for Term and Role
	Term      int           // local term number
	Role      int           // role of node: 0 = follower, 1 = candidate, 2 = leader
}

/* Returns a new instance of a Node (returns as a pointer) */
func NewNode(id int) Node {
	return Node{
		mu:        new(sync.RWMutex),
		ID:        id,
		Hbcounter: 0,
		Time:      CalcTime(),
		Alive:     true,
		Term:      0,
		Role:      0, //FOLLOWER,
	}
}

/* Calculate time since the reference time */
func CalcTime() int64 {
	/* Parse reference time from string */
	layout := "2006-01-02 15:04:05"
	ref, _ := time.Parse(layout, "2025-04-22 12:00:00")
	now := time.Now()
	return now.Unix() - ref.Unix()
}

/* Generate random crash time */
func (n Node) CrashTime(min int, max int) int {
	return rand.Intn(max-min) + min
}

/* Generates two random neighbor IDs for the node associated with the input ID */
func (n Node) InitializeNeighbors(id int) [2]int {
	neighbor1 := RandInt()
	for neighbor1 == id {
		neighbor1 = RandInt()
	}
	neighbor2 := RandInt()
	for neighbor1 == neighbor2 || neighbor2 == id {
		neighbor2 = RandInt()
	}
	return [2]int{neighbor1, neighbor2}
}

/* Generate a random integer in the range [1, NUM_NODES] */
func RandInt() int {
	return rand.Intn(NUM_NODES-1+1) + 1
}

/* Get node term */
func (n *Node) GetTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.Term
}

/* Update node term */
func (n *Node) UpdateTermTo(term int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Term = term

	return nil
}

/* Get node role */
func (n *Node) GetRole() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.Role
}

/* Update node role */
func (n *Node) UpdateRoleTo(role int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Role = role

	return nil
}

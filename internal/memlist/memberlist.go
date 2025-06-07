package memlist

import (
	"errors"
	"fmt"
	"sync"

	"maps"

	"github.com/8red10/MapReduce_CSC569/internal/node"
)

/* Package level variable */
var Selflist *MemberList

/* MemberList struct represents participanting nodes - holds Node information */
type MemberList struct {
	mu      *sync.RWMutex     // enables thread-safe operations
	Members map[int]node.Node // holds node info
}

/* Returns a new instance of a MemberList (returns as a pointer) */
func NewMemberList() *MemberList {
	return &MemberList{
		mu:      new(sync.RWMutex),
		Members: make(map[int]node.Node),
	}
}

/* Adds (replaces) a node to the member list - method of MemberList - RPC ok */
func (m *MemberList) Add(payload node.Node, reply *node.Node) error {

	/* Check the input node's ID is within proper range */
	if payload.ID < 1 || payload.ID > node.NUM_NODES {
		return errors.New("MemberList.Add(): node ID out of range")
	}

	/* Add node to member list, replace existing node at ID if applicable */
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Members[payload.ID] = payload
	*reply = payload

	/* Indicate success */
	return nil
}

/* Updates a node in the member list - method of MemberList - RPC ok */
func (m *MemberList) Update(payload node.Node, reply *node.Node) error {

	/* Check ID is within range */
	if payload.ID < 1 || payload.ID > node.NUM_NODES {
		return errors.New("MemberList.Update(): node ID out of range")
	}

	/* Update node info at ID, adds node to ID entry if no node exists */
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Members[payload.ID] = payload
	*reply = payload

	/* Indicate success */
	return nil
}

/* Returns a node with specified ID - method of MemberList - RPC ok */
func (m *MemberList) Get(payload int, reply *node.Node) error {

	/* Check ID is within range */
	if payload < 1 || payload > node.NUM_NODES {
		return errors.New("MemberList.Get(): node ID out of range")
	}

	/* Return node with ID if exists in member list */
	m.mu.RLock()
	defer m.mu.RUnlock()
	if node, exists := m.Members[payload]; exists {
		*reply = node
	} else {
		return errors.New("MemberList.Get(): node with specified ID doesn't exist in member list")
	}

	/* Indicate success */
	return nil
}

/* Returns a safe copy of the current state of the table */
func (m *MemberList) SafeCopy() MemberList {
	m.mu.RLock()
	defer m.mu.RUnlock()

	newMembers := make(map[int]node.Node)
	maps.Copy(newMembers, m.Members)

	return MemberList{
		mu:      new(sync.RWMutex),
		Members: newMembers,
	}
}

/* Print this node's current member list */
func PrintMemberList(m MemberList, self_node *node.Node) {
	fmt.Println("")
	fmt.Printf("Member List for Node %d at local time %d\n", self_node.ID, self_node.Time)
	fmt.Println("------------------------------------------")
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %d and %s\n", val.ID, val.Hbcounter, val.Time, status)
	}
	fmt.Println("")
}

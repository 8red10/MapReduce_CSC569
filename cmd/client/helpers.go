package main

import (
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
	"github.com/8red10/MapReduce_CSC569/internal/timers"
)

const (
	DEBUG_MESSAGES = false
)

/* Global level variables */
var selfnode node.Node
var selflist *memlist.MemberList
var wg *sync.WaitGroup

func calcExitTime() int {
	return 200000 // 200 seconds //timers.GetCrashTime() // TODO
}

func getRPCServer() *rpc.Client {
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")
	return server
}

func parseArgs(exit_time int) int {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("No args given.")
		return -1
	} else if len(args) > 1 {
		fmt.Println("Bad args given. Only one positive int allowed.")
		return -1
	}
	id, err := strconv.Atoi(args[0])
	if err != nil || id < 1 || id > node.NUM_NODES {
		fmt.Println("Found ERROR:", err)
		fmt.Printf("Bad args given. Must be an integer in the range [1, %d]\n", node.NUM_NODES)
		return -1
	}
	fmt.Println("Node", id, "will fail after", exit_time, "milliseconds")
	return id
}

func createSelfNode(server *rpc.Client, id int) error {
	selfnode = node.NewNode(id)
	var self_node_response node.Node // allocate space for a response to overwrite this (var for RPC reply to be assigned)
	err := server.Call("MemberList.Add", selfnode, &self_node_response)
	if err != nil {
		fmt.Println("ERROR: MemberList.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id = %d\n", id)
	}
	return err
}

func createSelfTable(server *rpc.Client, id int) error {
	selflist = memlist.NewMemberList()
	var self_node_response node.Node
	selflist.Add(selfnode, &self_node_response)
	req := msgs.GossipMessage{Init: true, TargetID: id, Table: *selflist}
	updatedEntry := true
	err := server.Call("Requests.Add", req, &updatedEntry)
	if err != nil {
		fmt.Println("ERROR: Requests.Add()", err)
		return err
	} else if updatedEntry {
		if DEBUG_MESSAGES {
			fmt.Printf("Success: Node %d member list added to server Requests\n", id)
		}
	} else if !updatedEntry {
		fmt.Printf("Fail: Node %d member list not added to server Requests\n", id)
		return errors.New("createSelfTable(): table not added")
	}
	fmt.Println("Initialization complete.")
	fmt.Print("------------------------------------------\n\n")
	return nil
}

func createWG() {
	wg = &sync.WaitGroup{}
}

func startTimers(server *rpc.Client, id int, exit_time int) {
	/* Gossip */
	timers.StartUpdateSelfTimer(server, &selfnode, selflist, id)
	timers.StartGossipSelfTimer(server, selflist, &selfnode)
	timers.StartFailSelfTimer(server, &selfnode, wg, exit_time)
	/* Election */
	timers.StartElectionTimer(server, &selfnode)
	timers.StartCheckAppendEntriesTimer(server, &selfnode)
	timers.StartVoteRequestTimer(server, &selfnode)
	/* Log */

	// to be implemented and added

}

func runUntilFailure(id int) {
	wg.Add(1)
	wg.Wait()
	fmt.Printf("Exiting node %d\n", id)
}

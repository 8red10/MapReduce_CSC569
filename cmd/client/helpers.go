package main

import (
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
	"github.com/8red10/MapReduce_CSC569/internal/timers"
)

const (
	DEBUG_MESSAGES = false
)

/* Global level variables */
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
	node.Selfnode = node.NewNode(id)
	var self_node_response node.Node // allocate space for a response to overwrite this (var for RPC reply to be assigned)
	err := server.Call("MemberList.Add", *node.Selfnode, &self_node_response)
	if err != nil {
		fmt.Println("ERROR: MemberList.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id = %d\n", id)
	}
	return err
}

func createSelfTable(server *rpc.Client, id int) error {
	memlist.Selflist = memlist.NewMemberList()
	var self_node_response node.Node
	memlist.Selflist.Add(*node.Selfnode, &self_node_response)
	req := msgs.GossipMessage{Init: true, TargetID: id, Table: *memlist.Selflist}
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
	return nil
}

func createSelfLog() {
	logs.Selflog = logs.NewLog()
	fmt.Println("Success: created self log.")
}

func initComplete() {
	fmt.Println("Initialization complete.")
	fmt.Print("------------------------------------------\n\n")
}

func createWG() {
	wg = &sync.WaitGroup{}
}

func startTimers(server *rpc.Client, id int, exit_time int) {
	/* Gossip */
	timers.StartUpdateSelfTimer(server, node.Selfnode, memlist.Selflist, id)
	timers.StartGossipSelfTimer(server, memlist.Selflist, node.Selfnode)
	timers.StartFailSelfTimer(server, node.Selfnode, wg, exit_time)
	/* Election */
	timers.StartElectionTimer(server, node.Selfnode)
	timers.StartCheckAppendEntriesTimer(server, node.Selfnode)
	timers.StartVoteRequestTimer(server, node.Selfnode)
	/* Log */
	timers.StartCheckEntireLogTimer(server, node.Selfnode)
	timers.CheckEntireLogTimer.Stop()
	timers.StartCountLogMatchesTimer(server, node.Selfnode)
	timers.CountLogMatchesTimer.Stop()
	timers.StartCheckLogEntryTimer(server, node.Selfnode)
	timers.CheckLogEntryTimer.Stop()

	/* TESTING = after 50 ms, add to self log - see what happens */
	lem := msgs.LogEntryMessage{
		Exists: true,
		Entry: logs.NewLogEntry(
			true,
			logs.NewMapReduceData(-1),
		),
	}
	time.AfterFunc(
		time.Millisecond*time.Duration(50), // after 50 ms
		func() { msgs.SendLogEntryMessage(server, lem) },
	)

	time.AfterFunc(
		time.Millisecond*time.Duration(5000), // after 5s
		func() { sendLEM(server, lem) },
	)
}

func sendLEM(server *rpc.Client, msg msgs.LogEntryMessage) {
	lem := msgs.LogEntryMessage{
		Exists: true,
		Entry: logs.NewLogEntry(
			true,
			logs.NewMapReduceData(-1),
		),
	}
	msgs.SendLogEntryMessage(server, lem)
	time.AfterFunc(
		time.Millisecond*time.Duration(5000), // after 5s
		func() { sendLEM(server, msg) },
	)
}

func runUntilFailure(id int) {
	wg.Add(1)
	wg.Wait()
	fmt.Printf("Exiting node %d\n", id)
}

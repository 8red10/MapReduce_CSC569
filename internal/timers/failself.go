package timers

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	EXIT_TIME_MAX = 200000 // maximum time before node failure, in milliseconds
	EXIT_TIME_MIN = 10000  // minimum time before node failure, in milliseconds
)

/* Return random crash time for node */
func GetCrashTime() int {
	return crashTime(EXIT_TIME_MIN, EXIT_TIME_MAX)
}

/* Generate random crash time */
func crashTime(min int, max int) int {
	return rand.Intn(max-min) + min
}

/* Cause self to fail after EXIT_TIME */
func StartFailSelfTimer(server *rpc.Client, self_node *node.Node, wg *sync.WaitGroup, exit_time int) {
	time.AfterFunc(
		time.Millisecond*time.Duration(exit_time),
		func() { failSelfNode(server, self_node, wg) },
	)
}

func failSelfNode(server *rpc.Client, self_node *node.Node, wg *sync.WaitGroup) {
	self_node.Alive = false
	var responseNode node.Node
	if err := server.Call("MemberList.Update", self_node, &responseNode); err != nil {
		fmt.Println("ERROR: MemberList.Update()", err)
	} else {
		fmt.Printf("Successfully killed node %d in server member list\n", self_node.ID)
	}
	server.Close()
	wg.Done()
}

package timers

import (
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	GOSSIP_TIME = 2000 // delay between sending member list to neighbors, in milliseconds
)

/* Send self member list to neighbors every GOSSIP_TIME */
func StartGossipSelfTimer(server *rpc.Client, memberlist *memlist.MemberList, self_node *node.Node) {
	time.AfterFunc(
		time.Millisecond*GOSSIP_TIME,
		func() { gossipSelfTable(server, memberlist, self_node) },
	)
}

func gossipSelfTable(server *rpc.Client, memberlist *memlist.MemberList, self_node *node.Node) {
	/* Set timer to send our member list again */
	time.AfterFunc(time.Millisecond*GOSSIP_TIME, func() { gossipSelfTable(server, memberlist, self_node) })

	/* Send self member list to two neighbors */
	newNeighbors := self_node.InitializeNeighbors(self_node.ID)
	for i := range newNeighbors {
		msg := msgs.GossipMessage{Init: false, TargetID: newNeighbors[i], Table: *memberlist}
		go msgs.SendGossipMessage(server, msg)
	}
}

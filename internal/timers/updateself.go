package timers

import (
	"net/rpc"
	"time"

	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	UPDATE_SELF_TIME = 1000 // delay between heartbeat counter increment, in milliseconds
)

/* Update self info (node and member list) every UPDATE_SELF_TIME */
func StartUpdateSelfTimer(server *rpc.Client, self_node *node.Node, memberlist *memlist.MemberList, id int) {
	time.AfterFunc(
		time.Millisecond*UPDATE_SELF_TIME,
		func() { updateSelfInfo(server, self_node, memberlist, id) },
	)
}

func updateSelfInfo(server *rpc.Client, self_node *node.Node, memberlist *memlist.MemberList, id int) {

	/* Set timer to update this node's info and member list again */
	time.AfterFunc(time.Millisecond*UPDATE_SELF_TIME, func() { updateSelfInfo(server, self_node, memberlist, id) })

	/* Update self member list with other nodes' member lists */
	*memberlist = msgs.ReadGossipMessage(server, id, *memberlist)

	/* Increment this node's heartbeat counter and local time */
	var responseNode node.Node
	self_node.Hbcounter += 1
	self_node.Time = node.CalcTime()
	memberlist.Update(*self_node, &responseNode)

	/* Update self memberlist Requests entry with self memberlist */
	msg := msgs.GossipMessage{Init: false, TargetID: id, Table: *memberlist} // copying table works if mutex value not copied
	msgs.SendGossipMessage(server, msg)

	// /* Print this node's member list */
	// memlist.PrintMemberList(*memberlist, self_node)
}

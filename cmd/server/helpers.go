package main

import (
	"io"
	"net/http"
	"net/rpc"

	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
)

/* Registers server structs with `rpc.DefaultServer` for RPC */
func registerRPCStructs() {
	/* Gossip structs */
	rpc.Register(memlist.NewMemberList())
	rpc.Register(msgs.NewRequests())
	/* Election structs */
	rpc.Register(msgs.NewAppendEntryMessages())
	rpc.Register(msgs.NewVoteRequests())
	rpc.Register(msgs.NewVoteCounter())
	/* MapReduce structs */
	rpc.Register(msgs.NewLogMatchCounter())
	rpc.Register(msgs.NewLogErrorMessages())
	rpc.Register(msgs.NewEntireLogMessages())
}

/* Handle RPC requests */
func handleRPC() {
	/* Registers an HTTP handler for RPC communication */
	rpc.HandleHTTP()
	/* Sample test endpoint */
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		io.WriteString(res, "RPC SERVER LIVE!")
	})
	/* Listen and serve default HTTP server */
	http.ListenAndServe("localhost:9005", nil)
}

package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/8red10/MapReduce_CSC569/internal/mr"
)

/* Usage: server -file=some/text/file.txt -port=9005 */
func parseFlags() (*string, *string) {
	filePath := flag.String("file", "", "path to the .txt file to count words in")
	port := flag.String("port", "9005", "TCP port to listen on (e.g. 9005)")
	flag.Parse()

	if *filePath == "" {
		// log.Println("You must specify -file=<path>")                     // no makefile
		// log.Println("Usage: server -file=<path/to/file.txt> -port=9005") // no makefile
		log.Println("You must specify FILE=<path>")                    // with makefile
		log.Println("Usage: server FILE=<path/to/file.txt> PORT=9005") // with makefile
		os.Exit(1)
	}
	return filePath, port
}

/* Create a MRServer over the file and register `MRServer` for RPC */
func createMRServer(filePath *string) {
	/* Create the MRServer for filepath */
	mrServer, err := mr.NewMRServer(*filePath)
	if err != nil {
		log.Fatalf("cannot create MRServer: %v", err) // equal to call to Printf then os.Exit(1)
	}
	/* Register MRServer for RPC (clients can call “MRServer.RequestMapTask” etc.) */
	err = rpc.Register(mrServer)
	if err != nil {
		log.Fatalf("rpc.Register error: %v", err)
	}
}

/* Create listener that serves connections forever */
func serveConnections(filePath *string, port *string) {
	/* Create listener */
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen error: %v", err)
	}
	log.Printf("MapReduce server listening on port %s; file=%s\n", *port, *filePath)

	/* Accept and serve connections forever */
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

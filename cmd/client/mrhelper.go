package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/mr"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
	"github.com/8red10/MapReduce_CSC569/internal/node"
)

const (
	VERBOSE = false
)

/* Usage: client -id:<int> -server=localhost:9005 */
func parseFlags() (int, string, bool) {
	id := flag.Int("id", -1, "node id number to split work with (within range [1,8]")
	serverAddr := flag.String("server", "localhost:9005", "TCP hostname:port to connect to (e.g. localhost:9005)")
	verboseFlag := flag.Bool("v", false, "verbose flag to enable log output")
	flag.Parse()

	fmt.Printf("verbose status %t\n", *verboseFlag)

	time.Sleep(time.Second)

	if *id < 1 || *id > node.NUM_NODES {
		log.Printf("ID %d out of range [1,%d]\n", *id, node.NUM_NODES)
		// log.Fatalf("Usage: client -id:<int> -server=localhost:9005") // no makefile
		log.Fatalf("Usage: client ID:<int> SERVER=localhost:9005") // with makefile
	}

	return *id, *serverAddr, *verboseFlag
}

/* Dial TCP connection */
func getServerConnection(serverAddr string) *rpc.Client {
	log.Printf("Dialing RPC server at %s ...\n", serverAddr)
	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("Dial error: %v", err)
	}
	return client
}

func performMR(server *rpc.Client, id int, verboseFlag bool) {
	performMap(server, id, verboseFlag)
	performReduce(server, id, verboseFlag)
	printOutput(server, id)
}

/* PHASE 1: loop, keep requesting map tasks and submitting results */
func performMap(server *rpc.Client, id int, verboseFlag bool) {
	/* Loop: keep calling RequestMapTask until it returns Status != "mapping" or no ChunkIdx. */
	for {
		/* Create a gossip message to pass id and self table */
		msg := msgs.GossipMessage{Init: false, TargetID: id, Table: *memlist.Selflist}
		/* Get the next map task */
		var st logs.State
		err := server.Call("MRServer.RequestMapTask", msg, &st)
		go msgs.StartAddStateToLog(server, st)
		if err != nil {
			log.Printf("RequestMapTask RPC error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue // go back to the start of the for loop
		}

		/* Log whatever the server sent us */
		if verboseFlag {
			fmt.Printf("[map] State=%+v\n", st)
		}

		/* If we got a real chunk to process */
		if st.Status == "mapping" && st.ChunkIdx >= 0 && st.FilePath != "" {
			/* Read that one line from the file */
			lineWords, err := readLineWords(st.FilePath, st.ChunkIdx)
			if err != nil {
				log.Fatalf("client cannot read chunk: %v", err)
			}

			/* Count each word in that line: */
			counter := map[string]int{}
			for _, w := range lineWords {
				/* normalize lower-case and strip punctuation if needed */
				token := trimPunct(strings.ToLower(w))
				counter[token]++
			}

			/* Turn line (chunk) into []KeyValue */
			var kvs []mr.KeyValue
			for word, cnt := range counter {
				kvs = append(kvs, mr.KeyValue{Key: word, Value: cnt})
			}

			/* Submit mapper result */
			args := mr.MapResult{
				ChunkIdx: st.ChunkIdx,
				KVs:      kvs,
			}
			var reply logs.State
			err = server.Call("MRServer.SubmitMapResult", args, &reply)
			go msgs.StartAddStateToLog(server, reply)
			if err != nil {
				log.Fatalf("SubmitMapResult RPC error: %v", err)
			}
			if verboseFlag {
				log.Printf("[map] after SubmitMapResult → State=%+v\n", reply)
			}

			/* Continue mapping loop */
			continue // go back to the start of the for loop
		}

		/* If server says st.Status != "mapping," then either “reducing” or “done” */
		break // end this loop = start reduce
	}
}

/* PHASE 2: loop, keep requesting reduce tasks and submitting results */
func performReduce(server *rpc.Client, id int, verboseFlag bool) {
	/* Loop: keep calling RequestReduceTask until it returns Status != "reducing" or key == "". */
	for {
		/* Create a gossip message to pass id and self table */
		msg := msgs.GossipMessage{Init: false, TargetID: id, Table: *memlist.Selflist}
		/* Get the next reduce task */
		var st logs.State
		err := server.Call("MRServer.RequestReduceTask", msg, &st)
		go msgs.StartAddStateToLog(server, st)
		if err != nil {
			log.Printf("RequestReduceTask RPC error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue // go back to the start of this for loop
		}

		/* If we are still waiting on map tasks to finish */
		if st.Status == "mapping" {
			/* Go back to map and check a map task leftover from a failed node */
			performMap(server, id, verboseFlag)
			continue // go back to the start of this for loop after checking
		}

		if verboseFlag {
			log.Printf("[reduce] State=%+v\n", st)
		}

		/* If we got a real key to reduce */
		if st.Status == "reducing" && st.Key != "" && len(st.Values) > 0 {
			/* Sum all the counts for that word */
			sum := 0
			for _, v := range st.Values {
				sum += v
			}

			/* Submit reduce result */
			args := mr.ReduceResult{
				Key:   st.Key,
				Value: sum,
			}
			var reply logs.State
			err = server.Call("MRServer.SubmitReduceResult", args, &reply)
			go msgs.StartAddStateToLog(server, reply)
			if err != nil {
				log.Fatalf("SubmitReduceResult RPC error: %v", err)
			}
			if verboseFlag {
				log.Printf("[reduce] after SubmitReduceResult → State=%+v\n", reply)
			}

			/* Continue reducing loop */
			continue // go back to the start of the for loop
		}

		/* If server says st.Status == "done" (or no Key), we’re completely finished */
		if st.Status == "done" {
			log.Println("All done! Exiting.")
			break // exit the for loop = done with mapreduce
		}

		// Otherwise (e.g. mapping still in progress), wait a bit and retry: // TODO - what if we go back to map task here ?
		time.Sleep(200 * time.Millisecond)
		// TODO - go back to the mapper function here ? would this screw things up ?
		// yes call mapper func if there is no possibility that the mapper tasks would get reset
	}

}

/* trims punctuation at the ends of a string */
func trimPunct(s string) string {
	return strings.TrimFunc(s, unicode.IsPunct)
}

/*
readLineWords reads exactly one line (by index) from the file at filePath,
splits it into words by spaces (strings.Fields), and returns the slice of words.
*/
func readLineWords(filePath string, targetIdx int) ([]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	idx := 0
	for scanner.Scan() {
		if idx == targetIdx {
			return strings.Fields(scanner.Text()), nil
		}
		idx++
	}
	return nil, scanner.Err()
}

/* Get the final output from server and print it */
func printOutput(server *rpc.Client, id int) {
	var finalCounts map[string]int
	err := server.Call("MRServer.GetFinalOutput", id, &finalCounts)
	if err != nil {
		log.Fatalf("GetFinalOutput RPC error: %v", err)
	}
	for word, count := range finalCounts {
		fmt.Printf("%s: %d\n", word, count)
	}
}

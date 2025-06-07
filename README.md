# MapReduce with Leader Election in Go


## Overview

A simple distributed MapReduce that counts word frequencies in a text file, using Go’s `net/rpc` and Raft for fault-tolerant state replication.

We also utilize gossip heartbeat detection, RAFT-based leader election, and log consistency and replication for increased robustness to node failure. This project is intended to be deployed using multiple terminal windows on a single computer to simulate a real-world distributed system. To stay consistent with popular modern distributed systems, we use GoLang. 

A short video demonstration of this implementation can be seen [here](https://youtu.be/EyOqX1WBP5M).


## User Instructions

To replicate the setup seen in the demo video, follow the steps below: 

1. Configure a Go programming environemnt. 
2. Clone this repository.
```
git clone https://github.com/8red10/MapReduce_CSC569.git
cd MapReduce_CSC569
```
3. Utilize the Makefile recipes to build the executables. The server executable will be created in the `./cmd/server/` directory. The client executable will be created in the `./cmd/client/` directory.
```
# Builds both the server and client binaries
make build
```
4. Open 9 terminal windows: 1 for the server and 8 for clients. The next steps utilize the Makefile, so it is not necessary to `cd` to the location of the created binaries.
5. First start the server. Adjust the flags as you desire. Just ***make sure the port numbers match*** between the server and client or else they will not be able to communicate.
```
# Generic recipe
make server
# Is the same as below and should work straight out of the box
./cmd/server/server -file=files/pg-being_ernest.txt -port=9005
```
6. Next start the clients. This program runs quick, so it can be hard to start many of them and see leader election or log replication in action. In separate terminal windows, execute `make client1`, `make client2`, ... up to `make client8`. 
```
# Generic recipe for client 1
make client1
# Is the same as below and should work straight out of the box
./cmd/client/client -id=1 -server=localhost:9005 -v=false
```

## Components

- **`mr` package** (`/internal/mr/mr.go`):  
  - Defines a single RPC server (`MRServer`) that holds one job and its progress state.  
  - Splits the input file into **map** chunks (one line per chunk).  
  - Once all map chunks finish, automatically publishes **reduce** tasks (one per unique word).  
  - Exposes only these RPC methods—each returns exactly one `State` struct:
    ```go
    RequestMapTask     // hand out next map-chunk  
    SubmitMapResult    // collect one chunk’s word counts  
    RequestReduceTask  // hand out next reduce-key + its counts  
    SubmitReduceResult // collect one word’s total count  
    GetFinalOutput     // fetches full word→count map once done  
    ```
  - **Raft integration**: every time the server computes a new `State` (e.g. after assigning a task or merging results), it **sends that `State` to the Raft leader**, which appends it to the Raft log. Once a majority of nodes replicate the entry, the state machine applies it—ensuring all replicas stay in sync and can recover if the leader crashes.

- **Server binary** (`/cmd/server/main.go`):  
  1. Reads `-file=<path>` and `-port=<n>` flags.  
  2. Constructs `NewMRServer(file)`.  
  3. Wraps it in a Raft cluster (peers configured via flag or config file).  
  4. Registers the leader’s `MRServer` for RPC under service name `MRServer`.  
  5. Listens on TCP and calls `rpc.ServeConn` for each client.  

- **Client binary** (`/cmd/client/main.go`):  
  - Dials the RPC leader (or any Raft node).  
  - **Mapping phase** (loop until status ≠ `"mapping"`):  
    ```text
    Call RequestMapTask → State  
    log State  
    if State.ChunkIdx ≥ 0 {
      read that line, count words, call SubmitMapResult
      → new State; log it
    }
    ```
  - **Reducing phase** (loop until status == `"done"`):  
    ```text
    Call RequestReduceTask → State  
    log State  
    if State.Key != "" {
      sum State.Values, call SubmitReduceResult
      → new State; log it
    }
    ```
  - Logs **every** returned `State` so you can trace progress end-to-end.

- **Makefile**  
  - `make build` / `make build_server` / `make build_client`  
  - `make server FILE=... PORT=... `  
  - `make client1 SERVER=host:port VERBOSE=...`  


## End-to-End Flow

1. **Server startup & Raft init**  
   - The process calls `NewMRServer(file)` and boots a Raft node.  
   - On becoming leader, it registers its `MRServer` RPC handlers and begins listening.

2. **Job begins**  
   - Raft leader reads the file into lines → one subtask per line.  
   - Every client RPC (`RequestMapTask`, `SubmitMapResult`, etc.) produces a new `State`, which the leader **proposes** to its Raft log.  
   - Once that log entry is **committed** (majority replicated), the leader applies it and **replies** to the client with that `State`.

3. **Map phase**  
   - Multiple clients call `RequestMapTask` in parallel.  
   - Leader assigns each line → new `State` with `FilePath` & `ChunkIdx`.  
   - Client counts words, calls `SubmitMapResult`, sends `KVs`.  
   - Leader merges them into `Intermediate[word]`, logs new `State`.

4. **Transition to reduce**  
   - When **all** map chunks are marked done (tracked via Raft-committed states), the leader generates one reduce-task per unique word.  
   - It logs that transition in the Raft log, flips its internal phase to `"reducing"`.

5. **Reduce phase**  
   - Clients call `RequestReduceTask`; leader assigns a word + slice of counts in `State`.  
   - Clients sum the counts, call `SubmitReduceResult`.  
   - Leader records each final `(word, total)` in `Output`, logging each state change.

6. **Completion**  
   - Once **all** reduce tasks are done, leader sets phase to `"done"` in Raft.  
   - Further RPC calls return `State{Status:"done"}`.  
   - Clients can then call `GetFinalOutput` (another Raft-logged RPC) to fetch the complete word→count map.

---

### Why Raft?

- **Fault tolerance**: if the leader crashes, a new leader is elected, and its state machine (replaying the committed log) has the exact same job progress—so no work is lost or re-computed unnecessarily.  
- **Strong consistency**: clients always see a linearized sequence of `State` transitions. Every assignment and result-merge is durably stored before acknowledging success.
- **Scalability**: you can run the server on multiple nodes (3, 5, etc.), and the RPC endpoint is always the current leader. Clients need only dial one address, or use any peer that forwards to the leader.


## Project Organization

The files in this repository are organized as illustrated below:
- `/cmd`: contains the executables and helper files.
- `/internal`: contains our APIs.
    - `/logs`: functions, structs, and RPC calls for the log.
    - `/memlist`: functions, structs, and RPC calls for the gossip heartbeat membership protocol.
    - `/mr`: functions, structs, and RPC calls for map and reduce tasks.
    - `/msgs`: functions, structs, and RPC calls for RPC message passing through the server to help facilitate communication and coordination.
    - `/node`: functions and structs for node info.
    - `/timers`: functions for timers that facilitate the sending and checking of messages through the RPC server.
- `/files`: contains text files to perform the word frequency count on.

``` bash
MapReduce_CSC569
├── LICENSE
├── Makefile
├── README.md
├── cmd
│   ├── client
│   │   ├── helpers.go
│   │   ├── main.go
│   │   └── mrhelper.go
│   └── server
│       ├── helpers.go
│       ├── main.go
│       └── mrhelper.go
├── files
│   ├── pg-being_ernest.txt
│   └── pg-metamorphosis.txt
├── go.mod
└── internal
    ├── logs
    │   ├── log.go
    │   └── logentry.go
    ├── memlist
    │   └── memberlist.go
    ├── mr
    │   └── mr.go
    ├── msgs
    │   ├── appendentry.go
    │   ├── entirelog.go
    │   ├── gossip.go
    │   ├── logentry.go
    │   ├── logerror.go
    │   ├── logmatchcounter.go
    │   ├── votecounter.go
    │   └── voterequest.go
    ├── node
    │   └── node.go
    └── timers
        ├── appendentry.go
        ├── countvotes.go
        ├── election.go
        ├── entirelog.go
        ├── failself.go
        ├── gossipself.go
        ├── logentry.go
        ├── logerror.go
        ├── logmatch.go
        ├── roles.go
        ├── updateself.go
        └── voterequest.go
```

## Authors

By Jack Krammer, Ashton Alonge, Logan Barker, and David Hernandez on June 6, 2025, for Advanced Distributed Systems (CSC 569) at California Polytechnic State University

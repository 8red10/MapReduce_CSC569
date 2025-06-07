package mr

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/8red10/MapReduce_CSC569/internal/logs"
	"github.com/8red10/MapReduce_CSC569/internal/memlist"
	"github.com/8red10/MapReduce_CSC569/internal/msgs"
)

// KeyValue is used to pass intermediate (word, count) pairs from a mapper
// back to the server.
type KeyValue struct {
	Key   string
	Value int
}

// MapResult is the argument to SubmitMapResult: it tells the server
// which chunk was processed and which KVs were produced.
type MapResult struct {
	ChunkIdx int
	KVs      []KeyValue
	Table    memlist.MemberList
}

// ReduceResult is the argument to SubmitReduceResult: it gives a single
// (word, totalCount) pair for the server’s final output.
type ReduceResult struct {
	Key   string
	Value int
	Table memlist.MemberList
}

/* MOVED TO LOGS PACKAGE FOR LOG ENTRY INCORPORATION */

// // State is returned (and only ever returned) from every RPC.
// // Clients log all of State’s fields to see “progress.”
// type State struct {
// 	// “mapping” → still handing out map‐chunks
// 	// “reducing” → still handing out reduce‐keys
// 	// “done”     → everything finished
// 	Status string

// 	// How many map‐chunks remain unassigned or in‐flight?
// 	MapTasksPending int
// 	// How many reduce‐keys remain unassigned or in‐flight?
// 	ReduceTasksPending int

// 	// If the server just handed you a *map* task, FilePath != "" and
// 	// ChunkIdx ≥ 0.  Otherwise ChunkIdx == –1 and FilePath == "".
// 	FilePath string
// 	ChunkIdx int

// 	// If the server just handed you a *reduce* task, Key != "" and
// 	// Values is the slice of counts to sum.  Otherwise Key == "".
// 	Key    string
// 	Values []int
// }

// -----------------------------------------------------------------------
// internal types and constants:

const (
	phaseMapping  = "mapping"
	phaseReducing = "reducing"
	phaseDone     = "done"
)

// subtask represents either a map‐chunk or a reduce‐key job.
type subtask struct {
	// for map‐chunks: chunkIdx ≥ 0, Key == "".
	// for reduce‐keys: chunkIdx == –1, Key != "".
	ChunkIdx int
	Key      string

	// if this is a reduce‐key, Values holds all intermediate counts
	// from every mapper for “Key.”
	Values []int

	// non-negative once some client has been handed this task
	ID int
	// true once some client has been handed this task
	Assigned bool
	// true once a client has returned results for this task
	Done bool
}

// mrJob holds all the data for a single file’s word‐count job.
type mrJob struct {
	FilePath string

	// intermediate[word] = []int of counts from every mapper
	Intermediate map[string][]int
	// final output[word] = totalCount
	Output map[string]int

	// all subtasks (map‐chunks first, then reduce‐keys later)
	Subtasks []*subtask

	// which phase we’re in now:
	Phase string
}

// MRServer is the RPC server.  It holds exactly one job in memory.
type MRServer struct {
	mu    sync.Mutex
	job   *mrJob
	lines [][]string
}

// NewMRServer constructs an MRServer that will count words in `filePath`.
// It reads the entire file, splits it into lines, and prepares one map‐chunk
// per line.  At start, Phase = “mapping,” and job.Subtasks holds one subtask
// per line (with ChunkIdx set to that line’s index).
func NewMRServer(filePath string) (*MRServer, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var lines [][]string
	for scanner.Scan() {
		text := scanner.Text()
		// split on spaces (simple): strings.Fields handles tabs too
		words := strings.Fields(text)
		lines = append(lines, words)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Build one subtask per line:
	var subtasks []*subtask
	for idx := range lines {
		subtasks = append(subtasks, &subtask{
			ChunkIdx: idx,
			Key:      "",
			Values:   nil,
			ID:       -1,
			Assigned: false,
			Done:     false,
		})
	}

	job := &mrJob{
		FilePath:     filePath,
		Intermediate: make(map[string][]int),
		Output:       make(map[string]int),
		Subtasks:     subtasks,
		Phase:        phaseMapping,
	}

	return &MRServer{
		job:   job,
		lines: lines,
	}, nil
}

/* check if node assigned subtask has since failed */
func checkAssignment(st *subtask, table memlist.MemberList) {
	if st.Assigned {
		// var n node.Node
		// table.Get(st.ID, &n)
		n := table.Members[st.ID]
		if !n.Alive {
			st.Assigned = false
			st.ID = -1
			fmt.Printf("unassigned subtask from node %d\n", n.ID)
		}
	}
}

// computeTaskCounts returns how many map‐chunks or reduce‐keys remain φpending (not yet Done).
func (mr *MRServer) computeTaskCounts(table memlist.MemberList) (mapPending, reducePending int) {
	for _, st := range mr.job.Subtasks {
		if !st.Done {
			checkAssignment(st, table)

			if mr.job.Phase == phaseMapping {
				// still in mapping: only count map‐chunks
				if st.Key == "" {
					mapPending++
				}
			} else if mr.job.Phase == phaseReducing {
				// only reduce‐keys remain (all map‐chunks Done)
				if st.Key != "" {
					reducePending++
				}
			}
		}
	}
	return
}

// PublishReduceTasks is called exactly once, once all map‐chunks are marked Done.
// It scans mr.job.Intermediate and creates one subtask for each distinct word.
// (No RPC method: called internally the moment “mapping” finishes.)
func (mr *MRServer) publishReduceTasks() {
	// collect all words:
	for word, counts := range mr.job.Intermediate {
		mr.job.Subtasks = append(mr.job.Subtasks, &subtask{
			ChunkIdx: -1,
			Key:      word,
			Values:   counts, // will be a slice of ints from every mapper
			ID:       -1,
			Assigned: false,
			Done:     false,
		})
	}
	mr.job.Phase = phaseReducing
}

// -----------------------------------------------------------------------
// RPC methods.  Each reply is exactly a *State, which clients simply log:

// RequestMapTask hands out one “map chunk” (one line index) at a time.
// If there is still an unassigned map‐chunk, we mark it Assigned, return in reply:
//
//	Status = “mapping”
//	MapTasksPending = how many map‐chunks (including in‐flight) remain
//	ReduceTasksPending = 0
//	FilePath = job.FilePath
//	ChunkIdx = the line‐index to process
//	Key = "" // no reduce key yet
//	Values = nil
//
// Once every map‐chunk is Done (server ↦ publishReduceTasks), further calls
// to RequestMapTask return State{Status=“reducing” or “done”, MapTasksPending=0, …} with no FilePath/ChunkIdx.
func (mr *MRServer) RequestMapTask(payload msgs.GossipMessage, reply *logs.State) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	// If we’ve moved on to “reducing,” just return the new status:
	if mr.job.Phase != phaseMapping {
		mapPending, redPending := mr.computeTaskCounts(payload.Table)
		reply.Status = mr.job.Phase
		reply.MapTasksPending = mapPending
		reply.ReduceTasksPending = redPending
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}

	// Still in “mapping.”  Find the first unassigned map‐chunk:
	for _, st := range mr.job.Subtasks {
		if st.Key == "" && !st.Assigned && !st.Done {
			st.Assigned = true
			st.ID = payload.TargetID
			mapPending, _ := mr.computeTaskCounts(payload.Table)
			reply.Status = phaseMapping
			reply.MapTasksPending = mapPending
			reply.ReduceTasksPending = 0
			reply.FilePath = mr.job.FilePath
			reply.ChunkIdx = st.ChunkIdx
			reply.Key = ""
			reply.Values = nil
			return nil
		}
	}

	// No unassigned map‐chunks remain → either in‐flight or Done.
	// Check if any are still in‐flight (Assigned but not Done). If so, just report counts:
	mapPending, _ := mr.computeTaskCounts(payload.Table)
	if mapPending > 0 {
		// still waiting for some mappers to finish
		reply.Status = phaseMapping
		reply.MapTasksPending = mapPending
		reply.ReduceTasksPending = 0
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}

	// All map‐chunks are Done.  Time to publish reduce tasks (once).
	mr.publishReduceTasks()
	_, reducePending := mr.computeTaskCounts(payload.Table)

	reply.Status = phaseReducing
	reply.MapTasksPending = 0
	reply.ReduceTasksPending = reducePending
	reply.FilePath = ""
	reply.ChunkIdx = -1
	reply.Key = ""
	reply.Values = nil
	return nil
}

// SubmitMapResult is called by a mapper who just processed line ChunkIdx.
// They send back every KeyValue (word→count within that one line).
// The server merges these into Intermediate[word] = append(...).
// In the reply State, we update counts and possibly flip to “reducing” if all map‐chunks are Done.
func (mr *MRServer) SubmitMapResult(args MapResult, reply *logs.State) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	// Merge each KeyValue into Intermediate:
	for _, kv := range args.KVs {
		mr.job.Intermediate[kv.Key] = append(mr.job.Intermediate[kv.Key], kv.Value)
	}

	// Mark that subtask Done:
	for _, st := range mr.job.Subtasks {
		if st.Key == "" && st.ChunkIdx == args.ChunkIdx {
			st.Done = true
			break
		}
	}

	// Check if any map‐chunks still pending or in‐flight:
	mapPending, _ := mr.computeTaskCounts(args.Table)
	if mapPending > 0 {
		// Still mapping
		reply.Status = phaseMapping
		reply.MapTasksPending = mapPending
		reply.ReduceTasksPending = 0
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}

	// All map‐chunks are Done → time for reduce:
	if mr.job.Phase == phaseMapping {
		mr.publishReduceTasks()
	}

	_, reducePending := mr.computeTaskCounts(args.Table)
	reply.Status = phaseReducing
	reply.MapTasksPending = 0
	reply.ReduceTasksPending = reducePending
	reply.FilePath = ""
	reply.ChunkIdx = -1
	reply.Key = ""
	reply.Values = nil
	return nil
}

// RequestReduceTask hands out one “reduce key” at a time.
// If a key remains unassigned, we mark it Assigned and return:
//
//	Status = “reducing”
//	MapTasksPending = 0
//	ReduceTasksPending = how many remain
//	FilePath = "" // not used in reduce phase
//	ChunkIdx = -1
//	Key = the next word to reduce
//	Values = the slice of counts to sum
//
// Once every reduce‐key is Done, the server flips to “done,” and future calls
// just return Status=“done” with both pending counts == 0.
func (mr *MRServer) RequestReduceTask(payload msgs.GossipMessage, reply *logs.State) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	// If we’re not yet in reducing, or we’re already done, handle those cases:
	if mr.job.Phase == phaseMapping {
		// mapping still in progress (server hasn’t yet published reduce‐tasks)
		mapPending, _ := mr.computeTaskCounts(payload.Table)
		reply.Status = phaseMapping
		reply.MapTasksPending = mapPending
		reply.ReduceTasksPending = 0
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}
	if mr.job.Phase == phaseDone {
		reply.Status = phaseDone
		reply.MapTasksPending = 0
		reply.ReduceTasksPending = 0
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}

	// Now we’re in phaseReducing.  Find the first unassigned reduce‐key:
	for _, st := range mr.job.Subtasks {
		if st.Key != "" && !st.Assigned && !st.Done {
			st.Assigned = true
			st.ID = payload.TargetID
			_, reducePending := mr.computeTaskCounts(payload.Table)
			reply.Status = phaseReducing
			reply.MapTasksPending = 0
			reply.ReduceTasksPending = reducePending
			reply.FilePath = ""
			reply.ChunkIdx = -1
			reply.Key = st.Key
			reply.Values = st.Values
			return nil
		}
	}

	// No unassigned reduce‐keys.  Check if any are still in‐flight:
	_, reducePending := mr.computeTaskCounts(payload.Table)
	if reducePending > 0 {
		reply.Status = phaseReducing
		reply.MapTasksPending = 0
		reply.ReduceTasksPending = reducePending
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}

	// All reduce‐keys Done → final phaseDone:
	mr.job.Phase = phaseDone
	reply.Status = phaseDone
	reply.MapTasksPending = 0
	reply.ReduceTasksPending = 0
	reply.FilePath = ""
	reply.ChunkIdx = -1
	reply.Key = ""
	reply.Values = nil
	return nil
}

// SubmitReduceResult is called by a reducer who just summed “args.Values” for “args.Key”.
// We record Output[Key] = Value, mark that subtask done, and—if it was the last—
// flip to phaseDone.  Then reply with the updated State.
func (mr *MRServer) SubmitReduceResult(args ReduceResult, reply *logs.State) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	// Record final count:
	mr.job.Output[args.Key] = args.Value

	// Mark that subtask Done:
	for _, st := range mr.job.Subtasks {
		if st.Key == args.Key {
			st.Done = true
			break
		}
	}

	// Check if any reduce‐keys remain:
	_, reducePending := mr.computeTaskCounts(args.Table)
	if reducePending > 0 {
		reply.Status = phaseReducing
		reply.MapTasksPending = 0
		reply.ReduceTasksPending = reducePending
		reply.FilePath = ""
		reply.ChunkIdx = -1
		reply.Key = ""
		reply.Values = nil
		return nil
	}

	// All reduce‐keys done:
	mr.job.Phase = phaseDone
	reply.Status = phaseDone
	reply.MapTasksPending = 0
	reply.ReduceTasksPending = 0
	reply.FilePath = ""
	reply.ChunkIdx = -1
	reply.Key = ""
	reply.Values = nil
	return nil
}

// GetFinalOutput returns the final word‐count map once Phase=="done".
func (mr *MRServer) GetFinalOutput(sourceID int, out *map[string]int) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.job.Phase != phaseDone {
		return fmt.Errorf("not done yet")
	}

	uniqueWords := len(mr.job.Output)
	totalWords := 0
	for _, count := range mr.job.Output {
		totalWords += count
	}

	fmt.Printf("\n")
	fmt.Printf("Node %d stats\n", sourceID)
	fmt.Printf("Num unique words: %d\n", uniqueWords)
	fmt.Printf("Total num words:  %d\n", totalWords)

	*out = mr.job.Output
	return nil
}

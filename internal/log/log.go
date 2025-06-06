package log

/*
Info:
- log is a struct with a slice for committed entries, slice for pending entries, and holds the entry currently being processed
Relies on:
- send AEM checking for the newest waiting entry before sending
- AEM always being sent with the most up to date waiting entry
- send AEM also sending the latest committed (appended) entry for follower to check the index with
- AEM message send being idempotent (followers check the previous committed entry's index before deciding to add)
- getlastcommitted being called in send AEM to send most up-to-date log info
- startwaitingprocess being called by whoever wants to append something to the log
- commitwaitingentry being called by count log matches if it receives majority approval
*/

import (
	"errors"
	"sync"
)

/* Package level variable */
var Selflog *Log

type Log struct {
	mu           *sync.RWMutex // enables thread-safe methods
	Committed    []LogEntry    // all committed log data
	Pending      []LogEntry    // entries pending the approval process, only used by leader (followers don't use at all)
	Waitingentry LogEntry      // entry currently being appended, only really used by leader (followers use but immmediately commit)
}

func NewLog() *Log {
	committed := make([]LogEntry, 0, 10)
	pending := make([]LogEntry, 0, 10)

	return &Log{
		mu:        new(sync.RWMutex),
		Committed: committed,
		Pending:   pending,
		Waitingentry: NewLogEntry(
			false,
			NewMapReduceData(-1),
		),
	}
}

/* for use in send AEM */
func (l *Log) GetLastCommitted() LogEntry {
	last := NewLogEntry(
		false,
		NewMapReduceData(-1),
	)

	l.mu.RLock()
	defer l.mu.RUnlock()

	if lenCommitted := len(l.Committed); lenCommitted > 0 {
		last = l.Committed[lenCommitted-1]
	}

	return last
}

/* for use in send AEM */
func (l *Log) GetWaitingEntry() LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.Waitingentry
}

/* for use in count log matches */
func (l *Log) CommitWaitingEntry() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.Waitingentry.Exists {
		return errors.New("Log.CommitWaitingEntry(): waiting entry doesn't exist")
	}

	l.Waitingentry.index = len(l.Committed)
	l.Committed = append(l.Committed, l.Waitingentry)

	if lenPending := len(l.Pending); lenPending > 0 {
		l.Waitingentry = l.Pending[0]
		l.Waitingentry.Exists = true
		l.Pending = l.Pending[1:]
	} else {
		l.Waitingentry.Exists = false
	}

	return nil
}

/* for use wherever trying to add to log - need to have good term input */
func (l *Log) StartAppendEntryProcess(entry LogEntry) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	updatedWaitingEntry := false

	if !l.Waitingentry.Exists {
		l.Waitingentry = entry
		l.Waitingentry.Exists = true
		updatedWaitingEntry = true
	} else {
		l.Pending = append(l.Pending, entry)
	}

	return updatedWaitingEntry
}

/* for use in send entire log */
func (l *Log) GetCommittedCopy() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	logCopy := make([]LogEntry, len(l.Committed))
	copy(logCopy, l.Committed)

	return logCopy
}

/* for use in check entire log callback */
func (l *Log) ReplaceCommitted(newLog []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Committed = newLog
}

/* for use when getting demoted from leader */
func (l *Log) ClearPending() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Pending = make([]LogEntry, 0, 10)
	l.Waitingentry = NewLogEntry(
		false,
		NewMapReduceData(-1),
	)
}

// func (l *Log) Add(entry LogEntry) error {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	index := len(l.Committed)
// 	entry.Index = index
// 	l.Committed = append(l.Committed, entry)

// 	return nil
// }

// func (l *Log) Update(entry LogEntry, index int) error {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	if s := len(l.Committed); index >= 0 && index < s {
// 		l.Committed[index] = entry
// 	} else {
// 		return errors.New("Log.Update(): index out of range")
// 	}

// 	return nil
// }

// func (l *Log) GetEntry(index int) LogEntry {
// 	temp := LogEntry{
// 		Exists: false,
// 		Index:  -1,
// 	}

// 	l.mu.RLock()
// 	defer l.mu.RUnlock()

// 	if s := len(l.Committed); index >= 0 && index < s {
// 		temp = l.Committed[index]
// 	}

// 	return temp
// }

// func (l *Log) GetSize() int {
// 	l.mu.RLock()
// 	defer l.mu.RUnlock()

// 	return len(l.Committed)
// }

// func (l *Log) GetPendingSize() int {
// 	l.mu.RLock()
// 	defer l.mu.RUnlock()

// 	return len(l.Pending)
// }

// func (l *Log) AddPending(entry LogEntry) error {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	index := len(l.Pending)
// 	entry.Index = index
// 	l.Pending = append(l.Pending, entry)

// 	return nil
// }

// func (l *Log) UpdatePending(entry LogEntry, index int) error {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	if s := len(l.Pending); index >= 0 && index < s {
// 		l.Pending[index] = entry
// 	} else {
// 		return errors.New("Log.UpdatePending(): index out of range")
// 	}

// 	return nil
// }

// func (l *Log) GetPendingEntry(index int) LogEntry {
// 	temp := LogEntry{
// 		Exists: false,
// 		Index:  -1,
// 	}

// 	l.mu.RLock()
// 	defer l.mu.RUnlock()

// 	if s := len(l.Pending); index >= 0 && index < s {
// 		temp = l.Pending[index]
// 	}

// 	return temp
// }

// func (l *Log) CheckWaitingEntry() bool {
// 	l.mu.RLock()
// 	defer l.mu.RUnlock()

// 	return l.Waitingentry.Exists
// }

// func (l *Log) UpdateWaitingEntry(entry LogEntry) error {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	l.Waitingentry = entry

// 	return nil
// }

package log

import (
	"errors"
	"sync"
)

/* Package level variable */
var Selflog *Log

type Log struct {
	mu      *sync.RWMutex // enables thread-safe methods
	Entries []LogEntry    // all log data
	Pending []LogEntry    // entries pending the approval process
	// Pending []LogEntry ? then can just loop through these pending to and add the matching one after LogMatchMessage
	// 	or should LogMatchCounter have the current LogEntry
	//	would then assume that could only have one pending LogEntry - this ok ?
	// TODO - check if we want to have this
	//	- OR it'd just be one thing allowed to be proposed (sent via AppendEntryMessage at a time)
	// Pending entries implementation
	// 	- would go in FIFO order
	// 	- add to pending as writes accumulate
	//	- can bypass pending if current appendEntry proposal is committed and len(Pending) == 0
}

func NewLog() *Log {
	entries := make([]LogEntry, 0, 10)
	pending := make([]LogEntry, 0, 10)

	// entries[0] = LogEntry{ // TODO - change this so that doesn't have 1 initially in log, just assumes that others will be able to check len via Log.GetSize()
	// 	Exists: false,
	// 	Index:  0,
	// }

	return &Log{
		mu:      new(sync.RWMutex),
		Entries: entries,
		Pending: pending,
	}
}

func (l *Log) Add(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	index := len(l.Entries) // TODO - CHECK - change this if possible to have temporary entries
	entry.Index = index     // would need to account for the pending entries in the log at indexes before this entry
	l.Entries = append(l.Entries, entry)

	return nil
}

func (l *Log) Update(entry LogEntry, index int) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if s := len(l.Entries); index >= 0 && index < s {
		l.Entries[index] = entry
	} else {
		return errors.New("Log.Update(): index out of range")
	}

	return nil
}

func (l *Log) GetEntry(index int) LogEntry {
	temp := LogEntry{
		Exists: false,
		Index:  -1,
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if s := len(l.Entries); index >= 0 && index < s {
		temp = l.Entries[index]
	}

	return temp
}

func (l *Log) GetSize() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.Entries)
}

func (l *Log) GetDeepCopy() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	logCopy := make([]LogEntry, len(l.Entries))
	copy(logCopy, l.Entries)

	return logCopy
}

func (l *Log) GetPendingSize() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.Pending)
}

func (l *Log) AddPending(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	index := len(l.Pending)
	entry.Index = index
	l.Pending = append(l.Pending, entry)

	return nil
}

func (l *Log) UpdatePending(entry LogEntry, index int) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if s := len(l.Pending); index >= 0 && index < s {
		l.Pending[index] = entry
	} else {
		return errors.New("Log.UpdatePending(): index out of range")
	}

	return nil
}

func (l *Log) GetPendingEntry(index int) LogEntry {
	temp := LogEntry{
		Exists: false,
		Index:  -1,
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if s := len(l.Pending); index >= 0 && index < s {
		temp = l.Pending[index]
	}

	return temp
}

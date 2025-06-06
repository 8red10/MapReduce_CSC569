package log

type LogEntry struct {
	Exists bool          // when reading: true if Log contains an entry, false otherwise
	Index  int           // index of entry in Log
	Term   int           // term entry was added to log
	Data   MapReduceData // data of entry in Log
	// Data   int // data of entry in Log
}

func NewLogEntry(exists bool, data MapReduceData) LogEntry {
	return LogEntry{
		Exists: exists,
		Index:  -1,
		Term:   -1,
		Data:   data,
	}
}

func (le LogEntry) GetIndex() int {
	return le.Index
}

func (le LogEntry) GetTerm() int {
	return le.Term
}

func NewMapReduceData(data int) MapReduceData {
	return MapReduceData{
		Data: data,
	}
}

func (e1 LogEntry) MatchesAndBothExist(e2 LogEntry) bool {
	if !e1.Exists || !e2.Exists {
		return false
	}
	return e1.Index == e2.Index && e1.Term == e2.Term
}

type MapReduceData struct {
	// FileContentID []int // constants identifying the worker's file ID
	// MapperStatus  int   // constant identifying the mapper action
	// ReducerStatus int   // constant identifying the reducer action
	Data int // temp data placeholder for before mapreduce integration
}

func (d1 MapReduceData) Matches(d2 MapReduceData) bool {
	// MapReduce TODO - @logan @david
	// 	- fix this method to reflect matching the rest of the attributes (ie FileContentID)
	//	- this function is used by the LogEntry.Matches() method
	//	- need this function to return a boolean
	// return d1.MapperStatus == d2.MapperStatus && d1.ReducerStatus == d2.ReducerStatus
	return d1.Data == d2.Data
}

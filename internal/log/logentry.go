package log

/* Package level variable */
var Waitingentry LogEntry

type LogEntry struct {
	Exists bool          // when reading: true if Log contains an entry, false otherwise
	Index  int           // index of entry in Log
	Data   MapReduceData // data of entry in Log
	// Data   int // data of entry in Log
}

func (e1 LogEntry) Matches(e2 LogEntry) bool {
	return e1.Exists == e2.Exists && e1.Index == e2.Index && e1.Data.Matches(e2.Data)
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

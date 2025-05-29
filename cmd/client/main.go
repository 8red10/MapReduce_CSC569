package main

func main() {
	exit_time := calcExitTime() // TODO
	server := getRPCServer()
	id := parseArgs(exit_time)
	if id < 0 {
		return
	}
	if createSelfNode(server, id) != nil {
		return
	}
	if createSelfTable(server, id) != nil {
		return
	}
	createSelfLog()
	initComplete()
	createWG()
	startTimers(server, id, exit_time)
	runUntilFailure(id)
}

package main

func main() {
	id, serverAddr, verboseFlag := parseFlags()
	exit_time := calcExitTime() // TODO

	server := getServerConnection(serverAddr)
	defer server.Close()

	createSelfNode(server, id)
	createSelfTable(server, id)
	createSelfLog()
	initComplete()

	createWG()
	startTimers(server, id, exit_time)

	go performMR(server, id, verboseFlag)
	runUntilFailure(id)
}

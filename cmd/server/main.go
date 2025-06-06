package main

func main() {
	filePath, port := parseFlags()
	registerRPCStructs()
	createMRServer(filePath)
	// handleRPC()
	serveConnections(filePath, port)
}

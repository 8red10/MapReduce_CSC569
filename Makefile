# Makefile for building and running the MapReduce server and client.

# You can override these on the command line, e.g.:
#   make server FILE=/path/to/file.txt PORT=5678
#   make client ID=<num> SERVER=hostname:5678

FILE     ?= files/pg-being_ernest.txt
# FILE     ?= files/pg-metamorphosis.txt
PORT     ?= 9005
ID		 ?= 1
SERVER   ?= localhost:9005
VERBOSE  ?= false

GO = go
SERVER_DIR = ./cmd/server
CLIENT_DIR = ./cmd/client

.PHONY: all build build_server build_client server client1 client2 client3 client4 client5 client6 client7 client8 clean

all: build

build: build_server build_client

build_server:
	$(GO) build -o $(SERVER_DIR) $(SERVER_DIR)

build_client:
	$(GO) build -o $(CLIENT_DIR) $(CLIENT_DIR)

server:
	$(SERVER_DIR)/server -file=$(FILE) -port=$(PORT)

client1: 
	$(CLIENT_DIR)/client -id=1 -server=$(SERVER) -v=$(VERBOSE)
client2: 
	$(CLIENT_DIR)/client -id=2 -server=$(SERVER) -v=$(VERBOSE)
client3: 
	$(CLIENT_DIR)/client -id=3 -server=$(SERVER) -v=$(VERBOSE)
client4: 
	$(CLIENT_DIR)/client -id=4 -server=$(SERVER) -v=$(VERBOSE)
client5: 
	$(CLIENT_DIR)/client -id=5 -server=$(SERVER) -v=$(VERBOSE)
client6: 
	$(CLIENT_DIR)/client -id=6 -server=$(SERVER) -v=$(VERBOSE)
client7: 
	$(CLIENT_DIR)/client -id=7 -server=$(SERVER) -v=$(VERBOSE)
client8: 
	$(CLIENT_DIR)/client -id=8 -server=$(SERVER) -v=$(VERBOSE)

clean: 
	rm ./cmd/server/server ./cmd/client/client

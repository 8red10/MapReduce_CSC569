GO = go
SERVER_DIR = ./cmd/server
CLIENT_DIR = ./cmd/client

.PHONY: all build build_server build_client clean server client1 client2 client3 client4 client5 client6 client7 client8

all: build

build: build_server build_client

build_server:
	$(GO) build -o $(SERVER_DIR) $(SERVER_DIR)

build_client:
	$(GO) build -o $(CLIENT_DIR) $(CLIENT_DIR)

server:
	$(SERVER_DIR)/server

client1:
	$(CLIENT_DIR)/client 1
client2:
	$(CLIENT_DIR)/client 2
client3:
	$(CLIENT_DIR)/client 3
client4:
	$(CLIENT_DIR)/client 4
client5:
	$(CLIENT_DIR)/client 5
client6:
	$(CLIENT_DIR)/client 6
client7:
	$(CLIENT_DIR)/client 7
client8:
	$(CLIENT_DIR)/client 8

clean: 
	rm ./cmd/server/server ./cmd/client/client

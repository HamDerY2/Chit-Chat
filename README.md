Chit Chat — Distributed Chat Service

1. Overview

Chit Chat is a distributed chat service implemented in Go using gRPC and Protocol Buffers. It allows multiple independent clients to join a chat, publish messages, receive broadcasts, and leave the conversation.
The server assigns a logical timestamp to each join notification, chat message, and leave notification. These timestamps establish a consistent ordering of chat events.
The implementation uses a server-client architecture with one server process and multiple client processes.


2. Features
Multiple clients can connect to one server.
Clients receive notifications when participants join or leave.
Published messages are broadcast to all currently active participants.
Each event includes a logical timestamp.
Messages are limited to 128 Unicode characters.
The server validates incoming messages independently of client-side validation.
Bidirectional gRPC streaming supports communication in both directions.
Go goroutines and channels handle concurrent connections and message delivery.
The Go standard logging library records server and client events.


3. Project Structure
text
chit-chat/
├── client/
│   └── main.go
├── grpc/
│   ├── chitchat.proto
│   ├── chitchat.pb.go
│   └── chitchat_grpc.pb.go
├── logs/
│   ├── test-result.md
│   └── test-run.log
├── server/
│   └── main.go
├── client-app
├── server-app
├── go.mod
├── go.sum
└── README.md


The client directory contains the client implementation. The server directory contains the chat server. The grpc directory contains the Protocol Buffers definition and generated Go files. The logs directory contains the integration-tests.
The executable files client-app and server-app are built locally and may be regenerated using the commands below.


4. Requirements
Go installed on the machine.
The Go dependencies specified in go.mod.
A terminal for starting the server and multiple client processes.
Port 50051 available for the gRPC server.


5. Build Instructions

Clone the repository and enter the project directory:
git clone (https://github.com/HamDerY2/Chit-Chat.git)
cd chit-chat

Download the required Go modules:
go mod download


Build the server and client:
go build -o server-app ./server
go build -o client-app ./client
These commands produce the server-app and client-app executables in the project root.


6. Running the System
Step 1: Start the server

In the project root, run:
./server-app


The server listens for gRPC connections on port 50051.
Keep the server terminal open while running the clients.

Step 2: Start client X
Open a new terminal, navigate to the project root, and run:
./client-app --id X

Step 3: Start client Y
Open another terminal in the same project directory and run:
./client-app --id Y

Step 4: Start client Z
Open a third client terminal and run:
./client-app --id Z

The system now consists of one server process and three independent client processes.
Each client displays received events with their logical timestamps. The client can publish a message by typing it and pressing Enter.

To leave the chat gracefully, type:
/quit
The server broadcasts a departure notification to the remaining active clients.


7. gRPC API
The gRPC service is defined in grpc/chitchat.proto.
The service provides one bidirectional streaming RPC:

protobuf
rpc Chat(stream ClientRequest) returns (stream ServerEvent);
Clients send ClientRequest messages to the server. These requests contain one of the following operations:

JoinRequest: registers a client with a unique client ID.
PublishRequest: publishes a chat message.
LeaveRequest: requests a graceful departure.

The server returns ServerEvent messages containing the event content, logical timestamp, event type, and associated client ID.
Bidirectional streaming allows the client and server to communicate independently over the same RPC stream.


8. Logical Timestamps
The server maintains a monotonically increasing logical clock protected by a mutex.
The clock is incremented for each join notification, published chat message, and leave notification. The resulting timestamp is included in the corresponding broadcast event.

For the recorded integration test, the events were ordered as follows:
Logical timestamp	Event
1	X joins
2	Y joins
3	Z joins
4	X publishes Hello everyone!
5	Y publishes Hi X!
6	Z publishes Hello X and Y!
7	X leaves
8	Y leaves
9	Z leaves
The sequence diagram in the project report illustrates this interaction.


9. Logging
The server and clients use Go's standard log package to record relevant events.
The server logs startup and shutdown, client connections and departures, broadcasts, message enqueuing, and successful gRPC stream sends. The client logs received messages and their logical timestamps.

The integration-tests are stored in:
logs/test-run.log
logs/test-result.md


10. Testing
The recorded integration test covers:

Three clients joining the chat.
Join notifications being broadcast to active participants.
Each client publishing a message.
Message broadcasts being enqueued for all active clients.
Clients leaving in sequence.
Leave notifications being sent to the remaining participants.
Logical timestamps increasing from 1 to 9 during the tested sequence.

The recorded test output and summary are included in the logs directory.
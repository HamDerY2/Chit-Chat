package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"unicode/utf8"

	pb "chit-chat/grpc"

	"google.golang.org/grpc"
)

type outboundMessage struct {
	event       *pb.ServerEvent
	recipientID string
}

type participant struct {
	id   string
	send chan *outboundMessage
}

type chatServer struct {
	pb.UnimplementedChitChatServer

	mu      sync.Mutex
	clock   uint64
	clients map[string]*participant
}

func newChatServer() *chatServer {
	return &chatServer{
		clients: make(map[string]*participant),
	}
}

// broadcastLocked must be called while holding s.mu
func (s *chatServer) broadcastLocked(
	content, eventType, clientID string,
) {
	s.clock++

	event := &pb.ServerEvent{
		Content:          content,
		LogicalTimestamp: s.clock,
		EventType:        eventType,
		ClientId:         clientID,
	}

	log.Printf(
		"component=Server event=broadcast type=%s client_id=%q logical_timestamp=%d content=%q",
		eventType, clientID, s.clock, content,
	)

	for id, client := range s.clients {
		message := &outboundMessage{
			event:       event,
			recipientID: id,
		}

		select {
		case client.send <- message:
			log.Printf(
				"component=Server event=message_enqueued recipient_id=%q sender_id=%q logical_timestamp=%d",
				id, clientID, s.clock,
			)
		default:
			// Delete slow clients
			delete(s.clients, id)
			close(client.send)

			log.Printf(
				"component=Server event=slow_client_removed client_id=%q",
				id,
			)
		}
	}
}

func (s *chatServer) Chat(
	stream grpc.BidiStreamingServer[pb.ClientRequest, pb.ServerEvent],
) error {
	out := make(chan *outboundMessage, 256)
	var current *participant

	// One goroutine sends messages on this client's stream.
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		for {
			select {
			case <-stream.Context().Done():
				return

			case message, ok := <-out:
				if !ok {
					return
				}

				if err := stream.Send(message.event); err != nil {
					log.Printf(
						"component=Server event=send_error recipient_id=%q error=%q",
						message.recipientID, err,
					)
					return
				}

				log.Printf(
					"component=Server event=message_sent recipient_id=%q sender_id=%q logical_timestamp=%d",
					message.recipientID,
					message.event.ClientId,
					message.event.LogicalTimestamp,
				)
			}
		}
	}()

	defer func() {
		s.mu.Lock()

		if current != nil && s.clients[current.id] == current {
			delete(s.clients, current.id)
			close(out)

			s.broadcastLeaveLocked(current.id)

			log.Printf(
				"component=Server event=client_disconnected client_id=%q",
				current.id,
			)
		}

		s.mu.Unlock()
		<-writerDone
	}()

	for {
		request, err := stream.Recv()

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch r := request.Request.(type) {

		case *pb.ClientRequest_Join:
			id := r.Join.GetClientId()

			if id == "" {
				continue
			}

			s.mu.Lock()

			if current != nil || s.clients[id] != nil {
				s.mu.Unlock()
				continue
			}

			current = &participant{
				id:   id,
				send: out,
			}
			s.clients[id] = current

			s.clock++
			content := fmt.Sprintf(
				"Participant %s joined Chit Chat at logical time %d",
				id, s.clock,
			)

			s.broadcastAtCurrentLocked(content, "join", id)

			log.Printf(
				"component=Server event=client_connected client_id=%q",
				id,
			)

			s.mu.Unlock()

		case *pb.ClientRequest_Publish:
			if current == nil {
				continue
			}

			content := r.Publish.GetContent()

			if !utf8.ValidString(content) ||
				utf8.RuneCountInString(content) > 128 {
				log.Printf(
					"component=Server event=message_rejected client_id=%q",
					current.id,
				)
				continue
			}

			s.mu.Lock()
			s.broadcastLocked(content, "chat", current.id)
			s.mu.Unlock()

		case *pb.ClientRequest_Leave:
			if current == nil {
				continue
			}

			s.mu.Lock()

			if s.clients[current.id] == current {
				delete(s.clients, current.id)
				close(out)

				s.broadcastLeaveLocked(current.id)

				log.Printf(
					"component=Server event=client_left client_id=%q",
					current.id,
				)
			}

			s.mu.Unlock()
			return nil
		}
	}
}

func (s *chatServer) broadcastAtCurrentLocked(
	content, eventType, clientID string,
) {
	event := &pb.ServerEvent{
		Content:          content,
		LogicalTimestamp: s.clock,
		EventType:        eventType,
		ClientId:         clientID,
	}

	log.Printf(
		"component=Server event=broadcast type=%s client_id=%q logical_timestamp=%d content=%q",
		eventType, clientID, s.clock, content,
	)

	for id, client := range s.clients {
		message := &outboundMessage{
			event:       event,
			recipientID: id,
		}

		select {
		case client.send <- message:
			log.Printf(
				"component=Server event=message_enqueued recipient_id=%q sender_id=%q logical_timestamp=%d",
				id, clientID, s.clock,
			)
		default:
			delete(s.clients, id)
			close(client.send)

			log.Printf(
				"component=Server event=slow_client_removed client_id=%q",
				id,
			)
		}
	}
}

func (s *chatServer) broadcastLeaveLocked(id string) {
	s.clock++

	content := fmt.Sprintf(
		"Participant %s left Chit Chat at logical time %d",
		id, s.clock,
	)

	s.broadcastAtCurrentLocked(content, "leave", id)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()
	pb.RegisterChitChatServer(server, newChatServer())

	log.Printf("component=Server event=startup address=:50051")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Printf("component=Server event=shutdown")
		server.GracefulStop()
	}()

	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

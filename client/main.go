package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"unicode/utf8"

	pb "chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.String("id", "", "unique client ID")
	flag.Parse()

	if strings.TrimSpace(*id) == "" {
		log.Fatal("provide a client ID, e.g. --id X")
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := pb.NewChitChatClient(conn).Chat(ctx)
	if err != nil {
		log.Fatal(err)
	}

	if err := stream.Send(&pb.ClientRequest{
		Request: &pb.ClientRequest_Join{
			Join: &pb.JoinRequest{ClientId: *id},
		},
	}); err != nil {
		log.Fatal(err)
	}

	log.Printf("component=Client event=connected client_id=%q", *id)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Printf("component=Client event=receive_error error=%q", err)
				return
			}

			log.Printf(
				"component=Client event=message_received client_id=%q type=%s logical_timestamp=%d content=%q",
				*id, event.EventType, event.LogicalTimestamp, event.Content,
			)
			log.Printf("[%d] %s", event.LogicalTimestamp, event.Content)
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	log.Println("Type a message and press Enter. Type /quit to leave.")

	for scanner.Scan() {
		content := scanner.Text()

		if content == "/quit" {
			err := stream.Send(&pb.ClientRequest{
				Request: &pb.ClientRequest_Leave{
					Leave: &pb.LeaveRequest{ClientId: *id},
				},
			})
			if err != nil {
				log.Printf("component=Client event=leave_send_error error=%q", err)
			}
			break
		}

		if !utf8.ValidString(content) || utf8.RuneCountInString(content) > 128 {
			log.Println("Message rejected: use valid UTF-8 and at most 128 characters.")
			continue
		}

		if err := stream.Send(&pb.ClientRequest{
			Request: &pb.ClientRequest_Publish{
				Publish: &pb.PublishRequest{Content: content},
			},
		}); err != nil {
			log.Printf("component=Client event=publish_error error=%q", err)
			break
		}
	}

	_ = stream.CloseSend()
	cancel()
	wg.Wait()

	log.Printf("component=Client event=shutdown client_id=%q", *id)
}

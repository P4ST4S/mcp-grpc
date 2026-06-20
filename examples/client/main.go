// Command client connects to the gRPC MCP echo server, initializes a session,
// lists tools and calls "echo".
//
//	go run ./examples/client -addr localhost:7777 -text "hello over grpc"
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/P4ST4S/mcp-grpc/transport"
)

func main() {
	addr := flag.String("addr", "localhost:7777", "server address")
	text := flag.String("text", "hello over grpc", "text to echo")
	flag.Parse()

	// The caller owns and configures the ClientConn (TLS, keepalive, message
	// sizes). v0.1 exposes no Dial helper on purpose. insecure here for the
	// example only.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-grpc-example-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, &transport.ClientTransport{Conn: conn}, nil)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = session.Close() }()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		log.Fatalf("list tools: %v", err)
	}
	for _, t := range tools.Tools {
		log.Printf("tool: %s — %s", t.Name, t.Description)
	}

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "echo",
		Arguments: map[string]any{"text": *text},
	})
	if err != nil {
		log.Fatalf("call echo: %v", err)
	}
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			log.Printf("echo result: %s", tc.Text)
		}
	}
}

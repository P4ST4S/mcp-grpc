// Command server runs a minimal MCP server exposing an "echo" tool over the
// gRPC transport, listening on a TCP address.
//
// Run alongside examples/client:
//
//	go run ./examples/server -addr :7777
//	go run ./examples/client -addr localhost:7777
package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/P4ST4S/mcp-grpc/examples/echo"
	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
	"github.com/P4ST4S/mcp-grpc/transport"
)

func main() {
	addr := flag.String("addr", ":7777", "TCP address to listen on")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// One shared MCP server serves every incoming stream as its own session.
	mcpServer := echo.NewServer()

	grpcServer := grpc.NewServer()
	mcpgrpcv1.RegisterMCPTransportServer(grpcServer, transport.NewHandler(mcpServer))

	log.Printf("mcp-grpc echo server listening on %s", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

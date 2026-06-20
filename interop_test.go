package mcpgrpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/P4ST4S/mcp-grpc/examples/echo"
	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
	"github.com/P4ST4S/mcp-grpc/transport"
)

// dialBufconn wires a real HTTP/2 gRPC stack over an in-memory bufconn pipe: a
// *grpc.Server running the echo handler and a *grpc.ClientConn dialing it. It
// returns the client conn and registers all teardown via t.Cleanup.
func dialBufconn(t *testing.T) *grpc.ClientConn {
	t.Helper()

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)

	grpcServer := grpc.NewServer()
	mcpgrpcv1.RegisterMCPTransportServer(grpcServer, transport.NewHandler(echo.NewServer()))

	go func() {
		// Serve returns when the listener closes; ignore that error in tests.
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	})
	return conn
}

// TestInterop_InitializeListCall is the headline end-to-end test: a real MCP
// client over ClientTransport talks to a real MCP server over the gRPC handler,
// across a real HTTP/2 stack (bufconn). initialize -> tools/list -> tools/call.
func TestInterop_InitializeListCall(t *testing.T) {
	conn := dialBufconn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mcp.NewClient(&mcp.Implementation{Name: "interop-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, &transport.ClientTransport{Conn: conn}, nil)
	if err != nil {
		t.Fatalf("client.Connect (initialize): %v", err)
	}
	defer func() { _ = session.Close() }()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools.Tools) != 1 || tools.Tools[0].Name != "echo" {
		t.Fatalf("expected single tool 'echo', got %+v", tools.Tools)
	}

	const want = "round-trip over grpc"
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "echo",
		Arguments: map[string]any{"text": want},
	})
	if err != nil {
		t.Fatalf("CallTool echo: %v", err)
	}
	if res.IsError {
		t.Fatalf("echo returned tool error: %+v", res.Content)
	}
	if len(res.Content) != 1 {
		t.Fatalf("expected 1 content item, got %d", len(res.Content))
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if tc.Text != want {
		t.Fatalf("echo mismatch: got %q want %q", tc.Text, want)
	}
}

// TestInterop_CleanClose verifies that closing the client session ends the
// server-side handler cleanly (no hang): the server's blocking Wait() must
// return when the stream closes.
func TestInterop_CleanClose(t *testing.T) {
	conn := dialBufconn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mcp.NewClient(&mcp.Implementation{Name: "close-client", Version: "0.1.0"}, nil)
	session, err := client.Connect(ctx, &transport.ClientTransport{Conn: conn}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// A first call proves the session is live.
	if _, err := session.ListTools(ctx, nil); err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	// Closing should not hang and should be reflected on both ends.
	done := make(chan error, 1)
	go func() { done <- session.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("session.Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session.Close hung — handler did not return")
	}
}

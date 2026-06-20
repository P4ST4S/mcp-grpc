package main

import (
	"context"
	"crypto/tls"
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/P4ST4S/mcp-grpc/examples/echo"
	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
	"github.com/P4ST4S/mcp-grpc/transport"
)

// startTLSEchoServer brings up the echo MCP server over a real TCP listener
// with TLS, returning its address. Teardown is registered via t.Cleanup.
func startTLSEchoServer(t *testing.T, fix tlsFixture) string {
	t.Helper()

	cert, err := tls.LoadX509KeyPair(fix.serverCert, fix.serverKey)
	if err != nil {
		t.Fatalf("load server keypair: %v", err)
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(grpc.Creds(creds))
	mcpgrpcv1.RegisterMCPTransportServer(srv, transport.NewHandler(echo.NewServer()))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

// buildBridge compiles the bridge binary into t.TempDir() and returns its path.
func buildBridge(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "mcp-grpc-bridge")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build bridge: %v\n%s", err, out)
	}
	return bin
}

// TestBridge_E2E_TLS is the headline v0.2 test: a real MCP client talks stdio
// to the bridge sub-process, which relays over a real TCP+TLS gRPC connection
// to the echo server. initialize -> tools/list -> tools/call echo.
func TestBridge_E2E_TLS(t *testing.T) {
	fix := newTLSFixture(t)
	addr := startTLSEchoServer(t, fix)
	bin := buildBridge(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The client launches the bridge as a sub-process and speaks MCP over its
	// stdio. The bridge dials the server over TLS, trusting our test CA, and
	// overriding the server name to the cert's CN ("localhost") since we dial
	// by 127.0.0.1:port.
	cmd := exec.CommandContext(ctx, bin,
		"-addr", addr,
		"-tls",
		"-ca", fix.caFile,
		"-server-name", fix.serverName,
	)
	client := mcp.NewClient(&mcp.Implementation{Name: "bridge-test-client", Version: "0.2.0"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("connect through bridge (initialize): %v", err)
	}
	defer func() { _ = session.Close() }()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools through bridge: %v", err)
	}
	if len(tools.Tools) != 1 || tools.Tools[0].Name != "echo" {
		t.Fatalf("expected single tool 'echo', got %+v", tools.Tools)
	}

	const want = "round-trip through the tls bridge"
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "echo",
		Arguments: map[string]any{"text": want},
	})
	if err != nil {
		t.Fatalf("CallTool echo through bridge: %v", err)
	}
	if res.IsError {
		t.Fatalf("echo returned tool error: %+v", res.Content)
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if tc.Text != want {
		t.Fatalf("echo mismatch through bridge: got %q want %q", tc.Text, want)
	}
}

package transport

import (
	"context"
	"fmt"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"

	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
)

// ClientTransport is an mcp.Transport that opens an MCP session over a gRPC
// bidirectional stream on a caller-provided *grpc.ClientConn.
//
// The caller owns the ClientConn: they configure TLS/keepalive/message sizes
// and are responsible for closing it. v0.1 intentionally exposes no Dial
// helper (grpc.Dial is deprecated in favour of grpc.NewClient with different
// fail-fast semantics, and reusing a single ClientConn is the recommended
// pattern).
type ClientTransport struct {
	// Conn is the gRPC connection to dial the MCPTransport service on. Required.
	Conn *grpc.ClientConn

	// CallOptions are passed through to the Connect RPC (e.g. per-call message
	// size overrides). Optional.
	CallOptions []grpc.CallOption
}

// Connect implements mcp.Transport. It is called exactly once by
// Client.Connect. It opens the bidi stream and returns a Connection bound to
// it.
func (t *ClientTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	if t.Conn == nil {
		return nil, fmt.Errorf("mcp-grpc: ClientTransport.Conn is nil")
	}

	// Derive a cancellable context that outlives Connect: Close() cancels it,
	// which unblocks any in-flight Recv on the stream (the stream's lifetime is
	// tied to this context, the parent that Recv watches).
	streamCtx, cancel := context.WithCancel(ctx)

	client := mcpgrpcv1.NewMCPTransportClient(t.Conn)
	stream, err := client.Connect(streamCtx, t.CallOptions...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("mcp-grpc: open stream: %w", err)
	}

	return &clientConn{
		writer: writer{stream: stream},
		stream: stream,
		cancel: cancel,
	}, nil
}

// clientConn is the client side of the transport. Close cancels the call
// context, which both ends the stream and unblocks Recv — so Read can call
// Recv directly without a pump goroutine (unlike serverConn).
type clientConn struct {
	writer
	stream    grpc.BidiStreamingClient[mcpgrpcv1.Frame, mcpgrpcv1.Frame]
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func (c *clientConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	// Recv watches the stream's context (cancelled by Close), so a concurrent
	// Close unblocks this call. We don't need the per-Read ctx select that the
	// server side requires.
	f, err := c.stream.Recv()
	if err != nil {
		return nil, readError(err)
	}
	return decodeFrame(f)
}

func (c *clientConn) Write(_ context.Context, msg jsonrpc.Message) error {
	return c.write(msg)
}

// Close is idempotent and safe under concurrent calls. It flips the writer's
// closed flag under the lock, then tears down OUTSIDE the lock (cancel) so an
// in-flight Write cannot deadlock against it.
func (c *clientConn) Close() error {
	c.markClosed() // serialize the flag flip with Write
	c.closeOnce.Do(func() {
		c.cancel()
	})
	return nil
}

// SessionID is part of mcp.Connection. Session identity belongs in gRPC
// metadata, not in the transport frame, so this returns the empty string.
// (The SDK marks SessionID for removal in a future version.)
func (c *clientConn) SessionID() string { return "" }

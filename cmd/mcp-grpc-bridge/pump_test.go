package main

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// fakeConn is an in-memory mcp.Connection: Read drains an inbound channel,
// Write appends to a recorded slice. Close makes a pending Read return io.EOF.
type fakeConn struct {
	in     chan jsonrpc.Message
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	written []jsonrpc.Message
	closed  bool
}

func newFakeConn() *fakeConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeConn{in: make(chan jsonrpc.Message, 16), ctx: ctx, cancel: cancel}
}

func (c *fakeConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case m := <-c.in:
		return m, nil
	case <-c.ctx.Done():
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConn) Write(_ context.Context, msg jsonrpc.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = append(c.written, msg)
	return nil
}

func (c *fakeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.cancel()
	}
	return nil
}

func (c *fakeConn) SessionID() string { return "" }

func (c *fakeConn) recorded() []jsonrpc.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]jsonrpc.Message(nil), c.written...)
}

func mustReq(t *testing.T, id int, method string) jsonrpc.Message {
	t.Helper()
	rid, err := jsonrpc.MakeID(float64(id))
	if err != nil {
		t.Fatalf("MakeID: %v", err)
	}
	return &jsonrpc.Request{ID: rid, Method: method}
}

// TestPump_RelaysBothDirections feeds messages into each side and asserts they
// arrive verbatim on the other, then a clean close ends the pump without error.
func TestPump_RelaysBothDirections(t *testing.T) {
	a := newFakeConn() // stdio side
	b := newFakeConn() // grpc side

	a.in <- mustReq(t, 1, "initialize")
	a.in <- mustReq(t, 2, "tools/call")
	b.in <- mustReq(t, 3, "notifications/message")

	done := make(chan error, 1)
	go func() { done <- pump(context.Background(), a, b) }()

	// Give the pump a moment to relay, then close one side cleanly.
	time.Sleep(50 * time.Millisecond)
	_ = a.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pump returned error on clean close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not return after close — deadlock")
	}

	// a's two requests should have been written to b; b's one to a.
	if got := len(b.recorded()); got != 2 {
		t.Fatalf("expected 2 messages relayed to b, got %d", got)
	}
	if got := len(a.recorded()); got != 1 {
		t.Fatalf("expected 1 message relayed to a, got %d", got)
	}
	// Integrity: method names preserved.
	if r, ok := b.recorded()[0].(*jsonrpc.Request); !ok || r.Method != "initialize" {
		t.Fatalf("first relayed message corrupted: %+v", b.recorded()[0])
	}
}

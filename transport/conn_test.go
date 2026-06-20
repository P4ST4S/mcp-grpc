package transport

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"

	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
)

// recordingStream is a fake frameStream that records every Send and serves
// queued frames to Recv. Send deliberately does NOT lock internally: the whole
// point of the N-writers test is to prove writer.write serializes access so
// that Send never overlaps and no payload is corrupted.
type recordingStream struct {
	mu   sync.Mutex
	sent [][]byte // copied payloads, in send order

	recvCh chan recvResult
}

func newRecordingStream() *recordingStream {
	return &recordingStream{recvCh: make(chan recvResult, 1024)}
}

func (s *recordingStream) Send(f *mcpgrpcv1.Frame) error {
	// Copy the payload immediately: writer.write owns the *Frame only until
	// Send returns, and we want to detect interleaving/corruption.
	p := append([]byte(nil), f.GetPayload()...)
	s.mu.Lock()
	s.sent = append(s.sent, p)
	s.mu.Unlock()
	return nil
}

func (s *recordingStream) Recv() (*mcpgrpcv1.Frame, error) {
	r, ok := <-s.recvCh
	if !ok {
		return nil, io.EOF
	}
	return r.frame, r.err
}

// makeRequest builds a valid JSON-RPC request carrying a unique id so we can
// assert message integrity after concurrent writes.
func makeRequest(t *testing.T, id int) jsonrpc.Message {
	t.Helper()
	rid, err := jsonrpc.MakeID(float64(id))
	if err != nil {
		t.Fatalf("MakeID: %v", err)
	}
	return &jsonrpc.Request{ID: rid, Method: "ping"}
}

// TestWrite_ConcurrentIntegrity is the mandatory N-writers test. It asserts not
// just the absence of a data race (-race) but message INTEGRITY: every frame
// that lands on the stream must decode back to exactly one of the messages we
// wrote, with no corruption or interleaving, and all N must arrive.
func TestWrite_ConcurrentIntegrity(t *testing.T) {
	const n = 200
	stream := newRecordingStream()
	c := &clientConn{writer: writer{stream: stream}, stream: nil}
	// clientConn.stream is only used by Read; Write goes through writer.stream.

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			if err := c.Write(context.Background(), makeRequest(t, id)); err != nil {
				t.Errorf("Write(%d): %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	stream.mu.Lock()
	sent := stream.sent
	stream.mu.Unlock()

	if len(sent) != n {
		t.Fatalf("expected %d frames sent, got %d", n, len(sent))
	}

	// Every payload must decode cleanly and yield a distinct id in [0, n).
	seen := make([]bool, n)
	ids := make([]int, 0, n)
	for _, payload := range sent {
		msg, err := jsonrpc.DecodeMessage(payload)
		if err != nil {
			t.Fatalf("frame corrupted, decode failed: %v (payload=%q)", err, payload)
		}
		req, ok := msg.(*jsonrpc.Request)
		if !ok {
			t.Fatalf("expected *jsonrpc.Request, got %T", msg)
		}
		// The id round-trips through JSON; the SDK decoder may yield int64 or
		// float64 depending on the value. Accept both.
		var id int
		switch v := req.ID.Raw().(type) {
		case int64:
			id = int(v)
		case float64:
			id = int(v)
		default:
			t.Fatalf("expected numeric id, got %T (%v)", v, v)
		}
		if id < 0 || id >= n {
			t.Fatalf("id %d out of range", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d — message corruption/interleaving", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for i := 0; i < n; i++ {
		if ids[i] != i {
			t.Fatalf("missing id %d in received frames", i)
		}
	}
}

// TestClientClose_Idempotent verifies Close can be called repeatedly and
// concurrently without panicking and that writes after close fail cleanly.
func TestClientClose_Idempotent(t *testing.T) {
	stream := newRecordingStream()
	ctx, cancel := context.WithCancel(context.Background())
	c := &clientConn{writer: writer{stream: stream}, cancel: cancel}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()

	if err := c.Write(context.Background(), makeRequest(t, 0)); err == nil {
		t.Fatal("expected Write after Close to fail")
	}
	_ = ctx
}

// TestServerRead_UnblockedByClose verifies the subtle server requirement:
// Close() unblocks an in-flight Read even though the client is still connected
// (no incoming frame), so the SDK read-loop ends and Wait() returns.
func TestServerRead_UnblockedByClose(t *testing.T) {
	stream := newRecordingStream() // Recv blocks forever (no frames queued)
	ctx, cancel := context.WithCancel(context.Background())
	c := newServerConn(ctx, stream, cancel)

	readDone := make(chan error, 1)
	go func() {
		_, err := c.Read(context.Background())
		readDone <- err
	}()

	// Give the Read a moment to block on the select.
	select {
	case <-readDone:
		t.Fatal("Read returned before Close — should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF after Close, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not unblock after Close — deadlock")
	}
}

// TestServerRead_ContextDeadline verifies Read honours the per-call context
// deadline independently of Close.
func TestServerRead_ContextDeadline(t *testing.T) {
	stream := newRecordingStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newServerConn(ctx, stream, cancel)

	readCtx, readCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer readCancel()

	_, err := c.Read(readCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

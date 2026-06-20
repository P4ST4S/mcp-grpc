// Package transport implements a gRPC transport for the Model Context Protocol
// (MCP). It carries opaque JSON-RPC messages over a single gRPC bidirectional
// stream, plugging into the official go-sdk mcp.Transport / mcp.Connection
// interfaces.
//
// Two halves make up the transport:
//
//   - ClientTransport opens a stream on a caller-provided *grpc.ClientConn.
//   - ServerTransport is built per-stream by the gRPC handler (NewHandler).
//
// The client and server connections are deliberately distinct types because
// Close is asymmetric (see clientConn vs serverConn): a client cancels its own
// call context, while a server signals its blocking handler to return.
package transport

import (
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
)

// frameStream is the common surface of grpc.BidiStreamingServer[Frame, Frame]
// and grpc.BidiStreamingClient[Frame, Frame]. Both expose Send/Recv with these
// exact signatures, which lets clientConn and serverConn share the serialized
// write path below.
type frameStream interface {
	Send(*mcpgrpcv1.Frame) error
	Recv() (*mcpgrpcv1.Frame, error)
}

// writer serializes Send on a single gRPC stream. gRPC forbids concurrent
// SendMsg on the same stream (and concurrent CloseSend with SendMsg); two
// concurrent Sends corrupt the protobuf marshal buffer in a way the race
// detector does not reliably catch. mcp.Connection.Write may be called
// concurrently, so every writer must funnel through this mutex.
//
// The mutex guards only the flag check + Send. Teardown (cancel, CloseSend,
// waiting) must happen OUTSIDE this lock — see closeOnce in clientConn /
// serverConn — otherwise an in-flight Write and a concurrent Close deadlock on
// each other.
type writer struct {
	mu     sync.Mutex
	stream frameStream
	closed bool
}

// write encodes msg and sends it under the mutex. It returns a transport-level
// error (which the SDK turns into a Close) when the connection is already
// closed or the underlying Send fails.
func (w *writer) write(msg jsonrpc.Message) error {
	data, err := jsonrpc.EncodeMessage(msg)
	if err != nil {
		// A message we cannot encode is a programming/transport fault, not an
		// MCP-application error; surface it as InvalidArgument.
		return status.Errorf(codes.InvalidArgument, "mcp-grpc: encode message: %v", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return status.Error(codes.Canceled, "mcp-grpc: write on closed connection")
	}
	return w.stream.Send(&mcpgrpcv1.Frame{Payload: data})
}

// markClosed sets the closed flag under the lock and reports whether this call
// is the one that transitioned the writer to closed. Teardown is the caller's
// job, performed after this returns (outside any further Send).
func (w *writer) markClosed() (firstClose bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.closed = true
	return true
}

// decodeFrame turns a received Frame into a jsonrpc.Message. A malformed
// payload is a transport error (InvalidArgument), distinct from a well-formed
// JSON-RPC message that happens to carry an MCP-application error.
func decodeFrame(f *mcpgrpcv1.Frame) (jsonrpc.Message, error) {
	msg, err := jsonrpc.DecodeMessage(f.GetPayload())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "mcp-grpc: decode frame: %v", err)
	}
	return msg, nil
}

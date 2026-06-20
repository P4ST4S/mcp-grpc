package transport

import (
	"context"
	"io"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
)

// NewHandler returns a gRPC MCPTransportServer that serves every incoming
// stream as a fresh MCP session backed by the single shared srv.
//
// Register it on a *grpc.Server:
//
//	mcpgrpcv1.RegisterMCPTransportServer(grpcServer, transport.NewHandler(mcpServer))
//
// One Connect call == one MCP session. The handler blocks for the whole
// session lifetime (see Connect), so each concurrent session holds one gRPC
// stream goroutine. Bound concurrency with grpc.MaxConcurrentStreams (v0.2).
func NewHandler(srv *mcp.Server) mcpgrpcv1.MCPTransportServer {
	return &handler{srv: srv}
}

type handler struct {
	mcpgrpcv1.UnimplementedMCPTransportServer
	srv *mcp.Server
}

// Connect implements the gRPC service. It builds a per-stream transport,
// connects the shared MCP server to it, then blocks on session.Wait() — the
// handler returning would close the stream, so its lifetime must equal the
// session's. We do NOT use mcp.Server.Run (single-transport); the per-stream
// handler is what lets one server serve many concurrent streams.
func (h *handler) Connect(stream mcpgrpcv1.MCPTransport_ConnectServer) error {
	// A child context whose cancel is the Close signal for this connection.
	// Note: cancelling this child does NOT unblock stream.Recv() (Recv watches
	// stream.Context(), the parent) — serverConn.Read handles that via a pump
	// + select on this same context's Done.
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	conn := newServerConn(ctx, stream, cancel)
	sess, err := h.srv.Connect(ctx, &serverTransport{conn: conn}, nil)
	if err != nil {
		return err
	}
	// Block until the session ends. When it does, returning closes the stream;
	// the pump's Recv then errors out and the pump goroutine exits.
	return sess.Wait()
}

// serverTransport is the mcp.Transport handed to mcp.Server.Connect. Its
// Connect simply returns the connection the handler already built around the
// live stream (the stream cannot be opened lazily server-side — it already
// exists). Connect is called exactly once by the SDK.
type serverTransport struct {
	conn *serverConn
}

func (t *serverTransport) Connect(context.Context) (mcp.Connection, error) {
	return t.conn, nil
}

// serverConn is the server side of the transport. Unlike clientConn it needs a
// receive pump: cancelling the handler's child context does not unblock
// stream.Recv(), so Read selects between a pumped channel and ctx.Done().
type serverConn struct {
	writer
	stream frameStream

	cancel context.CancelFunc // Close signal: makes the handler return
	ctx    context.Context    // handler's child context; Done() means "closed"

	recvCh    chan recvResult
	closeOnce sync.Once
}

type recvResult struct {
	frame *mcpgrpcv1.Frame
	err   error
}

// newServerConn builds the connection and starts the single receive pump. ctx
// is the handler's child context: Close cancels it (via cancel), and Read
// selects on its Done to unblock — cancelling it does NOT unblock stream.Recv
// (which watches the parent stream.Context()), which is exactly why the pump
// exists.
func newServerConn(ctx context.Context, stream frameStream, cancel context.CancelFunc) *serverConn {
	c := &serverConn{
		writer: writer{stream: stream},
		stream: stream,
		cancel: cancel,
		ctx:    ctx,
		recvCh: make(chan recvResult, 1),
	}
	// The single receive goroutine. It loops on Recv and pushes each result
	// onto recvCh. It exits when Recv errors (EOF on clean close, or a status
	// error once the handler returns and gRPC tears the stream down) or when
	// the connection context is cancelled.
	go func() {
		for {
			f, err := c.stream.Recv()
			select {
			case c.recvCh <- recvResult{frame: f, err: err}:
			case <-c.ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return c
}

func (c *serverConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case r := <-c.recvCh:
		if r.err != nil {
			return nil, readError(r.err)
		}
		return decodeFrame(r.frame)
	case <-c.ctx.Done():
		// Close() (or handler teardown) cancelled the context. Report a clean
		// end so the SDK read-loop stops without a parasitic error.
		return nil, io.EOF
	case <-ctx.Done():
		// Respect a deadline/cancel on the per-Read context too.
		return nil, ctx.Err()
	}
}

func (c *serverConn) Write(_ context.Context, msg jsonrpc.Message) error {
	return c.writer.write(msg)
}

// Close is idempotent and concurrency-safe. It flips the closed flag under the
// writer lock, then cancels OUTSIDE the lock. Cancelling makes the blocking
// handler's session end and the handler return, which closes the stream; the
// pump's in-flight Recv then errors and the pump exits.
func (c *serverConn) Close() error {
	c.markClosed()
	c.closeOnce.Do(func() {
		c.cancel()
	})
	return nil
}

func (c *serverConn) SessionID() string { return "" }

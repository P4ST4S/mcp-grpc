package transport

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// readError normalizes an error coming off stream.Recv (or the receive pump)
// into what the MCP SDK read-loop expects.
//
// The SDK treats io.EOF (via errors.Is) as a clean end-of-connection and any
// other error as a real failure. gRPC's RecvMsg returns io.EOF on a graceful
// half-close and a status error otherwise, so we:
//
//   - pass io.EOF through unchanged (clean close);
//   - map a Canceled status (our own Close cancelled the context) to io.EOF,
//     since from the SDK's point of view that is just the connection ending;
//   - return every other status error as-is so the SDK logs a real fault.
func readError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	if errors.Is(err, context.Canceled) {
		return io.EOF
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Canceled:
			// Triggered by our own Close() cancelling the context, or the peer
			// cancelling — either way the connection is over.
			return io.EOF
		case codes.Unavailable:
			// Connection lost. Surface it as a real error so the session ends
			// with a diagnosable cause rather than a silent clean close.
			return err
		}
	}
	return err
}

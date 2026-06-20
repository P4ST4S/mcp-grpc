package transport

import (
	"testing"

	mcpgrpcv1 "github.com/P4ST4S/mcp-grpc/mcpgrpcv1"
)

// FuzzFrameDecode feeds arbitrary bytes through the frame decode path. A
// malformed payload must always return a clean error (or a valid message),
// never panic.
func FuzzFrameDecode(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("{}"))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	f.Add([]byte(`{"jsonrpc":"2.0"`))
	f.Add([]byte("\x00\x01\x02\xff"))

	f.Fuzz(func(t *testing.T, payload []byte) {
		// Must not panic. The error/message result is unconstrained: we only
		// require that a bad payload yields an error and a good one a message,
		// never both nil and never a crash.
		msg, err := decodeFrame(&mcpgrpcv1.Frame{Payload: payload})
		if err == nil && msg == nil {
			t.Fatalf("decodeFrame returned nil message and nil error for %q", payload)
		}
	})
}

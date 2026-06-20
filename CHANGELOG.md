# Changelog

All notable changes to mcp-grpc are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-06-20

The bridge release. `mcp-grpc` now ships `cmd/mcp-grpc-bridge`, a transparent
stdio → gRPC bridge that lets existing stdio MCP clients (Claude Desktop,
Cursor, …) reach a remote MCP server over a gRPC connection, with TLS/mTLS. The
bridge is a local MCP server on its stdio side and an MCP client on its gRPC
side, in one process; it relays JSON-RPC messages in both directions without
interpreting MCP semantics.

### Added

- `cmd/mcp-grpc-bridge`: a stdio → gRPC bridge. It opens a stdio `mcp.Connection` and a gRPC `mcp.Connection` (via `transport.ClientTransport`) and pumps `jsonrpc.Message` values between them in both directions, ending cleanly when either side closes.
- TLS and mTLS support on the bridge's gRPC connection via `-tls`, `-ca`, `-cert`, `-key`, `-server-name`, and `-insecure-skip-verify` flags. TLS minimum version is 1.2; `-cert`/`-key` must be supplied together for mTLS.
- End-to-end test exercising a real MCP client → bridge sub-process → echo MCP server over a **real TCP listener with TLS** (certificates generated in-test), plus a unit test of the relay pump asserting both-direction relay and clean close.

### Changed

- README documents the bridge usage (plaintext, TLS, mTLS) and reframes the project's lasting value as connectivity rather than a scaffold.

## [0.1.0] - 2026-06-20

First release: a pluggable gRPC transport for the Model Context Protocol (MCP)
in Go, carrying MCP JSON-RPC traffic over a single gRPC bidirectional stream and
plugging into the official `go-sdk` `mcp.Transport` / `mcp.Connection`
interfaces. This is a library, not a binary.

### Added

- `transport.ClientTransport` implementing `mcp.Transport` over a gRPC bidi stream on a caller-provided `*grpc.ClientConn`. No Dial helper — the caller owns and configures the connection.
- `transport.NewHandler(*mcp.Server)` returning a gRPC `MCPTransportServer` that serves each incoming stream as a fresh MCP session, blocking on `ServerSession.Wait()` for the session lifetime.
- `mcp.grpc.v1.MCPTransport` proto contract with an opaque `Frame { bytes payload }`, generated via Buf into the public `mcpgrpcv1/` package.
- Serialized writes: a `sync.Mutex` around `stream.Send` (gRPC forbids concurrent `Send`) with teardown performed outside the lock to avoid a `Close`/`Write` deadlock.
- Cancellable server reads via a receive-pump goroutine plus `select`, so `Close` and per-read deadlines unblock a waiting `Read` even though cancelling a child context does not unblock `stream.Recv`.
- Asymmetric, idempotent, concurrency-safe `Close`: the client cancels its call context, the server signals its blocking handler to return.
- gRPC status to transport error mapping and `io.EOF` translation for graceful stream close.
- Runnable echo example (`examples/server`, `examples/client`) and a shared `examples/echo` package.
- End-to-end interop test over `bufconn` (`initialize` → `tools/list` → `tools/call echo`), a concurrent N-writers test asserting message integrity, a clean-close test, and `FuzzFrameDecode`.
- GitHub Actions CI (`go vet`, `go test -race`, fuzz smoke, `buf lint`/`buf breaking`, `golangci-lint`) and a release workflow on `v*` tags.

### Known Limitations

- Experimental; not yet production tested at scale.
- No bridge/proxy (`cmd/mcp-grpc-bridge`) yet — planned for v0.2.
- Opaque JSON-RPC payload only; no typed-per-method protobuf, so it gains HTTP/2 multiplexing but not the serialization savings of a typed proto.
- No Dial helper, no session multiplexing, no distributed stateless mode.
- TLS/mTLS over a real TCP listener is exercised in v0.2; v0.1 tests run over `bufconn`.
- Default gRPC message size is 4 MB; large `tools/call` results require raising `MaxRecvMsgSize`/`MaxSendMsgSize` on the caller's conn/server.

[Unreleased]: https://github.com/P4ST4S/mcp-grpc/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/P4ST4S/mcp-grpc/releases/tag/v0.1.0

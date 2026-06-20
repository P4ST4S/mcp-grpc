# mcp-grpc

A pluggable **gRPC transport for the Model Context Protocol (MCP)** in Go. It
carries MCP JSON-RPC traffic over a single gRPC bidirectional stream, plugging
directly into the official [`go-sdk`](https://github.com/modelcontextprotocol/go-sdk)
`mcp.Transport` / `mcp.Connection` interfaces.

> **Status: v0.1 — transport only.** This is not the destination. The MCP
> maintainers have committed to exactly two *official* transports (STDIO and
> Streamable HTTP) and to making custom transports easier; gRPC will be a
> custom transport. Google has announced an official, *typed* gRPC transport
> (Python first) aimed at cutting JSON serialization overhead. This library
> deliberately carries **opaque JSON-RPC bytes**, so it gains HTTP/2 multiplexing
> but **not** that serialization win — it is a transport/bridge, not a perf
> competitor to a typed proto. The release that will matter is **v0.2 (the
> bridge/proxy)**, whose value is connectivity (gRPC-native clients ↔ MCP
> JSON-RPC servers) regardless of opaque-vs-typed. v0.1 is the scaffold.

## What v0.1 does

- A `ClientTransport` and a `ServerTransport` implementing `mcp.Transport`.
- A minimal `.proto` (`mcp.grpc.v1.MCPTransport`) generated via Buf into the
  **public** `mcpgrpcv1/` package.
- Correct concurrency, asymmetric close, cancellable reads, and gRPC↔transport
  error mapping.
- Runnable echo example (server + client) and an end-to-end interop test.

## Usage

### Server

```go
mcpServer := mcp.NewServer(&mcp.Implementation{Name: "my-server", Version: "0.1.0"}, nil)
// ... mcp.AddTool(mcpServer, ...) ...

grpcServer := grpc.NewServer()
mcpgrpcv1.RegisterMCPTransportServer(grpcServer, transport.NewHandler(mcpServer))
grpcServer.Serve(lis)
```

One shared `mcp.Server` serves every incoming stream as its own session. The
gRPC handler blocks for the lifetime of the session (`ServerSession.Wait()`).

### Client

```go
// You own and configure the *grpc.ClientConn (TLS, keepalive, message sizes).
// v0.1 exposes no Dial helper on purpose.
conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))

client := mcp.NewClient(&mcp.Implementation{Name: "my-client", Version: "0.1.0"}, nil)
session, _ := client.Connect(ctx, &transport.ClientTransport{Conn: conn}, nil)
res, _ := session.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": "hi"}})
```

### Try the example

```sh
go run ./examples/server -addr :7777
go run ./examples/client -addr localhost:7777 -text "hello over grpc"
```

## Design notes

- **Opaque payload.** `Frame.payload` is one `jsonrpc.EncodeMessage` blob. We
  reuse the SDK's hardened (de)serialization rather than re-modelling the schema
  the SDK deliberately hides.
- **Writes are serialized** behind a mutex (`mcp.Connection.Write` may be called
  concurrently, but gRPC forbids concurrent `Send` on a stream). Close tears
  down *outside* that lock to avoid deadlock.
- **Server reads are cancellable** via a receive-pump goroutine + `select`, because
  cancelling a child context does not unblock `stream.Recv()` (it watches the
  parent `stream.Context()`).
- **Session id / auth / trace context belong in gRPC metadata**, not the Frame.
- **4 MB default gRPC message size.** A large `tools/call` result can exceed it;
  set `MaxRecvMsgSize`/`MaxSendMsgSize` on your conn/server as needed.

## Development

```sh
buf generate     # regenerate stubs into mcpgrpcv1/
buf lint
go test -race ./...
go test -run=xxx -fuzz=FuzzFrameDecode -fuzztime=30s ./transport/
```

## License

Apache-2.0.

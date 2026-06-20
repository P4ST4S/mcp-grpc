// Package echo defines a trivial MCP server with a single "echo" tool, shared
// by the server example and the interop tests.
package echo

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Input is the echo tool's typed input. The SDK derives the JSON schema from
// this struct.
type Input struct {
	Text string `json:"text" jsonschema:"the text to echo back"`
}

// Output is the echo tool's typed output.
type Output struct {
	Echo string `json:"echo" jsonschema:"the echoed text"`
}

// NewServer builds an mcp.Server exposing a single "echo" tool.
func NewServer() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "mcp-grpc-echo",
		Version: "0.1.0",
	}, nil)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "echo",
		Description: "Echoes back the provided text.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in Input) (*mcp.CallToolResult, Output, error) {
		out := Output{Echo: in.Text}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: in.Text}},
		}, out, nil
	})

	return srv
}

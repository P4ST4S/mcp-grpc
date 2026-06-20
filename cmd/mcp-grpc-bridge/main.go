// Command mcp-grpc-bridge is a transparent stdio -> gRPC bridge for MCP.
//
// It is a local MCP server on its stdio side (what Claude Desktop, Cursor, and
// other stdio MCP clients speak to) and an MCP client on its gRPC side (toward
// a remote MCP server served over the mcp-grpc transport). The two roles live
// in the same process: stdin/stdout in, gRPC bidi stream out, one hop.
//
// The bridge never interprets MCP semantics. It moves jsonrpc.Message values
// between the two connections in both directions, so it works for any MCP
// traffic (initialize, tools/*, resources/*, notifications, ...) without
// tracking sessions or schemas.
//
//	mcp-grpc-bridge -addr mcp.example.com:7777 \
//	    -tls -ca ca.pem -cert client.pem -key client-key.pem
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/P4ST4S/mcp-grpc/transport"
)

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatalf("mcp-grpc-bridge: %v", err)
	}
}

type config struct {
	addr       string
	useTLS     bool
	caFile     string
	certFile   string
	keyFile    string
	serverName string
	insecure   bool
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "", "address of the remote MCP gRPC server (host:port) (required)")
	flag.BoolVar(&cfg.useTLS, "tls", false, "use TLS for the gRPC connection")
	flag.StringVar(&cfg.caFile, "ca", "", "path to a CA certificate bundle to verify the server (PEM)")
	flag.StringVar(&cfg.certFile, "cert", "", "path to the client certificate for mTLS (PEM)")
	flag.StringVar(&cfg.keyFile, "key", "", "path to the client private key for mTLS (PEM)")
	flag.StringVar(&cfg.serverName, "server-name", "", "override the server name checked against its certificate")
	flag.BoolVar(&cfg.insecure, "insecure-skip-verify", false, "skip server certificate verification (TLS only; insecure)")
	flag.Parse()

	if cfg.addr == "" {
		fmt.Fprintln(os.Stderr, "mcp-grpc-bridge: -addr is required")
		flag.Usage()
		os.Exit(2)
	}
	return cfg
}

func run(ctx context.Context, cfg config) error {
	creds, err := transportCredentials(cfg)
	if err != nil {
		return fmt.Errorf("tls: %w", err)
	}

	// The caller (this bridge) owns the gRPC connection, per the v0.1 contract.
	conn, err := grpc.NewClient(cfg.addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.addr, err)
	}
	defer func() { _ = conn.Close() }()

	// gRPC side: open the MCP transport connection to the remote server.
	grpcConn, err := (&transport.ClientTransport{Conn: conn}).Connect(ctx)
	if err != nil {
		return fmt.Errorf("open grpc stream: %w", err)
	}
	defer func() { _ = grpcConn.Close() }()

	// stdio side: a Connection over this process's stdin/stdout.
	stdioConn, err := (&mcp.StdioTransport{}).Connect(ctx)
	if err != nil {
		return fmt.Errorf("open stdio: %w", err)
	}
	defer func() { _ = stdioConn.Close() }()

	log.Printf("mcp-grpc-bridge: relaying stdio <-> %s (tls=%v)", cfg.addr, cfg.useTLS)
	return pump(ctx, stdioConn, grpcConn)
}

// transportCredentials builds the gRPC transport credentials from the flags:
// insecure (no TLS), or a TLS config supporting a custom CA, mTLS client
// certs, and server-name override.
func transportCredentials(cfg config) (credentials.TransportCredentials, error) {
	if !cfg.useTLS {
		if cfg.caFile != "" || cfg.certFile != "" || cfg.keyFile != "" {
			return nil, errors.New("-ca/-cert/-key require -tls")
		}
		return insecure.NewCredentials(), nil
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.serverName,
		InsecureSkipVerify: cfg.insecure, //nolint:gosec // opt-in via explicit flag
	}

	if cfg.caFile != "" {
		pem, err := os.ReadFile(cfg.caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA %s: %w", cfg.caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in CA %s", cfg.caFile)
		}
		tlsCfg.RootCAs = pool
	}

	if (cfg.certFile == "") != (cfg.keyFile == "") {
		return nil, errors.New("-cert and -key must be provided together for mTLS")
	}
	if cfg.certFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.certFile, cfg.keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsCfg), nil
}

// pump relays jsonrpc.Message values between the two connections in both
// directions until either side ends (clean io.EOF) or errors. The first
// terminating direction cancels the other so both goroutines unwind.
func pump(ctx context.Context, a, b mcp.Connection) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- copyMessages(ctx, a, b) }() // stdio -> grpc
	go func() { errCh <- copyMessages(ctx, b, a) }() // grpc -> stdio

	// Wait for the first direction to finish, then tear down. A clean io.EOF is
	// a normal end of session, not an error.
	first := <-errCh
	cancel()
	_ = a.Close()
	_ = b.Close()
	<-errCh // let the second goroutine unwind

	if first != nil && !errors.Is(first, io.EOF) && !errors.Is(first, context.Canceled) {
		return first
	}
	return nil
}

// copyMessages reads from src and writes to dst until src ends. It returns the
// terminating error (io.EOF on clean close).
func copyMessages(ctx context.Context, src, dst mcp.Connection) error {
	for {
		msg, err := src.Read(ctx)
		if err != nil {
			return err
		}
		if err := dst.Write(ctx, msg); err != nil {
			return err
		}
	}
}

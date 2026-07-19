// tinysql-mcp-server – MCP server for the tinySQL embedded database.
//
// The server exposes tinySQL through the Model Context Protocol (MCP) over
// stdio, allowing any MCP-capable host (Claude Desktop, VS Code, etc.) to
// query, mutate, and introspect a tinySQL database.
//
// Usage:
//
//	tinysql-mcp-server [flags]
//
// Flags:
//
//	--dsn           Full tinySQL DSN (mem://?tenant=default or file:path?tenant=...&autosave=1).
//	                If both --dsn and --db-path are provided, --dsn takes precedence.
//	--db-path       Convenience shorthand for a file-backed database path.
//	--tenant        Tenant namespace (default: "default", or derived from --dsn).
//	--autosave      Enable auto-save for file-backed databases.
//	--readonly      Block all mutating tools (write_query, create_table).
//	--max-rows      Maximum rows returned by read_query (default: 1000; 0 = unlimited).
//	                This only truncates client-visible output, not engine-side
//	                computation — use --query-timeout to bound query execution itself.
//	--query-timeout Per-query timeout, e.g. "5s" (default: 30s; 0 = no timeout).
//	--log-level     Log verbosity: debug, info, warn, error (default: info).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"tinysql-mcp-server/internal/mcpserver"
	"tinysql-mcp-server/internal/tinysqldb"
)

// Defaults for the row/time limits that bound an LLM-agent-issued query. An
// agent (or a prompt-injected instruction reaching one) can issue arbitrary
// SQL, so an unmodified deployment must have a sane ceiling on both response
// size and execution time out of the box. Operators who genuinely want no
// limit can still pass an explicit 0 to opt back in.
const (
	defaultMaxRows      = 1000
	defaultQueryTimeout = 30 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		flagDSN      = flag.String("dsn", "", "Full tinySQL DSN (mem://?tenant=default or file:path?...)")
		flagDBPath   = flag.String("db-path", "", "Shorthand for a file-backed database path")
		flagTenant   = flag.String("tenant", "", `Tenant namespace (default "default")`)
		flagAutosave = flag.Bool("autosave", false, "Enable auto-save for file-backed databases")
		flagReadOnly = flag.Bool("readonly", false, "Block all mutating tools")
		flagMaxRows  = flag.Int("max-rows", defaultMaxRows, "Maximum rows returned by read_query (0 = unlimited). "+
			"Only truncates client-visible output, not engine-side computation; --query-timeout is the real "+
			"backstop against an expensive query.")
		flagQueryTimeout = flag.Duration("query-timeout", defaultQueryTimeout, `Per-query timeout, e.g. "5s" (0 = no timeout). `+
			"Bounds engine-side execution time; set to 0 only if you explicitly want unbounded queries.")
		flagLogLevel = flag.String("log-level", "info", "Log verbosity: debug, info, warn, error")
	)
	flag.Parse()

	// Configure structured logging.
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(*flagLogLevel)); err != nil {
		return fmt.Errorf("invalid --log-level %q: %w", *flagLogLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	if *flagDSN != "" && *flagDBPath != "" {
		slog.Warn("both --dsn and --db-path provided; --dsn takes precedence and --db-path is ignored")
	}

	cfg := tinysqldb.Config{
		DSN:          *flagDSN,
		DBPath:       *flagDBPath,
		Tenant:       *flagTenant,
		Autosave:     *flagAutosave,
		ReadOnly:     *flagReadOnly,
		MaxRows:      *flagMaxRows,
		QueryTimeout: *flagQueryTimeout,
	}

	ctx := context.Background()

	slog.Info("opening tinySQL database", "mode", dsnMode(cfg))
	store, err := tinysqldb.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			slog.Warn("close database", "err", cerr)
		}
	}()
	slog.Info("database ready", "tenant", store.Tenant)

	srv := mcpserver.New(store)
	slog.Info("starting MCP server over stdio")
	return srv.Run(ctx)
}

// dsnMode returns a log-safe description of the database mode.
func dsnMode(cfg tinysqldb.Config) string {
	if cfg.DSN != "" {
		if len(cfg.DSN) > 20 {
			return cfg.DSN[:20] + "..."
		}
		return cfg.DSN
	}
	if cfg.DBPath != "" {
		return "file:" + cfg.DBPath
	}
	return "mem://"
}

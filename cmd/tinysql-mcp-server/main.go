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
//	--max-rows      Maximum rows returned by read_query (0 = unlimited).
//	--query-timeout Per-query timeout, e.g. "5s" (0 = no timeout).
//	--log-level     Log verbosity: debug, info, warn, error (default: info).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"tinysql-mcp-server/internal/mcpserver"
	"tinysql-mcp-server/internal/tinysqldb"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		flagDSN          = flag.String("dsn", "", "Full tinySQL DSN (mem://?tenant=default or file:path?...)")
		flagDBPath       = flag.String("db-path", "", "Shorthand for a file-backed database path")
		flagTenant       = flag.String("tenant", "", `Tenant namespace (default "default")`)
		flagAutosave     = flag.Bool("autosave", false, "Enable auto-save for file-backed databases")
		flagReadOnly     = flag.Bool("readonly", false, "Block all mutating tools")
		flagMaxRows      = flag.Int("max-rows", 0, "Maximum rows returned by read_query (0 = unlimited)")
		flagQueryTimeout = flag.Duration("query-timeout", 0, `Per-query timeout, e.g. "5s" (0 = no timeout)`)
		flagLogLevel     = flag.String("log-level", "info", "Log verbosity: debug, info, warn, error")
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

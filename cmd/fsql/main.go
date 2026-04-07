// fsql – File System Query Language CLI
//
// fsql treats the filesystem as a relational database, enabling SQL queries
// over files, directories, and structured file contents (CSV, JSON, text).
//
// Usage:
//
//	fsql mount <name> <path>          Register a named filesystem mount
//	fsql umount <name>                Remove a named mount
//	fsql mounts                       List all registered mounts
//	fsql query [--scope <name>] <sql> Execute a SQL query
//	fsql index build <scope>          Build an index for a scope (stub)
//	fsql --mount <path> <sql>         Ad-hoc query with a temporary root mount
//
// Table-valued functions available in SQL:
//
//	files(path [, recursive])         Filesystem metadata table
//	lines(file)                       Text file lines table
//	csv_rows(file [, header])         CSV file rows table
//	json_rows(file [, path])          JSON file rows table
//
// Example:
//
//	fsql mount logs /var/log
//	fsql query "SELECT path, size FROM files('logs', true) WHERE ext = 'log'"
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"fsql/internal/adapter"
	"fsql/internal/scope"
	tinysql "github.com/SimonWaldherr/tinySQL"
)

const version = "0.1.0"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// Top-level flags
	fs := flag.NewFlagSet("fsql", flag.ContinueOnError)
	mountFlag := fs.String("mount", "", "Ad-hoc root path for the query (overrides named mounts)")
	outputFlag := fs.String("output", "table", "Output format: table, csv, json")
	scopeFlag := fs.String("scope", "", "Named scope to use as the default mount root")
	versionFlag := fs.Bool("version", false, "Print version and exit")

	// Parse flags, stopping at first non-flag argument
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	if *versionFlag {
		fmt.Println("fsql", version)
		return nil
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		printUsage(fs)
		return nil
	}

	// Initialise scope manager
	mgr, err := scope.NewManager()
	if err != nil {
		return fmt.Errorf("initialise scope manager: %w", err)
	}

	// Build the scope resolver: if --mount is given it takes precedence,
	// otherwise fall back to named mounts, then direct path resolution.
	resolver := buildResolver(mgr, *mountFlag, *scopeFlag)

	// Register FSQL table-valued functions with the tinySQL engine.
	adapter.RegisterAll(resolver)

	cmd := strings.ToLower(remaining[0])
	cmdArgs := remaining[1:]

	switch cmd {
	case "mount":
		return cmdMount(mgr, cmdArgs)
	case "umount", "unmount":
		return cmdUmount(mgr, cmdArgs)
	case "mounts":
		return cmdMounts(mgr)
	case "query":
		return cmdQuery(cmdArgs, *outputFlag)
	case "index":
		return cmdIndex(cmdArgs)
	default:
		// Treat the first argument as a SQL query (shorthand)
		return executeQuery(remaining[0], *outputFlag)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Subcommands
// ─────────────────────────────────────────────────────────────────────────────

func cmdMount(mgr *scope.Manager, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: fsql mount <name> <path>")
	}
	name, path := args[0], args[1]
	if err := mgr.Add(name, path); err != nil {
		return err
	}
	fmt.Printf("Mounted %q -> %s\n", name, path)
	return nil
}

func cmdUmount(mgr *scope.Manager, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: fsql umount <name>")
	}
	if err := mgr.Remove(args[0]); err != nil {
		return err
	}
	fmt.Printf("Unmounted %q\n", args[0])
	return nil
}

func cmdMounts(mgr *scope.Manager) error {
	mounts := mgr.List()
	if len(mounts) == 0 {
		fmt.Println("No mounts registered.")
		return nil
	}
	sort.Slice(mounts, func(i, j int) bool { return mounts[i].Name < mounts[j].Name })
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPATH")
	for _, m := range mounts {
		fmt.Fprintf(w, "%s\t%s\n", m.Name, m.Path)
	}
	return w.Flush()
}

func cmdQuery(args []string, output string) error {
	// Support: fsql query [--scope <name>] <sql>
	qfs := flag.NewFlagSet("query", flag.ContinueOnError)
	_ = qfs.String("scope", "", "Named scope") // already parsed at top level
	if err := qfs.Parse(args); err != nil {
		return err
	}
	if qfs.NArg() == 0 {
		return fmt.Errorf("usage: fsql query <sql>")
	}
	sql := strings.Join(qfs.Args(), " ")
	return executeQuery(sql, output)
}

func cmdIndex(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: fsql index build <scope>")
	}
	op := strings.ToLower(args[0])
	if op != "build" {
		return fmt.Errorf("unknown index operation %q (supported: build)", op)
	}
	scope := args[1]
	fmt.Printf("Index build for scope %q: not yet implemented (coming in a future release)\n", scope)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Query execution
// ─────────────────────────────────────────────────────────────────────────────

func executeQuery(sql, output string) error {
	db := tinysql.NewDB()
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		return fmt.Errorf("parse SQL: %w", err)
	}

	ctx := context.Background()
	rs, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	if rs == nil {
		return nil
	}

	return printResultSet(rs, output)
}

// ─────────────────────────────────────────────────────────────────────────────
// Output formatting
// ─────────────────────────────────────────────────────────────────────────────

func printResultSet(rs *tinysql.ResultSet, format string) error {
	switch strings.ToLower(format) {
	case "json":
		return printJSON(rs)
	case "csv":
		return printCSV(rs)
	default:
		return printTable(rs)
	}
}

func printTable(rs *tinysql.ResultSet) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(rs.Cols, "\t"))
	sep := make([]string, len(rs.Cols))
	for i, c := range rs.Cols {
		sep[i] = strings.Repeat("-", len(c))
	}
	fmt.Fprintln(w, strings.Join(sep, "\t"))
	for _, row := range rs.Rows {
		vals := make([]string, len(rs.Cols))
		for i, col := range rs.Cols {
			vals[i] = fmt.Sprintf("%v", row[col])
		}
		fmt.Fprintln(w, strings.Join(vals, "\t"))
	}
	return w.Flush()
}

func printCSV(rs *tinysql.ResultSet) error {
	w := csv.NewWriter(os.Stdout)
	if err := w.Write(rs.Cols); err != nil {
		return err
	}
	for _, row := range rs.Rows {
		rec := make([]string, len(rs.Cols))
		for i, col := range rs.Cols {
			rec[i] = fmt.Sprintf("%v", row[col])
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func printJSON(rs *tinysql.ResultSet) error {
	out := make([]map[string]any, len(rs.Rows))
	for i, row := range rs.Rows {
		m := make(map[string]any, len(rs.Cols))
		for _, col := range rs.Cols {
			m[col] = row[col]
		}
		out[i] = m
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Resolver helpers
// ─────────────────────────────────────────────────────────────────────────────

// buildResolver creates a path resolver closure that combines:
//  1. An explicit --mount path (treated as the "/" scope root)
//  2. A default --scope name
//  3. Named mounts registered in the manager
//  4. Direct filesystem path resolution as a fallback
func buildResolver(mgr *scope.Manager, mountPath, defaultScope string) func(string) (string, error) {
	return func(name string) (string, error) {
		// If a --mount flag was specified and the name is "/" or ".",
		// use the mount path as the root.
		if mountPath != "" && (name == "/" || name == "." || name == "") {
			return mountPath, nil
		}
		// If a --scope was specified and name matches "/" or the scope name
		if defaultScope != "" && (name == "/" || name == "." || name == defaultScope) {
			resolved, err := mgr.Resolve(defaultScope)
			if err == nil {
				return resolved, nil
			}
		}
		// Delegate to the scope manager (handles named mounts + direct paths)
		return mgr.Resolve(name)
	}
}

func printUsage(fs *flag.FlagSet) {
	fmt.Println(`fsql - File System Query Language

Usage:
  fsql [flags] mount <name> <path>      Register a named filesystem mount
  fsql [flags] umount <name>            Remove a named mount
  fsql [flags] mounts                   List registered mounts
  fsql [flags] query <sql>              Execute SQL query
  fsql [flags] index build <scope>      Build index for a scope (stub)
  fsql [flags] <sql>                    Shorthand query execution

Flags:`)
	fs.PrintDefaults()
	fmt.Println(`
Available table-valued functions:
  files(path [, recursive])      Filesystem metadata (path, name, size, ext, mod_time, is_dir)
  lines(file)                    Text file lines (line_number, line)
  csv_rows(file [, header])      CSV file rows
  json_rows(file [, path])       JSON file rows

Examples:
  fsql mount logs /var/log
  fsql query "SELECT path, size FROM files('logs', true) WHERE ext = 'log'"
  fsql --mount /var/log "SELECT count(*) FROM files('/', true)"
  fsql --output json query "SELECT * FROM csv_rows('/data/report.csv')"`)
}

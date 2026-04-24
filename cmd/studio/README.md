# tinySQL Studio (`studio`)

A native **desktop GUI** for tinySQL built with [Wails](https://wails.io/). It
provides a dark-themed native window where you can write and run SQL queries,
import CSV/JSON files, browse the schema, and export results — all backed by an
embedded in-memory tinySQL database.

## Prerequisites

- Go 1.25+
- [Wails v2 CLI](https://wails.io/docs/gettingstarted/installation):
  ```bash
  go install github.com/wailsapp/wails/v2/cmd/wails@latest
  ```
- Platform build dependencies for your OS (see the
  [Wails requirements](https://wails.io/docs/gettingstarted/installation#platform-specific-dependencies))

## Build

```bash
cd cmd/studio
wails build
```

The compiled binary is placed in `cmd/studio/build/bin/`.

## Development mode (hot-reload)

```bash
cd cmd/studio
wails dev
```

Opens the application with live-reload on every Go source change.

## Features

| Feature | Description |
|---------|-------------|
| SQL editor | Multi-line editor with keyboard shortcut execution |
| Result table | Paginated result grid with column widths |
| Schema browser | List tables and inspect column definitions |
| File import | Drag-and-drop or browse for CSV, TSV, JSON, and SQL files |
| Fuzzy import | Tolerant parsing for malformed CSV/JSON (auto-detects delimiter, infers types) |
| Database save/load | Persist the in-memory database to a `.gob` snapshot and reload it later |
| CSV export | Export any table to a CSV file via native save dialog |

## Keyboard shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` / `Cmd+Enter` | Execute the current query |
| `Ctrl+Shift+Enter` | Execute all statements |

## Architecture

The Go backend (`app.go`) uses the tinySQL `database/sql` driver for query
execution and the native tinySQL API (`tinysql.FuzzyImportCSV`,
`tinysql.FuzzyImportJSON`) for file imports. The Wails runtime bridges Go
methods to JavaScript, which drives a vanilla-JS frontend in `frontend/`.

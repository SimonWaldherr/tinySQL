# tinySQL Studio (`studio`)

Part of [tinySQL](../../README.md). See the root guide for engine capabilities,
imports, persistence, and limitations.

A native desktop GUI for tinySQL built with [Wails](https://wails.io/): write
and run SQL, import CSV/JSON files, browse the schema and export results,
backed by an embedded in-memory tinySQL database. Dark theme by default, with a
light theme toggle.

## Prerequisites

- Go 1.26.5+
- [Wails v2 CLI](https://wails.io/docs/gettingstarted/installation):
  ```bash
  go install github.com/wailsapp/wails/v2/cmd/wails@latest
  ```
- Platform build dependencies for your OS (see the
  [Wails requirements](https://wails.io/docs/gettingstarted/installation#platform-specific-dependencies))

## Build and develop

```bash
cd cmd/studio
wails build   # binary in cmd/studio/build/bin/
wails dev     # live-reload on every Go source change
```

## Features

| Feature | Description |
|---------|-------------|
| SQL editor | Multi-line editor with query tabs |
| Result table | Paginated result grid with column widths |
| Schema browser | List tables and inspect column definitions |
| File import | Drag-and-drop or browse for CSV, TSV, JSON, and SQL files |
| Fuzzy import | Tolerant parsing for malformed CSV/JSON (auto-detects delimiter, infers types) |
| Database save/load | Persist the in-memory database to a `.gob` snapshot and reload it |
| CSV export | Export any table to CSV via native save dialog |

## Keyboard shortcuts

| Shortcut | Action |
|----------|--------|
| `F5` | Execute the current query |
| `Ctrl+N` / `Cmd+N` | New query tab |
| `Ctrl+S` / `Cmd+S` | Save database |
| `Ctrl+W` / `Cmd+W` | Close current tab |
| `Ctrl+Tab` | Switch to next tab |

## Architecture

The Go backend (`app.go`) uses the tinySQL `database/sql` driver for query
execution and the native API (`tinysql.FuzzyImportCSV`,
`tinysql.FuzzyImportJSON`, `tinysql.SaveToFile`, `tinysql.LoadFromFile`) for
imports and snapshots. The Wails runtime bridges Go methods to JavaScript,
which drives a vanilla-JS frontend in `frontend/`.

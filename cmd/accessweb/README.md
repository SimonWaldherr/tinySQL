# AccessWeb

A minimalist, modern browser-based database manager for [tinySQL](https://github.com/SimonWaldherr/tinySQL) — think a lightweight Microsoft Access alternative that runs entirely in your terminal and browser with zero dependencies.

## Features

| Feature | Description |
|---|---|
| **Table Browser** | Sidebar lists all tables; click to open a paginated datasheet view |
| **Datasheet View** | View, sort, and page through table rows |
| **Record CRUD** | Add, edit, and delete records from any table with an `id INT` column |
| **Table Design** | Create new tables with a visual column designer (INT, FLOAT, TEXT, BOOL) |
| **Export** | Download whole tables or SQL query results as CSV or JSON |
| **Drop Table** | Delete any table with a one-click confirmation |
| **SQL Editor** | Run arbitrary SQL with an async JSON API; results are rendered inline |
| **File Persistence** | Optionally read/write a `.gob` file on disk |

## Quick Start

```bash
# File-backed by default (accessweb.db)
go run .

# Explicit in-memory mode (data lost on exit)
go run . -db :memory:

# Persist to a custom file
go run . -db mydata.db

# Custom port
go run . -addr :9090
```

Open your browser at **http://localhost:8080**.

## Command-line flags

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:8080` | HTTP listen address |
| `-db` | `accessweb.db` | Path to a `.gob` database file. Use `:memory:` or an empty value for in-memory mode. |
| `-tenant` | `default` | Tenant namespace within the database |

## Architecture

```
cmd/accessweb/
├── main.go          # Server setup, embed, flag parsing, template funcs
├── db.go            # App struct, table/record helpers, SQL execution
├── handlers.go      # HTTP route handlers
├── main_test.go     # HTTP integration tests
├── static/
│   └── app.js       # Minimal client-side helpers
└── templates/
    ├── base.html    # Layout: top nav + sidebar (shared across pages)
    ├── index.html   # Empty-state landing page
    ├── table_view.html   # Datasheet with pagination + sort
    ├── record_form.html  # Create/edit record form
    ├── query.html   # SQL editor with async JSON API
    └── create_table.html # Table design wizard
```

### HTTP Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Redirect to first table, or empty-state |
| `GET` | `/t/{table}` | Datasheet view (query params: `page`, `sort`, `dir`) |
| `GET` | `/t/{table}/export?format=csv\|json` | Download a full table export |
| `GET` | `/t/{table}/new` | New record form |
| `POST` | `/t/{table}/new` | Create record |
| `GET` | `/t/{table}/{id}/edit` | Edit record form |
| `POST` | `/t/{table}/{id}/edit` | Update record |
| `POST` | `/t/{table}/{id}/delete` | Delete record |
| `POST` | `/drop-table/{table}` | Drop table |
| `GET` | `/query` | SQL editor page |
| `POST` | `/api/query` | Execute SQL (JSON API) |
| `POST` | `/api/export` | Download SQL query results as CSV or JSON |
| `GET` | `/create-table` | Table designer |
| `POST` | `/create-table` | Create table |
| `GET/POST` | `/static/*` | Static assets |

### JSON API

**POST /api/query**

Request:
```json
{ "sql": "SELECT * FROM my_table LIMIT 5" }
```

Response (SELECT):
```json
{
  "columns": ["id", "name"],
  "rows": [["1", "Alice"]],
  "elapsed_ms": 2
}
```

Response (DML):
```json
{ "affected": 1, "elapsed_ms": 1 }
```

Response (error):
```json
{ "error": "table not found" }
```

**POST /api/export**

Request:
```json
{ "sql": "SELECT * FROM my_table", "format": "csv" }
```

The endpoint accepts result-producing SQL (`SELECT`, `WITH`, `SHOW`, `EXPLAIN`) and returns an attachment in `csv` or `json` format.

## Running Tests

```bash
go test ./...
```

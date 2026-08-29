# AccessWeb

Part of [tinySQL](../../README.md). See the root guide for engine capabilities,
storage choices, and current limitations.

Browser-based database manager for
[tinySQL](https://github.com/SimonWaldherr/tinySQL): a datasheet browser, record
editor, table designer, SQL editor and CSV/JSON export, with no dependencies
beyond the Go standard library.

Record CRUD requires the table to have an `id INT` column. The table designer
supports INT, FLOAT, TEXT and BOOL columns.

## Quick start

```bash
go run .                  # file-backed, accessweb.db
go run . -db :memory:     # in-memory, data lost on exit
go run . -db mydata.db
go run . -addr :9090
```

Then open http://localhost:8080.

## Flags

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:8080` | HTTP listen address |
| `-db` | `accessweb.db` | `.gob` database file; `:memory:` or empty for in-memory |
| `-tenant` | `default` | Tenant namespace within the database |

## Layout

```text
cmd/accessweb/
├── main.go          # server setup, embed, flags, template funcs
├── db.go            # App struct, table/record helpers, SQL execution
├── handlers.go      # HTTP route handlers
├── main_test.go     # HTTP integration tests
├── static/app.js    # client-side helpers
└── templates/
    ├── base.html         # layout: top nav + sidebar
    ├── index.html        # empty-state landing page
    ├── table_view.html   # datasheet with pagination + sort
    ├── record_form.html  # create/edit record form
    ├── query.html        # SQL editor
    └── create_table.html # table design wizard
```

## Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Redirect to first table, or empty state |
| `GET` | `/t/{table}` | Datasheet view (`page`, `sort`, `dir` query params) |
| `GET` | `/t/{table}/export?format=csv\|json` | Full table export |
| `GET`/`POST` | `/t/{table}/new` | New record form / create |
| `GET`/`POST` | `/t/{table}/{id}/edit` | Edit record form / update |
| `POST` | `/t/{table}/{id}/delete` | Delete record |
| `POST` | `/drop-table/{table}` | Drop table |
| `GET` | `/query` | SQL editor page |
| `POST` | `/query` | Execute SQL from the editor form |
| `POST` | `/api/query` | Execute SQL (JSON API) |
| `POST` | `/api/export` | Export SQL query results |
| `GET`/`POST` | `/create-table` | Table designer / create |
| `GET` | `/static/` | Static assets |

## JSON API

`POST /api/query` takes `{ "sql": "SELECT * FROM my_table LIMIT 5" }` and
answers with one of:

```json
{ "columns": ["id", "name"], "rows": [["1", "Alice"]], "elapsed_ms": 2 }
{ "affected": 1, "elapsed_ms": 1 }
{ "error": "table not found" }
```

`POST /api/export` takes `{ "sql": "SELECT * FROM my_table", "format": "csv" }`.
It accepts result-producing SQL (`SELECT`, `WITH`, `SHOW`, `EXPLAIN`) and
returns an attachment in `csv` or `json` format.

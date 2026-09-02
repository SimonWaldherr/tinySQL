# tinySQL Page Server (`tinysqlpage`)

Part of [tinySQL](../../README.md). See the root guide for supported SQL and
the [command index](../README.md) for other serving options.

An HTTP server that renders SQL-driven web pages. Each URL path maps to a `.sql`
file in a pages directory; result sets are converted into HTML components (hero
banner, stat cards, data table, text blocks) and served through a customisable
template.

## Build and run

```bash
go build -o tinysqlpage ./cmd/tinysqlpage

# bundled sample pages and seed data
./tinysqlpage

./tinysqlpage \
  -addr :8080 \
  -pages ./cmd/tinysqlpage/pages \
  -seed  ./cmd/tinysqlpage/sample_data.sql
```

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | HTTP listen address; pass `:8080` to accept connections from other machines | `127.0.0.1:8080` |
| `-pages` | Directory containing `.sql` page definitions | `cmd/tinysqlpage/pages` |
| `-seed` | SQL file executed at startup to populate demo data | `cmd/tinysqlpage/sample_data.sql` |
| `-css` | Custom CSS file (replaces the built-in dark theme) | — |
| `-template` | Custom HTML template file | — |
| `-request-timeout` | Maximum SQL rendering time per request; `0` disables it | `5s` |

## How it works

1. At startup, `-seed` SQL populates the in-memory database.
2. `GET /some/path` loads `<pages-dir>/some/path.sql`.
3. Each statement runs; result sets with a `component` column become HTML
   components.
4. Components are assembled and rendered through the HTML template.

Each request uses the caller's context and the configured timeout, so a
cancelled client connection or a slow query does not keep rendering work alive.

Navigation links are auto-generated from the `.sql` files in the pages
directory. Labels, ordering and page title come from SQL comment front-matter.

## Component types

| `component` value | Columns expected | Rendered as |
|-------------------|-----------------|-------------|
| `hero` | `title`, `subtitle` | Large centered heading |
| `text` | `content` | Paragraph block |
| `stat_list` | `title`, `label`/`name`, `value`, `info` | Stat card grid |
| `table` | `title` + data columns | Sortable data table |
| *(any other)* | — | Generic table |

## Example page (`pages/index.sql`)

```sql
-- nav_label: Home
-- nav_order: 0
-- title: Sales Dashboard
SELECT 'hero' AS component, 'My Dashboard' AS title, 'Powered by tinySQL' AS subtitle;

SELECT 'stat_list' AS component,
       'Overview' AS title,
       department AS label,
       COUNT(*) AS value
FROM employees
GROUP BY department;

SELECT 'table' AS component, 'Recent Orders' AS title,
       id, customer, amount, created_at
FROM orders
ORDER BY created_at DESC
LIMIT 10;
```

## Endpoints

| Path | Description |
|------|-------------|
| `/` | Renders `index.sql` |
| `/<page>` | Renders `<page>.sql` |
| `/healthz` | Liveness probe (`200 OK`) |

## Custom template

Pass `-template path/to/page.html`. The template is parsed with Go's
`html/template` and receives:

| Field | Type | Description |
|-------|------|-------------|
| `.Title` | `string` | Page title |
| `.Styles` | `template.CSS` | Inline CSS |
| `.Nav` | `template.HTML` | Navigation links |
| `.Body` | `template.HTML` | Rendered component HTML |

```html
<!DOCTYPE html>
<html>
<head><title>{{.Title}}</title><style>{{.Styles}}</style></head>
<body>
  <nav>{{.Nav}}</nav>
  <main>{{.Body}}</main>
</body>
</html>
```

Legacy `{{TITLE}}`, `{{STYLES}}`, `{{NAV}}` and `{{BODY}}` placeholders are
rewritten to the fields above before parsing.

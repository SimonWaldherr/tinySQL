# tinySQL Page Server (`tinysqlpage`)

An HTTP server that renders **SQL-driven web pages**. Each URL path maps to a
`.sql` file in a configurable pages directory; the queries produce structured
result sets that are automatically converted into HTML components (hero banner,
stat cards, data table, text blocks) and served through a customisable template.

## Build

```bash
go build -o tinysqlpage ./cmd/tinysqlpage
```

## Run

```bash
# Default — uses the bundled sample pages and seed data
./tinysqlpage

# Custom configuration
./tinysqlpage \
  -addr :8080 \
  -pages ./cmd/tinysqlpage/pages \
  -seed  ./cmd/tinysqlpage/sample_data.sql
```

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | HTTP listen address | `:8080` |
| `-pages` | Directory containing `.sql` page definitions | `cmd/tinysqlpage/pages` |
| `-seed` | SQL file executed at startup to populate demo data | `cmd/tinysqlpage/sample_data.sql` |
| `-css` | Path to a custom CSS file (replaces the built-in dark theme) | — |
| `-template` | Path to a custom HTML template file | — |

## How it works

1. At startup, `-seed` SQL is executed to populate the in-memory database.
2. Every HTTP `GET /some/path` loads `<pages-dir>/some/path.sql`.
3. Each SQL statement is executed; result sets with a `component` column are
   turned into HTML components.
4. Components are assembled and rendered through the HTML template.

Navigation links are auto-generated from the `.sql` files found in the pages
directory. You can control labels and ordering with SQL comment front-matter:

```sql
-- nav_label: Dashboard
-- nav_order: 1
-- title: Sales Dashboard
SELECT 'hero' AS component, 'Sales Dashboard' AS title, 'Live metrics' AS subtitle;
```

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
| `/healthz` | Liveness probe (returns `200 OK`) |

## Custom template

Pass `-template path/to/page.html`. The template uses Go's `html/template`
syntax with these fields:

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

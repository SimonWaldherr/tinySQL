package main

import (
    "context"
    "flag"
    "fmt"
    "html"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "strings"
    "time"

    tsql "github.com/SimonWaldherr/tinySQL"
)

const (
    defaultTenant = "tinysqlpage"
    defaultTitle  = "tinySQLPage"
)

func main() {
    defaultPages := filepath.Join("cmd", "tinysqlpage", "pages")
    defaultSeed := filepath.Join("cmd", "tinysqlpage", "sample_data.sql")

    addr := flag.String("addr", ":8080", "HTTP listen address")
    pagesDir := flag.String("pages", defaultPages, "Directory that holds SQL page definitions")
    seedFile := flag.String("seed", defaultSeed, "SQL file executed at startup to seed demo data")
    flag.Parse()

    db := tsql.NewDB()
    ctx := context.Background()

    if err := execSQLFile(ctx, db, defaultTenant, *seedFile); err != nil {
        log.Fatalf("seed database: %v", err)
    }

    handler := &pageHandler{
        db:       db,
        tenant:   defaultTenant,
        pagesDir: *pagesDir,
        ctx:      ctx,
    }

    mux := http.NewServeMux()
    mux.Handle("/", handler)

    log.Printf("tinysqlpage listening on %s (pages=%s)", *addr, *pagesDir)
    if err := http.ListenAndServe(*addr, mux); err != nil {
        log.Fatal(err)
    }
}

type pageHandler struct {
    db       *tsql.DB
    tenant   string
    pagesDir string
    ctx      context.Context
}

func (h *pageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    if r.URL.Path == "/healthz" {
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write([]byte("ok"))
        return
    }

    page := strings.Trim(r.URL.Path, "/")
    if page == "" {
        page = "index"
    }
    clean := filepath.Clean(page)
    if clean == "." {
        clean = "index"
    }
    if strings.HasPrefix(clean, "..") {
        http.NotFound(w, r)
        return
    }

    sqlPath := filepath.Join(h.pagesDir, clean+".sql")
    data, err := os.ReadFile(sqlPath)
    if err != nil {
        http.Error(w, "page not found", http.StatusNotFound)
        return
    }

    comps, err := h.renderComponents(string(data))
    if err != nil {
        log.Printf("render %s: %v", sqlPath, err)
        http.Error(w, "failed to render page", http.StatusInternalServerError)
        return
    }
    if len(comps) == 0 {
        http.Error(w, "page produced no components", http.StatusInternalServerError)
        return
    }

    title := deriveTitle(comps)
    htmlBody := renderShell(title, comps)

    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    _, _ = w.Write([]byte(htmlBody))
}

func (h *pageHandler) renderComponents(script string) ([]component, error) {
    statements := splitSQLStatements(script)
    var comps []component
    for _, stmtSQL := range statements {
        parsed, err := tsql.ParseSQL(stmtSQL)
        if err != nil {
            return nil, fmt.Errorf("parse statement: %w", err)
        }
        rs, err := tsql.Execute(h.ctx, h.db, h.tenant, parsed)
        if err != nil {
            return nil, fmt.Errorf("execute statement: %w", err)
        }
        if rs == nil || len(rs.Rows) == 0 {
            continue
        }
        built, err := componentsFromResult(rs)
        if err != nil {
            return nil, err
        }
        comps = append(comps, built...)
    }
    return comps, nil
}

func execSQLFile(ctx context.Context, db *tsql.DB, tenant, path string) error {
    data, err := os.ReadFile(path)
    if err != nil {
        return err
    }
    return execSQLScript(ctx, db, tenant, string(data))
}

func execSQLScript(ctx context.Context, db *tsql.DB, tenant, script string) error {
    statements := splitSQLStatements(script)
    for _, stmtSQL := range statements {
        parsed, err := tsql.ParseSQL(stmtSQL)
        if err != nil {
            return fmt.Errorf("parse statement: %w", err)
        }
        if _, err := tsql.Execute(ctx, db, tenant, parsed); err != nil {
            return fmt.Errorf("execute statement: %w", err)
        }
    }
    return nil
}

func splitSQLStatements(script string) []string {
    var stmts []string
    var buf strings.Builder
    inString := false

    for i := 0; i < len(script); i++ {
        ch := script[i]
        next := byte(0)
        if i+1 < len(script) {
            next = script[i+1]
        }

        if ch == '\'' {
            buf.WriteByte(ch)
            if inString && next == '\'' {
                buf.WriteByte(next)
                i++
            } else {
                inString = !inString
            }
            continue
        }

        if ch == ';' && !inString {
            stmt := strings.TrimSpace(buf.String())
            if stmt != "" {
                stmts = append(stmts, stmt)
            }
            buf.Reset()
            continue
        }

        buf.WriteByte(ch)
    }

    stmt := strings.TrimSpace(buf.String())
    if stmt != "" {
        stmts = append(stmts, stmt)
    }
    return stmts
}

type component interface {
    HTML() string
}

type heroComponent struct {
    Title    string
    Subtitle string
}

func (c heroComponent) HTML() string {
    var sb strings.Builder
    sb.WriteString(`<section class="component hero">`)
    sb.WriteString(`<h1>` + html.EscapeString(c.Title) + `</h1>`)
    if c.Subtitle != "" {
        sb.WriteString(`<p>` + html.EscapeString(c.Subtitle) + `</p>`)
    }
    sb.WriteString(`</section>`)
    return sb.String()
}

type textComponent struct {
    Content string
}

func (c textComponent) HTML() string {
    return `<section class="component text">` + html.EscapeString(c.Content) + `</section>`
}

type statItem struct {
    Label string
    Value string
    Info  string
}

type statListComponent struct {
    Title string
    Items []statItem
}

func (c statListComponent) HTML() string {
    var sb strings.Builder
    sb.WriteString(`<section class="component stats">`)
    if c.Title != "" {
        sb.WriteString(`<div class="section-title">` + html.EscapeString(c.Title) + `</div>`)
    }
    sb.WriteString(`<div class="stat-grid">`)
    for _, item := range c.Items {
        sb.WriteString(`<div class="stat">`)
        sb.WriteString(`<div class="stat-label">` + html.EscapeString(item.Label) + `</div>`)
        sb.WriteString(`<div class="stat-value">` + html.EscapeString(item.Value) + `</div>`)
        if item.Info != "" {
            sb.WriteString(`<div class="stat-info">` + html.EscapeString(item.Info) + `</div>`)
        }
        sb.WriteString(`</div>`)
    }
    sb.WriteString(`</div></section>`)
    return sb.String()
}

type tableComponent struct {
    Title   string
    Columns []string
    Rows    []map[string]string
}

func (c tableComponent) HTML() string {
    var sb strings.Builder
    sb.WriteString(`<section class="component card">`)
    if c.Title != "" {
        sb.WriteString(`<div class="section-title">` + html.EscapeString(c.Title) + `</div>`)
    }
    sb.WriteString(`<div class="table-wrapper"><table><thead><tr>`)
    for _, col := range c.Columns {
        sb.WriteString(`<th>` + html.EscapeString(col) + `</th>`)
    }
    sb.WriteString(`</tr></thead><tbody>`)
    for _, row := range c.Rows {
        sb.WriteString(`<tr>`)
        for _, col := range c.Columns {
            sb.WriteString(`<td>` + html.EscapeString(row[col]) + `</td>`)
        }
        sb.WriteString(`</tr>`)
    }
    sb.WriteString(`</tbody></table></div></section>`)
    return sb.String()
}

func componentsFromResult(rs *tsql.ResultSet) ([]component, error) {
    if len(rs.Rows) == 0 {
        return nil, nil
    }
    rawType, hasType := tsql.GetVal(rs.Rows[0], "component")
    if !hasType {
        return []component{genericTableFromResult(rs, "Query result")}, nil
    }
    compType := strings.ToLower(strings.TrimSpace(fmt.Sprint(rawType)))
    switch compType {
    case "hero":
        return []component{heroComponent{
            Title:    stringValue(rs.Rows[0], "title"),
            Subtitle: stringValue(rs.Rows[0], "subtitle"),
        }}, nil
    case "text":
        var comps []component
        for _, row := range rs.Rows {
            comps = append(comps, textComponent{Content: stringValue(row, "content")})
        }
        return comps, nil
    case "stat_list":
        title := stringValue(rs.Rows[0], "title")
        var items []statItem
        for _, row := range rs.Rows {
            items = append(items, statItem{
                Label: stringValue(row, "label"),
                Value: stringValue(row, "value"),
                Info:  stringValue(row, "info"),
            })
        }
        return []component{statListComponent{Title: title, Items: items}}, nil
    case "table":
        title := stringValue(rs.Rows[0], "title")
        return []component{buildTableComponent(rs, title)}, nil
    default:
        fallbackTitle := fmt.Sprintf("%s result", strings.ToUpper(compType))
        return []component{genericTableFromResult(rs, fallbackTitle)}, nil
    }
}

func buildTableComponent(rs *tsql.ResultSet, title string) component {
    meta := map[string]struct{}{
        "component": {},
        "title":     {},
        "subtitle":  {},
        "content":   {},
    }
    var cols []string
    for _, col := range rs.Cols {
        if _, skip := meta[strings.ToLower(col)]; skip {
            continue
        }
        cols = append(cols, col)
    }
    if len(cols) == 0 {
        cols = append(cols, rs.Cols...)
    }
    var rows []map[string]string
    for _, row := range rs.Rows {
        entry := make(map[string]string, len(cols))
        for _, col := range cols {
            entry[col] = stringValue(row, col)
        }
        rows = append(rows, entry)
    }
    return tableComponent{Title: title, Columns: cols, Rows: rows}
}

func genericTableFromResult(rs *tsql.ResultSet, title string) component {
    cols := append([]string(nil), rs.Cols...)
    rows := make([]map[string]string, 0, len(rs.Rows))
    for _, row := range rs.Rows {
        entry := make(map[string]string, len(cols))
        for _, col := range cols {
            entry[col] = stringValue(row, col)
        }
        rows = append(rows, entry)
    }
    return tableComponent{Title: title, Columns: cols, Rows: rows}
}

func stringValue(row tsql.Row, column string) string {
    v, ok := tsql.GetVal(row, column)
    if !ok {
        return ""
    }
    return formatValue(v)
}

func formatValue(v any) string {
    switch val := v.(type) {
    case nil:
        return ""
    case time.Time:
        return val.Format("2006-01-02 15:04")
    default:
        return fmt.Sprint(val)
    }
}

func deriveTitle(comps []component) string {
    for _, comp := range comps {
        if hero, ok := comp.(heroComponent); ok && hero.Title != "" {
            return hero.Title + " · " + defaultTitle
        }
    }
    return defaultTitle
}

func renderShell(title string, comps []component) string {
    var body strings.Builder
    for _, comp := range comps {
        body.WriteString(comp.HTML())
    }

    return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>%s</title>
<style>%s</style>
</head>
<body>
<header class="topbar">
  <div class="logo">tinySQLPage</div>
  <nav>
    <a href="/">Home</a>
    <a href="/customers">Customers</a>
  </nav>
</header>
<main class="container">%s</main>
</body>
</html>`, html.EscapeString(title), baseCSS, body.String())
}

const baseCSS = `:root {
  font-family: "Inter", "SF Pro Display", system-ui, -apple-system, sans-serif;
  color-scheme: light dark;
  --bg: #0f172a;
  --surface: rgba(15, 23, 42, 0.6);
  --border: rgba(248, 250, 252, 0.08);
  --text: #f8fafc;
  --muted: #cbd5f5;
  --accent: #38bdf8;
}
body {
  margin: 0;
  background: linear-gradient(135deg, #020617, #0f172a 60%, #1e293b);
  color: var(--text);
  min-height: 100vh;
}
.topbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem 4vw;
  border-bottom: 1px solid var(--border);
  backdrop-filter: blur(12px);
  position: sticky;
  top: 0;
  background: rgba(2, 6, 23, 0.9);
  z-index: 10;
}
.topbar a {
  color: var(--muted);
  text-decoration: none;
  margin-left: 1.5rem;
  font-weight: 500;
}
.topbar a:hover {
  color: var(--text);
}
.logo {
  font-weight: 700;
  letter-spacing: 0.08em;
}
.container {
  max-width: 1100px;
  margin: 0 auto;
  padding: 2rem 4vw 4rem;
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}
.component {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 18px;
  padding: 1.75rem;
  box-shadow: 0 25px 65px rgba(2, 6, 23, 0.35);
}
.hero {
  text-align: center;
  padding: 3rem 2rem;
}
.hero h1 {
  margin: 0;
  font-size: clamp(2.2rem, 5vw, 3.5rem);
}
.hero p {
  color: var(--muted);
  font-size: 1.1rem;
}
.text {
  font-size: 1.05rem;
  color: var(--muted);
}
.section-title {
  font-size: 1.1rem;
  font-weight: 600;
  margin-bottom: 0.75rem;
}
.stat-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 1rem;
}
.stat {
  background: rgba(56, 189, 248, 0.05);
  border-radius: 14px;
  padding: 1rem 1.25rem;
  border: 1px solid rgba(56, 189, 248, 0.25);
}
.stat-label {
  text-transform: uppercase;
  font-size: 0.75rem;
  letter-spacing: 0.12em;
  color: var(--muted);
}
.stat-value {
  font-size: 1.75rem;
  font-weight: 600;
  margin-top: 0.2rem;
}
.stat-info {
  font-size: 0.85rem;
  color: var(--muted);
  margin-top: 0.3rem;
}
.card .table-wrapper {
  overflow-x: auto;
}
table {
  width: 100%;
  border-collapse: collapse;
}
th, td {
  text-align: left;
  padding: 0.65rem 0.5rem;
  border-bottom: 1px solid rgba(248, 250, 252, 0.08);
}
th {
  text-transform: uppercase;
  font-size: 0.7rem;
  letter-spacing: 0.12em;
  color: var(--muted);
}
tr:last-child td {
  border-bottom: none;
}
`

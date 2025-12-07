package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"html"
	"html/template"
	"os"
	"strings"

	_ "github.com/SimonWaldherr/tinySQL/internal/driver"
)

var flagDSN = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")
var flagEcho = flag.Bool("echo", false, "Echo SQL statements before execution")
var flagFormat = flag.String("format", "table", "Output format: table, csv, tsv, json, yaml, markdown")
var flagBeautiful = flag.Bool("beautiful", false, "Pretty-print SQL blocks and results (group statements until next SELECT)")
var flagHTML = flag.Bool("html", false, "Emit a single HTML page showing the SQL blocks and results (useful when redirecting input)")
var flagErrorsOnly = flag.Bool("errors-only", false, "Only print queries/results that produce errors (ERR)")

func main() {
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open error:", err)
		return
	}
	defer db.Close()

	runREPL(db, *flagEcho, *flagFormat, *flagBeautiful, *flagHTML, *flagErrorsOnly)
}

func runREPL(db *sql.DB, echo bool, format string, beautiful bool, htmlMode bool, errorsOnly bool) {
	sc := bufio.NewScanner(os.Stdin)
	// Scanner token limit is 64K by default; allow larger statements/files.
	sc.Buffer(make([]byte, 1024), 4*1024*1024)

	var buf strings.Builder
	firstPrompt := true

	// If stdin is not a terminal (e.g., redirected from a file) suppress
	// interactive prompts like `sql>` to keep non-interactive output clean.
	interactive := false
	if fi, err := os.Stdin.Stat(); err == nil {
		interactive = (fi.Mode() & os.ModeCharDevice) != 0
	}

	// Keep HTML output clean: never print banners/prompts to stdout in htmlMode.
	if interactive && !htmlMode {
		fmt.Println("tinysql REPL (database/sql). Statement mit ';' beenden. '.help' für Hilfe.")
	}

	// srcLines accumulates all input lines since the last printed SELECT (when
	// running in beautiful mode). We keep comments and DDL/DML lines here and
	// print them as a block when a SELECT produces output.
	var srcLines []string

	// When htmlMode is enabled, collect rendered HTML fragments here and
	// emit a single HTML document at the end of the run.
	var htmlParts []string

	for {
		if buf.Len() == 0 {
			// Only print spacing/newlines and prompts in interactive mode.
			if interactive && !htmlMode {
				if !firstPrompt {
					fmt.Println()
				}
				firstPrompt = false
				fmt.Print("sql> ")
			}
		} else {
			if interactive && !htmlMode {
				fmt.Print(" ... ")
			}
		}

		if !sc.Scan() {
			// Input closed (or read error).
			if err := sc.Err(); err != nil {
				if htmlMode {
					htmlParts = append(htmlParts, "<div class='err'>ERR: "+html.EscapeString(err.Error())+"</div>")
				} else {
					fmt.Fprintln(os.Stderr, "read error:", err)
				}
			}

			if htmlMode {
				emitHTMLPage(htmlParts)
			}
			return
		}

		raw := sc.Text()
		line := strings.TrimSpace(raw)

		// Always collect source lines when beautiful mode is enabled so we can
		// print the full block (comments, creates, inserts, etc.) later.
		if beautiful {
			srcLines = append(srcLines, raw)
		}

		// Skip pure comment or empty lines for execution-building as before
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "/*") {
			continue
		}

		if buf.Len() == 0 && strings.HasPrefix(line, ".") {
			if handleMeta(db, line) {
				continue
			}
		}

		buf.WriteString(line)
		if strings.HasSuffix(line, ";") {
			q := strings.TrimSpace(buf.String())
			q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
			buf.Reset()

			if echo && !beautiful && !htmlMode {
				// In non-beautiful mode preserve existing echo behaviour. When
				// beautiful is enabled we print the accumulated block instead.
				fmt.Println("--", q)
			}

			up := strings.ToUpper(q)
			var sqlFrag string

			// Treat plain SELECT and WITH (CTE) statements as queries that return rows.
			if strings.HasPrefix(up, "SELECT") || strings.HasPrefix(up, "WITH") {
				rows, err := db.Query(q)
				if beautiful {
					// Prepare the accumulated SQL block (preserve whitespace)
					if htmlMode {
						sqlFrag = renderBeautifulBlockHTML(srcLines)
					} else {
						printBeautifulBlock(srcLines)
					}
				} else if htmlMode {
					// When not in beautiful mode but producing HTML, always include the executed SQL text.
					sqlFrag = renderSQLHTML(q)
				}

				if err != nil {
					friendly := friendlyErrorString(err)
					if htmlMode {
						if sqlFrag != "" {
							htmlParts = append(htmlParts, sqlFrag)
						} else {
							htmlParts = append(htmlParts, renderSQLHTML(q))
						}
						htmlParts = append(htmlParts, "<div class='err'>ERR: "+html.EscapeString(friendly)+"</div>")
						htmlParts = append(htmlParts, "<hr/>")
					} else {
						if errorsOnly {
							// When running with -errors-only, show the SQL that failed
							// together with the error to make debugging easier.
							fmt.Println("--", q)
							fmt.Println("ERR:", err)
						} else {
							fmt.Println("ERR:", err)
						}
					}
					// Drop the accumulated source so we don't repeatedly print failing statements.
					if beautiful {
						srcLines = nil
					}
					continue
				}

				cols, _ := rows.Columns()
				if htmlMode {
					out, err := rowsToSlice(rows, cols)
					rows.Close()

					// Only include successful results when not in errors-only mode.
					if err != nil {
						if sqlFrag != "" {
							htmlParts = append(htmlParts, sqlFrag)
						} else {
							htmlParts = append(htmlParts, renderSQLHTML(q))
						}
						friendly := friendlyErrorString(err)
						htmlParts = append(htmlParts, "<div class='err'>ERR: "+html.EscapeString(friendly)+"</div>")
						htmlParts = append(htmlParts, "<hr/>")
					} else {
						if !errorsOnly {
							if sqlFrag != "" {
								htmlParts = append(htmlParts, sqlFrag)
							} else {
								htmlParts = append(htmlParts, renderSQLHTML(q))
							}
							htmlParts = append(htmlParts, renderRowsHTML(out, cols))
							htmlParts = append(htmlParts, "<hr/>")
						}
					}
				} else {
					// Non-HTML output: only print rows when not in errors-only mode.
					if !errorsOnly {
						printRows(rows, cols, format)
						if beautiful {
							// Ensure at least one blank line separates SELECT outputs for readability
							fmt.Println()
						}
					}
					rows.Close()
				}

				if beautiful {
					// After handling results, reset accumulated source lines
					srcLines = nil
				}
				continue
			}

			// Non-SELECT statements.
			if _, err := db.Exec(q); err != nil {
				friendly := friendlyErrorString(err)
				if htmlMode {
					// Show the statement to make errors debuggable in the HTML output.
					if sqlFrag == "" {
						if beautiful {
							sqlFrag = renderBeautifulBlockHTML(srcLines)
						} else {
							sqlFrag = renderSQLHTML(q)
						}
					}
					htmlParts = append(htmlParts, sqlFrag)
					htmlParts = append(htmlParts, "<div class='err'>ERR: "+html.EscapeString(friendly)+"</div>")
				} else {
					if errorsOnly {
						fmt.Println("--", q)
						fmt.Println("ERR:", err)
					} else {
						fmt.Println("ERR:", err)
					}
				}

				// Drop the accumulated source so we don't repeatedly print failing statements.
				if beautiful {
					srcLines = nil
				}
				continue
			}

			if htmlMode {
				// Show the non-select statement as a small block in the HTML.
				if sqlFrag == "" {
					sqlFrag = renderSQLHTML(q)
				}
				// Only append OK blocks when not in errors-only mode.
				htmlParts = append(htmlParts, sqlFrag)
				if !errorsOnly {
					htmlParts = append(htmlParts, "<div class='ok'>(ok)</div>")
				}
			} else {
				// In non-interactive mode (e.g. redirected input) avoid printing a flood of "(ok)" lines.
				if interactive && !errorsOnly {
					fmt.Println("(ok)")
				}
			}
		} else {
			buf.WriteString(" ")
		}
	}
}

func handleMeta(db *sql.DB, line string) bool {
	switch {
	case line == ".help":
		fmt.Println(`
.meta:
  .help                 Hilfe
  .quit                 Beenden`)
		return true
	case line == ".quit":
		os.Exit(0)
	}
	return false
}

//nolint:gocyclo // REPL printer performs scanning, formatting, and alignment for display.
func printRows(rows *sql.Rows, cols []string, format string) {
	type rowMap = map[string]any
	var out []rowMap
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Println("ERR:", err)
			return
		}
		m := rowMap{}
		for i, c := range cols {
			m[c] = dePtr(ptrs[i])
		}
		out = append(out, m)
	}

	switch strings.ToLower(format) {
	case "json":
		printJSON(out)
	case "yaml":
		printYAML(out, cols)
	case "csv":
		printCSV(out, cols)
	case "tsv":
		printTSV(out, cols)
	case "markdown", "md":
		printMarkdown(out, cols)
	default:
		printTable(out, cols)
	}
}

func printTable(out []map[string]any, cols []string) {
	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
	}
	for _, r := range out {
		for i, c := range cols {
			if w := len(cell(r[c])); w > width[i] {
				width[i] = w
			}
		}
	}
	for i, c := range cols {
		fmt.Print(padRight(c, width[i]))
		if i < len(cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for i := range cols {
		fmt.Print(strings.Repeat("-", width[i]))
		if i < len(cols)-1 {
			fmt.Print("  ")
		}
	}
	fmt.Println()
	for _, r := range out {
		for i, c := range cols {
			fmt.Print(padRight(cell(r[c]), width[i]))
			if i < len(cols)-1 {
				fmt.Print("  ")
			}
		}
		fmt.Println()
	}
}

func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-len(s))
}

func dePtr(p any) any {
	switch v := p.(type) {
	case *any:
		return *v
	default:
		return v
	}
}

func cell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

func printJSON(out []map[string]any) {
	fmt.Println("[")
	for i, r := range out {
		fmt.Print("  {")
		j := 0
		for k, v := range r {
			if j > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("\"%s\": ", k)
			if v == nil {
				fmt.Print("null")
			} else if s, ok := v.(string); ok {
				fmt.Printf("\"%s\"", strings.ReplaceAll(s, "\"", "\\\""))
			} else {
				fmt.Printf("%v", v)
			}
			j++
		}
		fmt.Print("}")
		if i < len(out)-1 {
			fmt.Println(",")
		} else {
			fmt.Println()
		}
	}
	fmt.Println("]")

}

// rowsToSlice reads all rows into a slice of maps so callers can render them
// into different output formats (table, html, json, ...).
func rowsToSlice(rows *sql.Rows, cols []string) ([]map[string]any, error) {
	type rowMap = map[string]any
	var out []rowMap
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := rowMap{}
		for i, c := range cols {
			m[c] = dePtr(ptrs[i])
		}
		out = append(out, m)
	}
	return out, nil
}

// renderRowsHTML returns an HTML table for the given rows and columns.
func renderRowsHTML(out []map[string]any, cols []string) string {
	var b strings.Builder
	b.WriteString("<table class=\"results\">\n<thead>\n<tr>\n")
	for _, c := range cols {
		b.WriteString("<th>" + html.EscapeString(c) + "</th>")
	}
	b.WriteString("\n</tr>\n</thead>\n<tbody>\n")
	for _, r := range out {
		b.WriteString("<tr>")
		for _, c := range cols {
			v := r[c]
			s := "NULL"
			if v != nil {
				s = fmt.Sprintf("%v", v)
			}
			b.WriteString("<td>" + html.EscapeString(s) + "</td>")
		}
		b.WriteString("</tr>\n")
	}
	b.WriteString("</tbody>\n</table>\n")
	return b.String()
}

// renderBeautifulBlockHTML returns an HTML fragment containing the SQL block
// with preserved whitespace.
func isSeparatorLine(s string) bool {
	if len(s) < 3 {
		return false
	}
	for _, r := range s {
		switch r {
		case '=', '-', '_', '*':
			continue
		default:
			return false
		}
	}
	return true
}

func isAllCaps(s string) bool {
	hasLetter := false
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			return false
		}
		if r >= 'A' && r <= 'Z' {
			hasLetter = true
		}
	}
	return hasLetter
}

func looksLikeIdentifier(s string) bool {
	if s == "" || strings.Contains(s, " ") {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')) {
				return false
			}
		} else {
			if !(r == '_' || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
				return false
			}
		}
	}
	return true
}

// splitFuncHeading parses comment headings like:
//
//	-- ROW_NUMBER: Assign unique row numbers
func splitFuncHeading(text string) (name, desc string, ok bool) {
	parts := strings.SplitN(text, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	name = strings.TrimSpace(parts[0])
	desc = strings.TrimSpace(parts[1])
	if name == "" || desc == "" {
		return "", "", false
	}
	if name == "NEW" {
		return "", "", false
	}
	if !looksLikeIdentifier(name) {
		return "", "", false
	}
	// Heuristic: function headings in FUNCTIONS.sql are typically ALL CAPS identifiers.
	if strings.ToUpper(name) != name {
		return "", "", false
	}
	return name, desc, true
}

// renderBeautifulBlockHTML returns an HTML fragment containing any human-readable
// comment headings *outside* the collapsible SQL <pre>, then the SQL block itself.
func renderBeautifulBlockHTML(lines []string) string {
	var head strings.Builder
	var sql strings.Builder

	for _, raw := range lines {
		rawLine := strings.TrimRight(raw, "\r\n")
		trim := strings.TrimSpace(rawLine)

		if strings.HasPrefix(trim, "--") {
			text := strings.TrimSpace(strings.TrimPrefix(trim, "--"))
			if text == "" || isSeparatorLine(text) {
				continue
			}

			if name, desc, ok := splitFuncHeading(text); ok {
				head.WriteString("<h3 class=\"func\"><code>" + html.EscapeString(name) + "</code><span class=\"muted\"> — " + html.EscapeString(desc) + "</span></h3>\n")
				continue
			}
			if isAllCaps(text) {
				head.WriteString("<h2>" + html.EscapeString(text) + "</h2>\n")
				continue
			}
			head.WriteString("<div class=\"caption\">" + html.EscapeString(text) + "</div>\n")
			continue
		}

		if trim == "" {
			continue
		}
		sql.WriteString(html.EscapeString(rawLine))
		sql.WriteString("\n")
	}

	var b strings.Builder
	if head.Len() > 0 {
		b.WriteString("<div class=\"doc-head\">\n")
		b.WriteString(head.String())
		b.WriteString("</div>\n")
	}
	b.WriteString("<div class=\"sql-block\"><pre>")
	b.WriteString(sql.String())
	b.WriteString("</pre></div>\n")
	return b.String()
}

// renderSQLHTML creates a compact HTML fragment containing the SQL text.
func renderSQLHTML(q string) string {
	return "<div class=\"sql-block\"><pre>" + html.EscapeString(q) + "</pre></div>"
}

// friendlyErrorString maps common demo errors to clearer, user-friendly
// messages so the generated HTML page explains why examples failed.
func friendlyErrorString(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "file(): open"):
		return "FILE() failed: missing file or path. This example uses a placeholder path; create the file or adjust the path for your environment."
	case strings.Contains(s, "http(): Get"):
		return "HTTP() failed: network or DNS lookup failed. External HTTP examples require network access; disable or replace with a local file."
	case strings.Contains(s, "table-valued function") && strings.Contains(s, "used as scalar"):
		return "table-valued function used as scalar; use in FROM clause (parser support pending)"
	case strings.Contains(s, "no such table \"TABLE_FROM_"):
		return "table-valued function not available in FROM; parser support pending"
	case strings.Contains(s, "no such table \"catalog."):
		return "system catalog SQL queries are not yet supported; catalog is accessible via the Go API"
	// Previously this repo used to special-case JOB parse errors with a
	// friendly message. SQL-level JOB support has been implemented, so
	// remove that special-case and fall through to the default error
	// message which preserves the original parser text.
	default:
		return s
	}
}

type htmlPageData struct {
	Title string
	Lead  string
	Parts []template.HTML
}

const htmlPageTemplate = `<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>{{.Title}}</title>
	<style>
		:root{
			--bg:#f3f4f6;--card:#ffffff;--text:#0f172a;--muted:#6b7280;--accent:#0b69ff;
			--border:#e6eef8;--codebg:#f8fafc;
		}
		@media (prefers-color-scheme: dark){
			:root{--bg:#0b0f19;--card:#0f172a;--text:#e5e7eb;--muted:#94a3b8;--accent:#60a5fa;--border:#1f2a44;--codebg:#0b1220;}
		}
		body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--text);padding:28px}
		.container{max-width:1100px;margin:0 auto}
		.topbar{position:sticky;top:14px;z-index:5;background:color-mix(in srgb, var(--bg) 70%, transparent);backdrop-filter:blur(10px);
			border:1px solid var(--border);border-radius:12px;padding:12px 14px;display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin-bottom:16px}
		h1{font-size:20px;margin:0}
		p.lead{margin:4px 0 0;color:var(--muted);font-size:13px}
		.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
		.btn{display:inline-flex;align-items:center;gap:8px;padding:7px 10px;background:var(--card);border:1px solid var(--border);
			border-radius:10px;font-size:13px;cursor:pointer;color:var(--accent)}
		.btn:hover{filter:brightness(0.98)}
		.btn:active{transform:translateY(1px)}
		hr{border:none;border-top:1px solid var(--border);margin:18px 0}

		.doc-head{margin:18px 2px 8px}
		.doc-head h2{font-size:16px;margin:18px 0 6px}
		.doc-head h3{font-size:14px;margin:12px 0 6px}
		.doc-head .caption{color:var(--muted);font-size:13px;margin:8px 0 4px}
		.doc-head code{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,monospace;font-size:12px;background:var(--codebg);
			border:1px solid var(--border);padding:2px 6px;border-radius:8px}
		.doc-head .muted{color:var(--muted);font-weight:500;margin-left:6px}

		.sql-block{background:var(--card);border:1px solid var(--border);padding:12px;margin:12px 0;border-radius:12px;position:relative;
			box-shadow:0 1px 2px rgba(15,23,42,0.06)}
		.sql-block .controls{position:absolute;top:10px;right:10px;display:flex;gap:6px;z-index:2}
		.sql-code{margin-top:8px}
		.sql-textarea{width:98%;min-height:70px;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,monospace;font-size:13px;line-height:1.45;
			background:linear-gradient(180deg,var(--codebg),color-mix(in srgb, var(--codebg) 70%, transparent));padding:10px;border-radius:10px;border:1px solid var(--border);resize:vertical}
		.static-result{margin-top:12px;opacity:0.6;position:relative}
		.static-result::before{content:'Generated Result';display:block;font-size:11px;color:var(--muted);margin-bottom:6px;font-weight:600}
		.inline-result{margin-top:12px;border-radius:10px;background:var(--card);position:relative}
		.inline-result::before{content:'▶ Live Result';display:block;font-size:11px;color:var(--accent);margin-bottom:8px;font-weight:700}
		.btn[disabled]{opacity:0.5;cursor:not-allowed}
		.results-wrap{overflow:auto;border-radius:12px;border:1px solid var(--border);margin:12px 0}
		table.results{border-collapse:separate;border-spacing:0;width:100%;background:transparent}
		table.results th,table.results td{border-bottom:1px solid var(--border);padding:10px;text-align:left;font-size:13px;background:var(--card)}
		table.results th{background:var(--codebg);color:var(--muted);font-weight:700}
		table.results tr:last-child td{border-bottom:none}

		.err{color:#b31d28;font-weight:700;margin:8px 0}
		.ok{color:#15803d;font-weight:700;margin:8px 0;font-size:13px}

		.toast{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);padding:10px 12px;border-radius:12px;
			background:rgba(15,23,42,0.90);color:#fff;font-size:13px;opacity:0;pointer-events:none;transition:opacity .12s ease,transform .12s ease}
		.toast.show{opacity:1;transform:translateX(-50%) translateY(-6px)}
	</style>
</head>
<body>
<div class="container">
	<div class="topbar">
		<div>
			<h1>{{.Title}}</h1>
			<p class="lead">{{.Lead}}</p>
		</div>
		<div class="actions">
			<button id="init-wasm" class="btn" type="button" onclick="initWasm()">Init WASM</button>
		</div>
	</div>

		<!--<script src="tinysql-runner.js" defer></script>-->
		<script>
		// Copy SQL helper: works with <pre> or <textarea> inside the sql-block.
		function copySQL(id){
			const e=document.getElementById(id);
			if(!e) return;
			const pre=e.querySelector('pre');
			const ta=e.querySelector('textarea');
			const t = ta ? ta.value : (pre ? pre.innerText : '');
			if(!t) return;
			if(navigator.clipboard && navigator.clipboard.writeText){
				navigator.clipboard.writeText(t).then(()=>toast('Copied SQL'), ()=>fallbackCopy(t));
			} else {
				fallbackCopy(t);
			}
		}
		function fallbackCopy(t){
			const ta=document.createElement('textarea');
			ta.value=t; document.body.appendChild(ta);
			ta.select();
			try{ document.execCommand('copy'); toast('Copied SQL'); }catch(e){}
			document.body.removeChild(ta);
		}
		let toastTimer;
		function toast(msg){
			const el=document.getElementById('toast');
			if(!el) return;
			el.textContent=msg;
			el.classList.add('show');
			clearTimeout(toastTimer);
			toastTimer=setTimeout(()=>el.classList.remove('show'), 1200);
		}

		// Simple HTML escape helper used when rendering inline results.
		function escapeHtml(s){
			return String(s).replace(/[&<>\"]/g, function(c){
				switch(c){case '&': return '&amp;'; case '<': return '&lt;'; case '>': return '&gt;'; case '"': return '&quot;';}
				return c;
			});
		}

		// Run SQL from a sql-block using available WASM APIs (if any).
		function checkWasm(){
			return (typeof executeQuery_wasm === 'function') || (window.wasmApi && typeof window.wasmApi.executeQuery === 'function');
		}

		async function initWasm(){
			// If the page already exposes a wasmApi initializer, call it.
			if(window.wasmApi && typeof window.wasmApi.init === 'function'){
				try{ await window.wasmApi.init(); toast('WASM initialized'); updateRunButtons(); return; }catch(e){ toast('WASM init failed: '+(e && e.message?e.message:String(e))); }
			}

			// Otherwise try to dynamically load the frontend script that usually
			// registers window.wasmApi (try several common locations).
			function loadScript(url){
				return new Promise((resolve,reject)=>{
					try{
						// If a script with same src already exists, don't load it again.
						if(document.querySelector('script[src="'+url+'"]')) return resolve(url);
					}catch(e){ /* ignore DOM errors */ }
					const s=document.createElement('script'); s.src=url; s.async=true;
					s.onload = ()=>resolve(url);
					s.onerror = ()=>reject(new Error('failed to load '+url));
					document.head.appendChild(s);
				});
			}

			// Load required scripts: wasm_exec.js (Go runtime), then wasm-init.js (minimal loader)
			const candidates = ['wasm_exec.js', 'wasm-init.js'];
			for(const url of candidates){
				try{
					await loadScript(url);
					// After loading wasm-init.js, call window.initWasm()
					if(url.includes('wasm-init.js')){
						// Wait for window.initWasm to be defined
						for(let i=0; i<50 && typeof window.initWasm !== 'function'; i++){
							await new Promise(r=>setTimeout(r,100));
						}
						if(typeof window.initWasm === 'function'){
							try{
								await window.initWasm();
								toast('WASM initialized');
								updateRunButtons();
								return;
							}catch(e){
								console.error('initWasm failed:', e);
								toast('WASM init failed: '+(e && e.message?e.message:String(e)));
							}
						} else {
							console.warn('wasm-init.js loaded but window.initWasm not found');
						}
					}
				} catch(e) {
					console.warn('Failed to load '+url+':', e);
				}
			}

			toast('WASM init not available');
		}

		function updateRunButtons(){
			const ok = checkWasm();
			document.querySelectorAll('.btn-run').forEach(b=>{ b.disabled = !ok; if(!ok) b.title = 'WASM unavailable — click Init WASM to try'; else b.title='Run this query'; });
			const initBtn = document.getElementById('init-wasm'); if(initBtn) initBtn.disabled = ok;
		}

		document.addEventListener('DOMContentLoaded', async ()=>{ 
			updateRunButtons(); 
			// Auto-initialize WASM on page load
			await initWasm();
		});

		async function runBlockQuery(id){
				const block = document.getElementById(id);
				if(!block) return;
			const ta = block.querySelector('textarea');
				const sql = ta ? ta.value.trim() : '';
				if(!sql) { toast('No SQL found'); return; }

			// If WASM not available show helpful guidance
			if(!checkWasm()){
				toast('WASM execute function not available — click Init WASM');
				const prev = block.querySelector('.inline-result');
				if(prev) prev.remove();
				const msg = document.createElement('div'); msg.className='inline-result'; msg.innerHTML = '<div class="err">WASM execute function not available. Click "Init WASM" to attempt initialization or serve the demo assets (app.js + query_files.wasm).</div>';
				block.appendChild(msg);
				return;
			}

			// Remove static result and any previous inline result
				const staticResult = block.querySelector('.static-result');
				if(staticResult) staticResult.remove();
				const prev = block.querySelector('.inline-result');
				if(prev) prev.remove();
				const resultWrap = document.createElement('div');
				resultWrap.className = 'inline-result';
			resultWrap.setAttribute('aria-live','polite');
				resultWrap.innerHTML = '<div class="ok">Running...</div>';
				block.appendChild(resultWrap);
				try{
					// Call executeQuery_wasm (provided by wasm-init.js)
					if(typeof executeQuery_wasm !== 'function'){
						resultWrap.innerHTML = '<div class="err">WASM execute function not available</div>';
						return;
					}
					let res = executeQuery_wasm(sql);
					// Support Promise-based results (though executeQuery_wasm returns sync)
					if(res && typeof res.then === 'function'){
						res = await res;
					}
					if(!res){
						resultWrap.innerHTML = '<div class="err">No result returned</div>';
						return;
					}
					if(res.success){
						const cols = Array.isArray(res.columns) ? res.columns : [];
						const rows = Array.isArray(res.rows) ? res.rows : [];
						let html = '<div class="results-wrap"><table class="results"><thead><tr>';
						for(const c of cols) html += '<th>'+escapeHtml(String(c))+'</th>';
						html += '</tr></thead><tbody>';
						for(const r of rows){ html += '<tr>'; for(const c of cols){ const v = r[c]; html += '<td>'+ (v==null? 'NULL' : escapeHtml(String(v))) +'</td>'; } html += '</tr>'; }
						html += '</tbody></table></div>';
						resultWrap.innerHTML = html;
					}else{
						resultWrap.innerHTML = '<div class="err">'+escapeHtml(String(res.error || 'Query failed'))+'</div>';
					}
				} catch(err){
					resultWrap.innerHTML = '<div class="err">'+escapeHtml(err && err.message ? err.message : String(err))+'</div>';
				}
			}
		</script>

		{{range .Parts}}
	{{.}}
	{{end}}
</div>
<div id="toast" class="toast" aria-live="polite"></div>
</body>
</html>
`

var htmlPageTmpl = template.Must(template.New("page").Parse(htmlPageTemplate))

func decorateHTMLFragment(p string, i int, next string) string {
	// Wrap result tables for horizontal scrolling on small screens.
	if strings.Contains(p, "<table class=\"results\">") {
		p = strings.Replace(p, "<table class=\"results\">", "<div class=\"results-wrap\"><table class=\"results\">", 1)
		p = strings.Replace(p, "</table>", "</table></div>", 1)
	}

	if !strings.Contains(p, "class=\"sql-block\"") {
		// If caller provided a next fragment but this is not a sql-block,
		// just append next after p so non-sql fragments are preserved.
		if next != "" {
			return p + next
		}
		return p
	}

	id := fmt.Sprintf("sql-%d", i)

	// Add id; do not collapse. Replace <pre> with a readonly textarea to
	// better preserve formatting and allow easy copying/selection.
	p = strings.Replace(p, "class=\"sql-block\"", fmt.Sprintf("class=\"sql-block\" id=\"%s\"", id), 1)

	// Add copy control (no toggle) and replace the pre block with textarea
	// while preserving the contained HTML-escaped text.
	controls := fmt.Sprintf("<div class=\"controls\"><button type=\"button\" class=\"btn\" onclick=\"copySQL('%s')\">Copy</button><button type=\"button\" class=\"btn btn-run\" onclick=\"runBlockQuery('%s')\">Run</button></div>", id, id)
	// Replace only the first <pre> occurrence.
	if strings.Contains(p, "<pre>") {
		// Make the textarea editable so users can tweak and re-run example queries.
		p = strings.Replace(p, "<pre>", controls+"<div class=\"sql-code\"><textarea class=\"sql-textarea\">", 1)
		p = strings.Replace(p, "</pre>", "</textarea></div>", 1)
	} else {
		// Fallback: just inject controls
		p = strings.Replace(p, "class=\"sql-block\"", fmt.Sprintf("class=\"sql-block\" id=\"%s\"", id), 1)
		p = strings.Replace(p, "</div>\n", controls+"</div>\n", 1)
	}

	// If caller provided a next fragment (results/error/ok), wrap it with
	// static-result class and insert before the final closing `</div>`
	if next != "" {
		// Wrap static results to distinguish from dynamic inline results
		next = "<div class=\"static-result\">" + next + "</div>"
		// Find the last closing div of this fragment and insert next before it.
		li := strings.LastIndex(p, "</div>")
		if li != -1 {
			p = p[:li] + next + p[li:]
		} else {
			p = p + next
		}
	}

	return p
}

// emitHTMLPage writes a single HTML document to stdout using the collected fragments.
func emitHTMLPage(parts []string) {
	data := htmlPageData{
		Title: "tinySQL - Function Examples",
		Lead:  "Auto-generated from input SQL. Results are shown below; SQL is visible above each result.",
		Parts: make([]template.HTML, 0, len(parts)),
	}

	for i := 0; i < len(parts); i++ {
		p := parts[i]
		var next string
		// If the next fragment looks like a result (table, err, or ok), merge it.
		if i+1 < len(parts) {
			nxt := parts[i+1]
			if strings.Contains(nxt, "class=\"results-wrap\"") || strings.Contains(nxt, "<table class=\"results\"") || strings.Contains(nxt, "class=\"err\"") || strings.Contains(nxt, "class=\"ok\"") {
				next = nxt
				i++ // skip the merged fragment
			}
		}
		// Inject show/copy controls into SQL blocks and optionally merge next.
		p = decorateHTMLFragment(p, i, next)
		data.Parts = append(data.Parts, template.HTML(p))
	}

	if err := htmlPageTmpl.Execute(os.Stdout, data); err != nil {
		fmt.Fprintln(os.Stderr, "template error:", err)
	}
}

func printYAML(out []map[string]any, cols []string) {
	for i, r := range out {
		fmt.Printf("- ")
		for j, c := range cols {
			if j > 0 {
				fmt.Print("  ")
			}
			v := r[c]
			if v == nil {
				fmt.Printf("%s: null", c)
			} else if s, ok := v.(string); ok {
				fmt.Printf("%s: \"%s\"", c, s)
			} else {
				fmt.Printf("%s: %v", c, v)
			}
			if j < len(cols)-1 {
				fmt.Println()
			}
		}
		if i < len(out)-1 {
			fmt.Println()
		}
		fmt.Println()
	}
}

func printCSV(out []map[string]any, cols []string) {
	// Header
	for i, c := range cols {
		if i > 0 {
			fmt.Print(",")
		}
		if strings.ContainsAny(c, ",\"\n") {
			fmt.Printf("\"%s\"", strings.ReplaceAll(c, "\"", "\"\""))
		} else {
			fmt.Print(c)
		}
	}
	fmt.Println()

	// Rows
	for _, r := range out {
		for i, c := range cols {
			if i > 0 {
				fmt.Print(",")
			}
			v := r[c]
			s := cell(v)
			if strings.ContainsAny(s, ",\"\n") {
				fmt.Printf("\"%s\"", strings.ReplaceAll(s, "\"", "\"\""))
			} else {
				fmt.Print(s)
			}
		}
		fmt.Println()
	}
}

func printTSV(out []map[string]any, cols []string) {
	// Header
	for i, c := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(c)
	}
	fmt.Println()

	// Rows
	for _, r := range out {
		for i, c := range cols {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(cell(r[c]))
		}
		fmt.Println()
	}
}

func printMarkdown(out []map[string]any, cols []string) {
	// Calculate widths
	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
	}
	for _, r := range out {
		for i, c := range cols {
			if w := len(cell(r[c])); w > width[i] {
				width[i] = w
			}
		}
	}

	// Header
	fmt.Print("|")
	for i, c := range cols {
		fmt.Print(" ")
		fmt.Print(padRight(c, width[i]))
		fmt.Print(" |")
	}
	fmt.Println()

	// Separator
	fmt.Print("|")
	for i := range cols {
		fmt.Print(strings.Repeat("-", width[i]+2))
		fmt.Print("|")
	}
	fmt.Println()

	// Rows
	for _, r := range out {
		fmt.Print("|")
		for i, c := range cols {
			fmt.Print(" ")
			fmt.Print(padRight(cell(r[c]), width[i]))
			fmt.Print(" |")
		}
		fmt.Println()
	}
}

// printBeautifulBlock prints collected source lines (comments and statements)
// in a readable multi-line form, suppressing empty lines.
func printBeautifulBlock(lines []string) {
	if len(lines) == 0 {
		return
	}
	fmt.Println("---- SQL ----")
	for _, raw := range lines {
		// Preserve leading indentation and internal spacing; only trim
		// trailing newlines so the original formatting from the SQL file
		// is retained when printing.
		t := strings.TrimRight(raw, "\r\n")
		fmt.Println(t)
	}
	fmt.Println("--------------")
}

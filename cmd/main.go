package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/internal/driver"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var flagDemo = flag.Bool("demo", false, "run built-in demo instead of REPL")
var flagWeb = flag.Bool("web", false, "start web interface on :8080")
var flagDSN = flag.String("dsn", "mem://?tenant=default", "DSN (mem:// or file:/path.db?tenant=...&autosave=1)")

func main() {
	flag.Parse()

	db, err := sql.Open("tinysql", *flagDSN)
	if err != nil {
		fmt.Println("open error:", err)
		return
	}
	defer db.Close()

	if *flagDemo {
		runDemo(db)
		return
	}
	if *flagWeb {
		runWeb()
		return
	}
	runREPL(db)
}

func runDemo(db *sql.DB) {
	exec := func(q string, args ...any) {
		fmt.Println("SQL>", q)
		up := strings.ToUpper(strings.TrimSpace(q))
		if strings.HasPrefix(up, "SELECT") {
			rows, err := db.Query(q, args...)
			if err != nil {
				fmt.Println("ERR:", err)
				return
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			printRows(rows, cols)
		} else {
			if _, err := db.Exec(q, args...); err != nil {
				fmt.Println("ERR:", err)
			} else {
				fmt.Println("(ok)")
			}
		}
		fmt.Println()
	}

	// Schema
	exec(`CREATE TABLE users (id INT, name TEXT, email TEXT, active BOOL)`)
	exec(`CREATE TABLE orders (id INT, user_id INT, amount FLOAT, status TEXT, meta JSON)`)

	// Inserts
	exec(`INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', TRUE)`)
	exec(`INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', NULL, TRUE)`)
	exec(`INSERT INTO users (id, name, email, active) VALUES (3, 'Carol', 'carol@example.com', NULL)`)

	exec(`INSERT INTO orders VALUES (101, 1, 100.5, 'PAID', '{"device":"web","items":[{"sku":"A","qty":1}]}' )`)
	exec(`INSERT INTO orders VALUES (102, 1,  75.0, 'PAID', '{"device":"app","items":[{"sku":"B","qty":2}]}' )`)
	exec(`INSERT INTO orders VALUES (103, 2, 200.0, 'PAID', '{"device":"web"}' )`)
	exec(`INSERT INTO orders VALUES (104, 2,  20.0, 'CANCELED', NULL )`)

	// DISTINCT
	exec(`SELECT DISTINCT active FROM users ORDER BY active ASC`)

	// JSON
	exec(`SELECT id, JSON_GET(meta, 'device') AS device FROM orders ORDER BY id`)

	// JOIN + GROUP BY
	exec(`
		SELECT u.name AS user, SUM(o.amount) AS total, COUNT(*) AS cnt
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'PAID'
		GROUP BY u.name
		ORDER BY total DESC
	`)

	// RIGHT JOIN
	exec(`
		SELECT o.id AS order_id, u.name AS user
		FROM users u
		RIGHT OUTER JOIN orders o ON u.id = o.user_id
		ORDER BY order_id
	`)

	// Temp table
	exec(`
		CREATE TEMP TABLE big_spenders AS
		SELECT u.id AS user_id, u.name, SUM(o.amount) AS total
		FROM users u
		JOIN orders o ON u.id = o.user_id
		WHERE o.status = 'PAID'
		GROUP BY u.id, u.name
		HAVING SUM(o.amount) >= 150
	`)
	exec(`SELECT * FROM big_spenders ORDER BY total DESC`)

	// UPDATE/DELETE
	exec(`UPDATE users SET email = 'alice@new.example', active = TRUE WHERE name = 'Alice'`)
	exec(`DELETE FROM users WHERE active = FALSE`)
	exec(`SELECT * FROM users ORDER BY id`)
}

func runREPL(db *sql.DB) {
	fmt.Println("tinysql REPL (database/sql). Statement mit ';' beenden. '.help' für Hilfe.")
	sc := bufio.NewScanner(os.Stdin)
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print(" ... ")
		}
		if !sc.Scan() {
			fmt.Println()
			return
		}
		line := strings.TrimSpace(sc.Text())
		if buf.Len() == 0 && strings.HasPrefix(line, ".") {
			if handleMeta(db, line) {
				continue
			}
		}
		buf.WriteString(line)
		if strings.HasSuffix(line, ";") {
			q := strings.TrimSpace(strings.TrimSuffix(buf.String(), ";"))
			buf.Reset()
			if q == "" {
				continue
			}
			up := strings.ToUpper(q)
			if strings.HasPrefix(up, "SELECT") {
				rows, err := db.Query(q)
				if err != nil {
					fmt.Println("ERR:", err)
					continue
				}
				defer rows.Close()
				cols, _ := rows.Columns()
				printRows(rows, cols)
			} else {
				if _, err := db.Exec(q); err != nil {
					fmt.Println("ERR:", err)
				} else {
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

func printRows(rows *sql.Rows, cols []string) {
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
	width := make([]int, len(cols))
	for i, c := range cols {
		width[i] = len(c)
	}
	cell := func(v any) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", v)
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

// Web interface globals
var (
	webDB    *storage.DB
	webCache *engine.QueryCache
)

type QueryResult struct {
	SQL      string                   `json:"sql"`
	Columns  []string                 `json:"columns"`
	Rows     []map[string]interface{} `json:"rows"`
	Error    string                   `json:"error,omitempty"`
	Duration string                   `json:"duration"`
	Count    int                      `json:"count"`
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

func runWeb() {
	// Initialize database and query cache
	webDB = storage.NewDB()
	webCache = engine.NewQueryCache(100)

	// Set up demo data
	setupWebDemoData()

	// Web routes
	http.HandleFunc("/", handleWebIndex)
	http.HandleFunc("/api/query", handleWebQuery)
	http.HandleFunc("/api/schema", handleWebSchema)

	fmt.Println("TinySQL Web Interface starting...")
	fmt.Println("Database initialized with demo data")
	fmt.Println("Web interface available at: http://localhost:8080")

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func setupWebDemoData() {
	ctx := context.Background()
	tenant := "demo"

	setupQueries := []string{
		// Create tables with new data types
		`CREATE TABLE departments (
			id INT PRIMARY KEY,
			name TEXT UNIQUE,
			budget FLOAT,
			created_date DATE
		)`,

		`CREATE TABLE employees (
			id INT PRIMARY KEY,
			name TEXT,
			birth_date DATE,
			hire_datetime DATETIME,
			work_duration DURATION,
			location_coords COMPLEX,
			profile JSON,
			manager_id POINTER,
			dept_id INT
		)`,

		// Insert data
		"INSERT INTO departments VALUES (1, 'Engineering', 750000.0, '2020-01-15')",
		"INSERT INTO departments VALUES (2, 'Marketing', 350000.0, '2020-02-01')",
		"INSERT INTO departments VALUES (3, 'HR', 200000.0, '2020-01-30')",

		"INSERT INTO employees VALUES (1, 'Alice Johnson', '1990-05-15', '2022-01-10 09:00:00', '08:30:00', '3.14+2.71i', '{\"role\": \"Senior Developer\", \"skills\": [\"Go\", \"SQL\"]}', NULL, 1)",
		"INSERT INTO employees VALUES (2, 'Bob Smith', '1985-12-01', '2021-03-15 09:00:00', '08:00:00', '1.41+1.73i', '{\"role\": \"Manager\", \"team_size\": 5}', 1, 1)",
		"INSERT INTO employees VALUES (3, 'Carol Davis', '1992-08-20', '2022-06-01 09:30:00', '07:45:00', '2.00+3.00i', '{\"role\": \"Designer\", \"tools\": [\"Figma\"]}', NULL, 2)",
	}

	for _, sql := range setupQueries {
		executeWebQuery(ctx, tenant, sql)
	}
}

func executeWebQuery(ctx context.Context, tenant, sql string) error {
	parser := engine.NewParser(sql)
	stmt, err := parser.ParseStatement()
	if err != nil {
		return err
	}

	_, err = engine.Execute(ctx, webDB, tenant, stmt)
	return err
}

func handleWebIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := `<!DOCTYPE html>
<html>
<head>
    <title>TinySQL Web Interface</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #333; color: white; padding: 20px; border-radius: 8px; text-align: center; margin-bottom: 20px; }
        .query-section { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        textarea { width: 100%; height: 100px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-family: monospace; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
        button:hover { background: #0056b3; }
        .examples { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 10px; margin-top: 15px; }
        .example-btn { background: #f8f9fa; color: #495057; text-align: left; padding: 12px; border: 1px solid #dee2e6; }
        .results { background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .results-header { background: #343a40; color: white; padding: 15px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #dee2e6; }
        th { background: #f8f9fa; }
        .error { background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px; margin: 10px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>TinySQL Web Interface</h1>
            <p>Interactive SQL database</p>
        </div>

        <div class="query-section">
            <h2>SQL Query Editor</h2>
            <textarea id="sqlInput" placeholder="Enter your SQL query here..."></textarea>
            
            <div>
                <button onclick="executeQuery()">Execute Query</button>
                <button onclick="clearQuery()" style="background: #6c757d;">Clear</button>
                <button onclick="loadSchema()" style="background: #17a2b8;">Show Schema</button>
            </div>
            
            <h3>Example Queries</h3>
            <div class="examples">
                <button class="example-btn" onclick="setQuery('SELECT * FROM employees ORDER BY name')">
                    <strong>All Employees</strong><br>Show all employee records
                </button>
                <button class="example-btn" onclick="setQuery('SELECT e.name, d.name as department FROM employees e JOIN departments d ON e.dept_id = d.id')">
                    <strong>Employees with Departments</strong><br>JOIN query example
                </button>
                <button class="example-btn" onclick="setQuery('SELECT name, location_coords, profile FROM employees WHERE id <= 2')">
                    <strong>Complex Data Types</strong><br>COMPLEX and JSON columns
                </button>
                <button class="example-btn" onclick="setQuery('SELECT d.name, COUNT(e.id) as emp_count FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.name')">
                    <strong>Department Stats</strong><br>Aggregate with GROUP BY
                </button>
            </div>
        </div>

        <div id="results" style="display: none;"></div>
    </div>

    <script>
        function setQuery(sql) {
            document.getElementById('sqlInput').value = sql;
        }

        function clearQuery() {
            document.getElementById('sqlInput').value = '';
            document.getElementById('results').style.display = 'none';
        }

        async function executeQuery() {
            const sql = document.getElementById('sqlInput').value.trim();
            if (!sql) {
                alert('Please enter a SQL query');
                return;
            }

            const resultsDiv = document.getElementById('results');
            resultsDiv.style.display = 'block';
            resultsDiv.innerHTML = '<div style="text-align: center; padding: 20px;">Executing query...</div>';

            try {
                const response = await fetch('/api/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sql: sql })
                });

                const result = await response.json();
                displayResults(result);
            } catch (error) {
                resultsDiv.innerHTML = '<div class="error">Network error: ' + error.message + '</div>';
            }
        }

        function displayResults(result) {
            const resultsDiv = document.getElementById('results');
            
            if (result.error) {
                resultsDiv.innerHTML = '<div class="error"><strong>Error:</strong> ' + result.error + '</div>';
                return;
            }

            let html = '<div class="results">';
            html += '<div class="results-header">';
            html += '<h3>Query Results</h3>';
            html += '<span>' + result.count + ' rows • ' + result.duration + '</span>';
            html += '</div>';

            if (result.rows && result.rows.length > 0) {
                html += '<div style="overflow-x: auto;"><table>';
                html += '<thead><tr>';
                result.columns.forEach(col => html += '<th>' + col + '</th>');
                html += '</tr></thead><tbody>';

                result.rows.forEach(row => {
                    html += '<tr>';
                    result.columns.forEach(col => {
                        let value = row[col];
                        if (value === null || value === undefined) {
                            value = '<em>NULL</em>';
                        } else if (typeof value === 'object') {
                            value = JSON.stringify(value);
                        }
                        html += '<td>' + value + '</td>';
                    });
                    html += '</tr>';
                });
                html += '</tbody></table></div>';
            } else {
                html += '<div style="padding: 20px; text-align: center; color: #28a745;">Query executed successfully</div>';
            }

            html += '</div>';
            resultsDiv.innerHTML = html;
        }

        async function loadSchema() {
            try {
                const response = await fetch('/api/schema');
                const schema = await response.json();
                
                let html = '<div class="results"><div class="results-header"><h3>Database Schema</h3></div>';
                html += '<div style="padding: 20px;">';
                
                schema.tables.forEach(table => {
                    html += '<h4>' + table.name + ' (' + table.rows + ' rows)</h4>';
                    html += '<table style="margin-bottom: 20px;"><thead><tr><th>Column</th><th>Type</th></tr></thead><tbody>';
                    
                    table.columns.forEach(col => {
                        html += '<tr><td><strong>' + col.name + '</strong></td><td>' + col.type + '</td></tr>';
                    });
                    html += '</tbody></table>';
                });
                
                html += '</div></div>';
                document.getElementById('results').style.display = 'block';
                document.getElementById('results').innerHTML = html;
            } catch (error) {
                document.getElementById('results').innerHTML = '<div class="error">Error loading schema: ' + error.message + '</div>';
            }
        }
    </script>
</body>
</html>`

	t, _ := template.New("index").Parse(tmpl)
	t.Execute(w, nil)
}

func handleWebQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(QueryResult{Error: "Invalid JSON: " + err.Error()})
		return
	}

	start := time.Now()
	ctx := context.Background()
	tenant := "demo"

	// Try to use compiled query for better performance
	compiled, err := webCache.Compile(req.SQL)
	if err != nil {
		json.NewEncoder(w).Encode(QueryResult{
			SQL:   req.SQL,
			Error: "Parse error: " + err.Error(),
		})
		return
	}

	rs, err := compiled.Execute(ctx, webDB, tenant)
	duration := time.Since(start)

	if err != nil {
		json.NewEncoder(w).Encode(QueryResult{
			SQL:      req.SQL,
			Error:    err.Error(),
			Duration: duration.String(),
		})
		return
	}

	// Convert result set to JSON format
	var columns []string
	var rows []map[string]interface{}

	if rs != nil && len(rs.Rows) > 0 {
		// Get columns from first row
		for col := range rs.Rows[0] {
			columns = append(columns, col)
		}

		// Convert rows
		for _, row := range rs.Rows {
			jsonRow := make(map[string]interface{})
			for col, val := range row {
				jsonRow[col] = val
			}
			rows = append(rows, jsonRow)
		}
	}

	json.NewEncoder(w).Encode(QueryResult{
		SQL:      req.SQL,
		Columns:  columns,
		Rows:     rows,
		Duration: duration.String(),
		Count:    len(rows),
	})
}

func handleWebSchema(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	tables := webDB.ListTables("demo")

	type ColumnInfo struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	type TableInfo struct {
		Name    string       `json:"name"`
		Rows    int          `json:"rows"`
		Columns []ColumnInfo `json:"columns"`
	}

	var schema struct {
		Tables []TableInfo `json:"tables"`
	}

	for _, table := range tables {
		var columns []ColumnInfo
		for _, col := range table.Cols {
			columns = append(columns, ColumnInfo{
				Name: col.Name,
				Type: col.Type.String(),
			})
		}

		schema.Tables = append(schema.Tables, TableInfo{
			Name:    table.Name,
			Rows:    len(table.Rows),
			Columns: columns,
		})
	}

	json.NewEncoder(w).Encode(schema)
}

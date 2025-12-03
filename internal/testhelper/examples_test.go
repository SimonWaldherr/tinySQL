package testhelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Structure mirrors tests/examples.yml
type examplesFile struct {
	Tables map[string]struct {
		Cols []string        `yaml:"cols"`
		Rows [][]interface{} `yaml:"rows"`
	} `yaml:"tables"`

	Queries []struct {
		ID          string                   `yaml:"id"`
		Description string                   `yaml:"description"`
		SQL         string                   `yaml:"sql"`
		Expected    struct {
			Cols []string                 `yaml:"cols"`
			Rows []map[string]interface{} `yaml:"rows"`
		} `yaml:"expected"`
	} `yaml:"queries"`
}

func TestExamplesYAML(t *testing.T) {
	// Locate tests/examples.yml. When `go test` runs package tests the
	// working directory may be the package folder, so try a few candidate
	// relative paths and pick the first that exists.
	candidates := []string{
		filepath.Join("tests", "examples.yml"),
		filepath.Join("..", "..", "tests", "examples.yml"),
		filepath.Join("..", "..", "..", "tests", "examples.yml"),
	}
	var b []byte
	var found string
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			bb, err := os.ReadFile(p)
			if err == nil {
				b = bb
				found = p
				break
			}
		}
	}
	if found == "" {
		t.Fatalf("failed to find tests/examples.yml (tried: %v)", candidates)
	}
	var ex examplesFile
	if err := yaml.Unmarshal(b, &ex); err != nil {
		t.Fatalf("failed to parse examples.yml: %v", err)
	}

	// Create a fresh storage DB for the examples
	db := storage.NewDB()

	// Helper to run a SQL statement
	run := func(sql string) (*engine.ResultSet, error) {
		p := engine.NewParser(sql)
		st, err := p.ParseStatement()
		if err != nil {
			return nil, fmt.Errorf("parse error: %w", err)
		}
		res, err := engine.Execute(context.Background(), db, "default", st)
		if err != nil {
			return nil, fmt.Errorf("exec error: %w", err)
		}
		return res, nil
	}

	// Create tables and insert data
	for tblName, tbl := range ex.Tables {
		// Infer simple column types from provided rows: prefer INTEGER, then REAL, else TEXT.
		cols := make([]string, len(tbl.Cols))
		for i, c := range tbl.Cols {
			colType := "TEXT"
			hasFloat := false
			stop := false
			for _, row := range tbl.Rows {
				if i >= len(row) {
					continue
				}
				v := row[i]
				switch v.(type) {
				case int, int64:
					if colType == "TEXT" {
						colType = "INT"
					}
				case float64:
					hasFloat = true
					colType = "FLOAT"
				case string:
					colType = "TEXT"
					// string forces TEXT; stop checking further rows
					hasFloat = false
					stop = true
				}
				if stop {
					break
				}
			}
			if hasFloat && colType != "TEXT" {
				colType = "FLOAT"
			}
			cols[i] = fmt.Sprintf("%s %s", c, colType)
		}
		createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tblName, strings.Join(cols, ", "))
		if _, err := run(createSQL); err != nil {
			t.Fatalf("failed to create table %s: %v", tblName, err)
		}

		// Insert rows
		for _, row := range tbl.Rows {
			vals := make([]string, len(row))
			for i, v := range row {
				vals[i] = literalFor(v)
			}
			ins := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tblName, strings.Join(vals, ", "))
			if _, err := run(ins); err != nil {
				t.Fatalf("failed to insert into %s: %v (sql: %s)", tblName, err, ins)
			}
		}
	}

	// Run queries
	for _, q := range ex.Queries {
		q := q // capture
		t.Run(q.ID, func(t *testing.T) {
			res, err := run(q.SQL)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			// Compare columns (order-agnostic). Normalize gotCols by stripping table prefixes like "u.name" -> "name".
			exCols := lowerSlice(q.Expected.Cols)
			gotCols := lowerSlice(res.Cols)
			for i, gc := range gotCols {
				if idx := strings.LastIndex(gc, "."); idx != -1 && idx < len(gc)-1 {
					gotCols[i] = gc[idx+1:]
				}
			}
			sort.Strings(exCols)
			sort.Strings(gotCols)
			if !reflect.DeepEqual(exCols, gotCols) {
				t.Fatalf("columns differ\nexpected: %v\ngot: %v", q.Expected.Cols, res.Cols)
			}

			// Compare rows count
			if len(q.Expected.Rows) != len(res.Rows) {
				t.Fatalf("row count differs: expected %d, got %d", len(q.Expected.Rows), len(res.Rows))
			}

			// Compare content row-by-row (order matters as YAML lists represent expected order)
			for i, expRow := range q.Expected.Rows {
				gotRow := res.Rows[i]
				for k, ev := range expRow {
					gv, ok := fetchRowVal(gotRow, k)
					if !ok {
						t.Fatalf("missing column %s in result row %d: keys=%v", k, i, keysOfRow(gotRow))
					}
					if !valueEqual(ev, gv) {
						t.Fatalf("mismatch at row %d column %s: expected=%v (%T) got=%v (%T)", i, k, ev, ev, gv, gv)
					}
				}
			}
		})
	}
}

// fetchRowVal looks up a column value in a row by trying the unqualified name
// and, if not found, any qualified name whose last segment matches the column.
func fetchRowVal(r engine.Row, col string) (any, bool) {
	lk := strings.ToLower(col)
	if v, ok := r[lk]; ok {
		return v, true
	}
	// try qualified keys (e.g. u.name or users.name)
	for k, v := range r {
		if idx := strings.LastIndex(k, "."); idx != -1 && idx < len(k)-1 {
			if k[idx+1:] == lk {
				return v, true
			}
		}
	}
	return nil, false
}

func keysOfRow(r engine.Row) []string {
	ks := make([]string, 0, len(r))
	for k := range r {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func literalFor(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case int, int64:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(x, "'", "''"))
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("'%v'", x)
	}
}

func lowerSlice(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = strings.ToLower(v)
	}
	return out
}

func valueEqual(a, b interface{}) bool {
	switch ea := a.(type) {
	case int:
		switch eb := b.(type) {
		case int:
			return ea == eb
		case int64:
			return int64(ea) == eb
		case float64:
			return float64(ea) == eb
		}
	case int64:
		switch eb := b.(type) {
		case int:
			return ea == int64(eb)
		case int64:
			return ea == eb
		case float64:
			return float64(ea) == eb
		}
	case float64:
		switch eb := b.(type) {
		case int:
			return ea == float64(eb)
		case int64:
			return ea == float64(eb)
		case float64:
			return ea == eb
		}
	case string:
		s, ok := b.(string)
		return ok && ea == s
	case bool:
		bb, ok := b.(bool)
		return ok && ea == bb
	}
	return reflect.DeepEqual(a, b)
}
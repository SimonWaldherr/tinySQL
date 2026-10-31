package engine

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The same fixtures run through the Browser/Node API in wasm-smoke.js.
func TestSQLSemanticsContract(t *testing.T) {
	if zone := os.Getenv("TZ"); zone != "" {
		if _, err := time.LoadLocation(zone); err != nil {
			t.Fatalf("timezone matrix requires %s: %v", zone, err)
		}
	}
	data, err := os.ReadFile("testdata/sql_semantics.json")
	if err != nil {
		t.Fatal(err)
	}
	var suite struct {
		Setup []string `json:"setup"`
		Cases []struct {
			Name    string   `json:"name"`
			SQL     string   `json:"sql"`
			Columns []string `json:"columns"`
			Rows    [][]any  `json:"rows"`
			Error   string   `json:"error"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&suite); err != nil {
		t.Fatal(err)
	}
	if len(suite.Cases) == 0 {
		t.Fatal("empty contract suite")
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, query := range suite.Setup {
		execSQL(t, db, query)
	}
	for _, tc := range suite.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			for _, mode := range []string{"rows", "stream", "columnar"} {
				t.Run(mode, func(t *testing.T) {
					statement, err := NewParser(tc.SQL).ParseStatement()
					var result *ResultSet
					if err == nil {
						result, err = executeContractMode(t, db, statement, mode)
					}
					if tc.Error != "" {
						if err == nil || !strings.Contains(err.Error(), tc.Error) {
							t.Fatalf("%s: got %v, want error containing %q", tc.SQL, err, tc.Error)
						}
						return
					}
					if err != nil {
						t.Fatalf("%s: %v", tc.SQL, err)
					}
					if !reflect.DeepEqual(result.Cols, tc.Columns) {
						t.Fatalf("columns %v, want %v", result.Cols, tc.Columns)
					}
					rows := make([][]any, len(result.Rows))
					for i, row := range result.Rows {
						rows[i] = make([]any, len(tc.Columns))
						for j, col := range tc.Columns {
							rows[i][j] = row[col]
						}
					}
					// Compare the public JSON value contract, independent of native int size.
					encoded, err := json.Marshal(rows)
					if err != nil {
						t.Fatal(err)
					}
					var actual [][]any
					if err := json.Unmarshal(encoded, &actual); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(actual, tc.Rows) {
						t.Fatalf("%s: rows %s, want %v", tc.SQL, encoded, tc.Rows)
					}
				})
			}
		})
	}
}

func executeContractMode(t *testing.T, db *storage.DB, statement Statement, mode string) (*ResultSet, error) {
	t.Helper()
	switch mode {
	case "stream":
		stream, err := ExecuteStream(t.Context(), db, "default", statement)
		if err != nil {
			return nil, err
		}
		defer stream.Close()
		result := &ResultSet{Cols: stream.Columns(), Rows: []Row{}}
		for stream.Next() {
			row := make(Row, len(result.Cols))
			for _, col := range result.Cols {
				row[col] = stream.Row()[col]
			}
			result.Rows = append(result.Rows, row)
		}
		return result, stream.Err()
	case "columnar":
		columns, err := ExecuteColumnar(t.Context(), db, "default", statement)
		if err != nil {
			return nil, err
		}
		result := &ResultSet{Cols: columns.Cols, Rows: make([]Row, columns.RowCount)}
		for i := range result.Rows {
			row := make(Row, len(columns.Cols))
			for j, col := range columns.Cols {
				row[col] = columns.Values[j][i]
			}
			result.Rows[i] = row
		}
		return result, nil
	default:
		return Execute(t.Context(), db, "default", statement)
	}
}

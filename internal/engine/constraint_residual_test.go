package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestConstraintLookupRetainsResidualPredicates(t *testing.T) {
	for _, constraint := range []string{"PRIMARY KEY", "UNIQUE"} {
		t.Run(constraint, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			execSQL(t, db, fmt.Sprintf("CREATE TABLE users (id INT %s, name TEXT)", constraint))
			execSQL(t, db, "INSERT INTO users VALUES (1, NULL), (2, 'Grace'), (3, 'Ada')")
			for _, tc := range []struct {
				predicate string
				count     int
				name      any
				residual  bool
			}{
				{"id = 2", 1, "Grace", false},
				{"id = 2 AND name IS NULL", 0, nil, true},
				{"id = 1 AND name IS NULL", 1, nil, true},
				{"id = 2 AND name LIKE 'A%'", 0, nil, true},
				{"id = 2 AND LENGTH(name) > 10", 0, nil, true},
				{"id = 2 AND NOT (name = 'Grace')", 0, nil, true},
				{"id = 2 AND (name IS NULL OR name = 'Ada')", 0, nil, true},
				{"id = 2 AND id = 3", 0, nil, true},
			} {
				t.Run(tc.predicate, func(t *testing.T) {
					sql := "SELECT name FROM users WHERE " + tc.predicate
					stmt := mustParse(sql).(*Select)
					plan, ok, err := buildSimpleSelectPlan(ExecEnv{ctx: t.Context(), db: db, tenant: "default"}, stmt)
					if err != nil || !ok {
						t.Fatalf("plan: ok=%v error=%v", ok, err)
					}
					if plan.scanType != "CONSTRAINT INDEX POINT SEEK" || plan.filterFullyCovered == tc.residual {
						t.Fatalf("scan=%s covered=%v, want residual=%v", plan.scanType, plan.filterFullyCovered, tc.residual)
					}
					for _, query := range []string{sql, sql + " ORDER BY name", "SELECT DISTINCT name FROM users WHERE " + tc.predicate} {
						stmt := mustParse(query)
						rows := execSQL(t, db, query).Rows
						if len(rows) != tc.count || (len(rows) > 0 && rows[0]["name"] != tc.name) {
							t.Fatalf("%s: rows=%v, want %d rows with name=%v", query, rows, tc.count, tc.name)
						}
						columns, err := ExecuteColumnar(t.Context(), db, "default", stmt)
						if err != nil || columns.RowCount != tc.count || (columns.RowCount > 0 && columns.Values[0][0] != tc.name) {
							t.Fatalf("%s: columnar=%v error=%v", query, columns, err)
						}
						stream, err := ExecuteStream(t.Context(), db, "default", stmt)
						if err != nil {
							t.Fatal(err)
						}
						count := 0
						for stream.Next() {
							count++
							if stream.Row()["name"] != tc.name {
								t.Errorf("stream row: %v, want name=%v", stream.Row(), tc.name)
							}
						}
						streamErr := stream.Err()
						_ = stream.Close()
						if streamErr != nil || count != tc.count {
							t.Fatalf("%s: stream rows=%d error=%v, want %d", query, count, streamErr, tc.count)
						}
					}
				})
			}
		})
	}
}

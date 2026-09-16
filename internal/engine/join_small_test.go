package engine

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSmallJoinEqualityMatchesGeneralEvaluation(t *testing.T) {
	rows := func(alias string, values ...any) []Row {
		result := make([]Row, len(values))
		for i, value := range values {
			result[i] = Row{"id": value, alias + ".id": value, alias + ".label": fmt.Sprintf("%s%d", alias, i)}
		}
		return result
	}
	instant := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		left    []Row
		right   []Row
		on      string
		binding *triggerRowBinding
		wantErr string
	}{
		{name: "duplicates and unmatched", left: rows("l", 2, 1, 1, 3), right: rows("r", 1, 1, 2), on: "l.id = r.id"},
		{name: "reversed operands", left: rows("l", 2, 1), right: rows("r", 1, 2), on: "r.id = l.id"},
		{name: "case insensitive", left: rows("l", 1), right: rows("r", 1), on: "L.ID = R.ID"},
		{name: "numeric coercion", left: rows("l", int(1), int64(2), float64(3)), right: rows("r", float64(1), int(2), int64(3)), on: "l.id = r.id"},
		{name: "exact integers", left: rows("l", int64(9007199254740992), int64(9007199254740993)), right: rows("r", int64(9007199254740993)), on: "l.id = r.id"},
		{name: "decimal", left: rows("l", big.NewRat(3, 2)), right: rows("r", big.NewRat(6, 4), big.NewRat(2, 1)), on: "l.id = r.id"},
		{name: "blob", left: rows("l", []byte{1, 2}, []byte{2}), right: rows("r", []byte{1, 2}), on: "l.id = r.id"},
		{name: "timestamp", left: rows("l", instant), right: rows("r", instant.In(time.FixedZone("offset", 3600))), on: "l.id = r.id"},
		{name: "null", left: rows("l", nil, 1), right: rows("r", nil, 1), on: "l.id = r.id"},
		{name: "right precedence", left: rows("l", 1), right: rows("r", 2, 3), on: "id = r.id"},
		{name: "right null precedence", left: rows("l", 1), right: rows("r", nil), on: "id = l.id"},
		{name: "same side references", left: rows("l", 1, 2), right: rows("r", 1, 3), on: "l.id = l.id"},
		{name: "empty right", left: rows("l", 1), on: "l.id = r.id"},
		{name: "empty left", right: rows("r", 1), on: "l.id = r.id"},
		{name: "incomparable", left: rows("l", 1), right: rows("r", "1"), on: "l.id = r.id", wantErr: "incomparable int and string"},
		{name: "incomparable reversed", left: rows("l", 1), right: rows("r", "1"), on: "r.id = l.id", wantErr: "incomparable string and int"},
		{name: "missing column", left: rows("l", 1), right: rows("r", 1), on: "l.missing = r.id", wantErr: "unknown column"},
		{name: "null still evaluates missing operand", left: rows("l", nil), right: rows("r", 1), on: "l.id = r.missing", wantErr: "unknown column"},
		{name: "sparse row", left: append(rows("l", 1), Row{"l.label": "missing"}), right: rows("r", 1), on: "l.id = r.id", wantErr: "unknown column"},
		{name: "trigger fallback", left: rows("l", 1, 2), right: rows("r", 1), on: "l.id = NEW.id", binding: &triggerRowBinding{newRow: Row{"id": 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			condition := mustParse("SELECT * FROM l JOIN r ON " + tc.on).(*Select).Joins[0].On
			// AND TRUE deliberately keeps the reference query on the generic
			// expression evaluator while preserving its values and errors.
			general := &Binary{Op: "AND", Left: condition, Right: &Literal{Val: true}}
			env := ExecEnv{ctx: context.Background(), triggerRow: tc.binding}
			for _, kind := range []string{"inner", "left", "right", "full"} {
				t.Run(kind, func(t *testing.T) {
					run := func(on Expr) ([]Row, error) {
						rightTable := &storage.Table{Cols: []storage.Column{{Name: "id"}, {Name: "label"}}}
						switch kind {
						case "inner":
							return processInnerJoin(env, tc.left, tc.right, on)
						case "right":
							return processRightJoin(env, tc.left, tc.right, on)
						case "full":
							return processFullOuterJoin(env, tc.left, tc.right, on, "r", rightTable)
						default:
							return processLeftJoin(env, tc.left, tc.right, on, "r", rightTable)
						}
					}
					want, wantErr := run(general)
					got, gotErr := run(condition)
					if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
						t.Fatalf("error = %v; general evaluation returned %v", gotErr, wantErr)
					}
					if tc.wantErr != "" && (gotErr == nil || !strings.Contains(gotErr.Error(), tc.wantErr)) {
						t.Fatalf("error = %v, want containing %q", gotErr, tc.wantErr)
					}
					if tc.wantErr == "" && gotErr != nil {
						t.Fatal(gotErr)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("rows = %#v; general evaluation returned %#v", got, want)
					}
				})
			}
		})
	}
}

func TestSmallGeneralJoinSQLPreservesDuplicatesAndNulls(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE l (id INT, label TEXT)`)
	execSQL(t, db, `CREATE TABLE r (id INT, label TEXT)`)
	execSQL(t, db, `INSERT INTO l VALUES (1, 'l1'), (2, 'l2'), (NULL, 'ln')`)
	execSQL(t, db, `INSERT INTO r VALUES (1, 'r1'), (1, 'r2'), (NULL, 'rn')`)
	for _, tc := range []struct {
		join string
		want []Row
	}{
		{"JOIN", []Row{{"ll": "l1", "rr": "r1"}, {"ll": "l1", "rr": "r2"}}},
		{"LEFT JOIN", []Row{{"ll": "l1", "rr": "r1"}, {"ll": "l1", "rr": "r2"}, {"ll": "l2", "rr": nil}, {"ll": "ln", "rr": nil}}},
		{"RIGHT JOIN", []Row{{"ll": "l1", "rr": "r1"}, {"ll": "l1", "rr": "r2"}, {"ll": nil, "rr": "rn"}}},
		{"FULL OUTER JOIN", []Row{{"ll": "l1", "rr": "r1"}, {"ll": "l1", "rr": "r2"}, {"ll": "l2", "rr": nil}, {"ll": "ln", "rr": nil}, {"ll": nil, "rr": "rn"}}},
	} {
		t.Run(tc.join, func(t *testing.T) {
			rs := execSQL(t, db, "SELECT l.label AS ll, r.label AS rr FROM l "+tc.join+" r ON r.id = l.id ORDER BY ll, rr")
			if !reflect.DeepEqual(rs.Rows, tc.want) {
				t.Fatalf("rows = %#v, want %#v", rs.Rows, tc.want)
			}
		})
	}
}

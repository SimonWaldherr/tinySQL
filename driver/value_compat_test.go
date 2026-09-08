package driver_test

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/testutil"
)

type compatInt int32
type compatText string
type compatValuer struct{ ValueText string }

func (v compatValuer) Value() (driver.Value, error) { return v.ValueText, nil }

type compatErrorValuer struct{}

var errCompatValue = errors.New("value conversion failed")

func (compatErrorValuer) Value() (driver.Value, error) { return nil, errCompatValue }

func TestDatabaseSQLValueCompatibility(t *testing.T) {
	db := testutil.Open(t)
	text := "O'Reilly"
	var nilText *string
	var nilValue *compatValuer
	for _, q := range []string{"SELECT ? AS value", "SELECT $1 AS value", "SELECT $1 AS value /* __tinysql_prepared_param_ */"} {
		for _, tc := range []struct {
			name  string
			input any
			want  any
		}{
			{"null valid", sql.NullString{String: "valid", Valid: true}, "valid"},
			{"null invalid", sql.NullString{}, nil},
			{"custom valuer", compatValuer{ValueText: "converted"}, "converted"},
			{"nil valuer", nilValue, nil},
			{"pointer", &text, text},
			{"nil pointer", nilText, nil},
			{"integer alias", compatInt(42), int64(42)},
			{"text alias", compatText("alias"), "alias"},
		} {
			t.Run(q+"/"+tc.name, func(t *testing.T) {
				var got any
				if err := db.QueryRow(q, tc.input).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != tc.want {
					t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
				}
			})
		}
		var got any
		if err := db.QueryRow(q, compatErrorValuer{}).Scan(&got); !errors.Is(err, errCompatValue) {
			t.Fatalf("valuer error lost: %v", err)
		}
	}
}

// Custom Scanner implementations must receive the same supported values as
// ordinary database/sql Scan destinations.
type compatScanner string

func (s *compatScanner) Scan(v any) error {
	text, ok := v.(string)
	if !ok {
		return fmt.Errorf("unexpected scanner value %T", v)
	}
	*s = compatScanner(text)
	return nil
}
func TestDatabaseSQLScannerCompatibility(t *testing.T) {
	db := testutil.Open(t)
	var got compatScanner
	if err := db.QueryRow("SELECT ?", "scanned").Scan(&got); err != nil || got != "scanned" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

func TestDatabaseSQLTimestampTextCompatibility(t *testing.T) {
	db := testutil.Open(t, "CREATE TABLE times (id INT, value TIMESTAMP)")
	want := time.Date(2026, 9, 8, 12, 34, 56, 123456789, time.UTC)
	if _, err := db.Exec("INSERT INTO times VALUES (?, ?)", 1, want); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.QueryRow("SELECT value FROM times WHERE id = ?", 1).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want.Format(time.RFC3339Nano) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDatabaseSQLRejectsNativeNamedParameters(t *testing.T) {
	db := testutil.Open(t)
	for _, query := range []string{"SELECT :value", "SELECT ?"} {
		var value any
		err := db.QueryRow(query, sql.Named("value", 1)).Scan(&value)
		if err == nil || !strings.Contains(err.Error(), "named parameters are not supported") {
			t.Fatalf("%s: expected explicit rejection, got %v", query, err)
		}
	}
}

func TestDatabaseSQLValuerFailureDoesNotWrite(t *testing.T) {
	db := testutil.Open(t, "CREATE TABLE items (value TEXT)")
	if _, err := db.Exec("INSERT INTO items VALUES (?)", compatErrorValuer{}); !errors.Is(err, errCompatValue) {
		t.Fatalf("valuer error lost: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed conversion wrote data: count=%d err=%v", count, err)
	}
}

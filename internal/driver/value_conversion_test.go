package driver

import (
	"database/sql/driver"
	"math"
	"reflect"
	"testing"
)

type badCompatValuer struct{}

func (badCompatValuer) Value() (driver.Value, error) { return []int{1}, nil }

type blobCompatValuer []byte

func (b blobCompatValuer) Value() (driver.Value, error) { return []byte(b), nil }

func TestDriverValueConversionBoundaries(t *testing.T) {
	for _, v := range []any{badCompatValuer{}, uint64(math.MaxUint64), make(chan int)} {
		if _, err := normalizeDriverArgument(v); err == nil {
			t.Fatalf("accepted invalid parameter %T", v)
		}
	}
	for _, v := range []any{map[string]any{"x": 1}, []float32{1, 2}, []float64{1, 2}, []any{1, "x"}, [2]int{1, 2}} {
		got, err := normalizeDriverArgument(v)
		if err != nil || !reflect.DeepEqual(got, v) {
			t.Fatalf("native extension %T: got %#v err=%v", v, got, err)
		}
	}
	input := blobCompatValuer{1, 2, 3}
	got, err := normalizeDriverArgument(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 9
	if !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("valuer BLOB aliases input: %v", got)
	}
}

// Keep compile-time checks for the public database/sql extension contracts.
var (
	_ driver.Driver             = (*drv)(nil)
	_ driver.DriverContext      = (*drv)(nil)
	_ driver.Connector          = (*connector)(nil)
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.StmtExecContext    = (*stmt)(nil)
	_ driver.StmtQueryContext   = (*stmt)(nil)
)

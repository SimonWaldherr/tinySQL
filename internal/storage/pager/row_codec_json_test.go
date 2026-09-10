package pager

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func TestRowCodecJSON(t *testing.T) {
	for _, value := range []any{map[string]any{"n": float64(3), "text": strings.Repeat("x", 70000)}, []any{true, nil, map[string]any{"s": "value"}}} {
		row := []any{value, []byte{0, 255, 1}, "tail"}
		encoded := MarshalRow(row, nil)
		decoded, err := UnmarshalRow(encoded)
		if err != nil || !reflect.DeepEqual(decoded, row) {
			t.Fatalf("round trip: %v %v", decoded, err)
		}
		cell, err := UnmarshalRowColumn(encoded, 0)
		if err != nil || !reflect.DeepEqual(cell, value) {
			t.Fatalf("column decode: %v %v", cell, err)
		}
		blob, err := unmarshalRowBytesColumn(encoded, 1, true)
		if err != nil || !reflect.DeepEqual(blob, row[1]) {
			t.Fatalf("skip JSON: %v %v", blob, err)
		}
		tail, err := UnmarshalRowColumn(encoded, 2)
		if err != nil || tail != "tail" {
			t.Fatalf("tail: %v %v", tail, err)
		}
	}
}

func TestRowCodecRejectsMalformedJSON(t *testing.T) {
	encoded := MarshalRow([]any{map[string]any{"a": true}}, nil)
	for _, invalid := range [][]byte{encoded[:len(encoded)-1], append(append([]byte(nil), encoded[:len(encoded)-1]...), '!')} {
		if _, err := UnmarshalRow(invalid); err == nil {
			t.Fatal("invalid JSON accepted")
		}
		if _, err := UnmarshalRowColumn(invalid, 0); err == nil {
			t.Fatal("invalid JSON column accepted")
		}
	}
	oversized := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint32(oversized[3:7], MaxValueBytes+1)
	if _, err := UnmarshalRow(oversized); err == nil {
		t.Fatal("oversized JSON accepted")
	}
}

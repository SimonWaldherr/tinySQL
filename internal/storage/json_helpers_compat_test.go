package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// This reference preserves the eager normalization used by the storage codec
// before containers were shared when no conversion was needed.
func eagerJSONNormalize(v any) any {
	switch x := v.(type) {
	case *big.Rat:
		return x.String()
	case big.Rat:
		return x.String()
	case uuid.UUID:
		return x.String()
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			out[k] = eagerJSONNormalize(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = eagerJSONNormalize(v)
		}
		return out
	default:
		return v
	}
}

func TestJSONNormalizationMatchesEagerEncoding(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-9abc-def012345678")
	var nilMap map[string]any
	var nilSlice []any
	inputs := []any{
		nilMap, nilSlice, map[string]any{"array": nilSlice, "object": nilMap},
		[]any{map[string]any{"z": []any{big.NewRat(1, 3), id, nilMap}, "a": "<x>\xff"}, []byte{0, 255}},
		map[string]any{"nested": []any{map[string]any{"n": big.NewRat(-7, 9)}}, "plain": []any{true, 1, nil}},
		[]any{map[string]any{"x": math.Inf(1)}},
	}
	for _, in := range inputs {
		before := eagerJSONNormalize(in)
		want, wantErr := json.Marshal(before)
		got, gotErr := JSONMarshal(in)
		if !bytes.Equal(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && reflect.TypeOf(gotErr) != reflect.TypeOf(wantErr) {
			t.Fatalf("%#v: got %s / %v, want %s / %v", in, got, gotErr, want, wantErr)
		}
		if !reflect.DeepEqual(eagerJSONNormalize(in), before) {
			t.Fatalf("input was changed: %#v", in)
		}
	}
}

type mutatingJSONMarshaler struct{ target map[string]any }

func (m mutatingJSONMarshaler) MarshalJSON() ([]byte, error) {
	m.target["value"] = "changed"
	return []byte(`"ok"`), nil
}

func TestJSONNormalizationPreservesIsolationFromCustomMarshaler(t *testing.T) {
	target := map[string]any{"value": "original"}
	input := map[string]any{"a": mutatingJSONMarshaler{target: target}, "z": target}
	got, err := JSONMarshal(input)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":"ok","z":{"value":"original"}}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestJSONRowNormalizationMatchesEagerEncoding(t *testing.T) {
	for _, rows := range [][][]any{
		nil, {}, {nil, {}},
		{{map[string]any{"nested": []any{big.NewRat(1, 3), true}}}, {nil, []any(nil), map[string]any(nil)}},
	} {
		wantRows := make([][]any, len(rows))
		for i, row := range rows {
			wantRows[i] = eagerJSONNormalize(row).([]any)
		}
		want, err := json.Marshal(wantRows)
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(normalizeJSONRows(rows))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("got %s / %v, want %s", got, err, want)
		}
	}
}

func TestJSONDiskNormalizationPreservesCrossRowIsolation(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("gzip=%t/encrypted=%t", compressed, encrypted), func(t *testing.T) {
				backend, err := NewJSONBackend(t.TempDir(), compressed)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := backend.Close(); err != nil {
						t.Error(err)
					}
				})
				if encrypted {
					encryptor, err := NewEncryptor(bytes.Repeat([]byte{0x31}, EncryptionKeySize))
					if err != nil {
						t.Fatal(err)
					}
					backend.SetEncryptor(encryptor)
				}
				target := map[string]any{"value": "original"}
				table := NewTable("marshal_isolation", []Column{{Name: "value", Type: StringType}}, false)
				table.Rows = [][]any{{mutatingJSONMarshaler{target: target}}, {target}}
				if err := backend.SaveTable("default", table); err != nil {
					t.Fatal(err)
				}
				loaded, err := backend.LoadTable("default", table.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]any{"value": "original"}
				if !reflect.DeepEqual(loaded.Rows[1][0], want) {
					t.Fatalf("custom marshaler changed another persisted row: got %#v, want %#v", loaded.Rows[1][0], want)
				}
			})
		}
	}
}

func TestJSONDiskNormalizationPreservesDefaultMarshalerIsolation(t *testing.T) {
	target := map[string]any{"value": "original"}
	table := diskTable{
		Cols: []diskColumn{{Name: "value", HasDefault: true, DefaultValue: mutatingJSONMarshaler{target: target}}},
		Rows: [][]any{{target}},
	}
	var output bytes.Buffer
	backend := &DiskBackend{format: diskFormatJSON}
	if err := backend.encodeTableInto(&output, table); err != nil {
		t.Fatal(err)
	}
	var loaded diskTable
	if err := json.Unmarshal(output.Bytes(), &loaded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.Rows[0][0], map[string]any{"value": "original"}) {
		t.Fatalf("default marshaler changed a persisted row: %#v", loaded.Rows[0][0])
	}
	if loaded.Cols[0].DefaultValue != "ok" {
		t.Fatalf("default representation changed: %#v", loaded.Cols[0].DefaultValue)
	}
}

func TestJSONDiskCyclicDefaultReportsEncodingError(t *testing.T) {
	value := map[string]any{}
	value["self"] = value
	table := diskTable{
		Cols: []diskColumn{{Name: "value", HasDefault: true, DefaultValue: value}},
		Rows: [][]any{{"plain"}},
	}
	backend := &DiskBackend{format: diskFormatJSON}
	err := backend.encodeTableInto(io.Discard, table)
	var unsupported *json.UnsupportedValueError
	if !errors.As(err, &unsupported) {
		t.Fatalf("cyclic default: expected an encoding error, got %v", err)
	}
}

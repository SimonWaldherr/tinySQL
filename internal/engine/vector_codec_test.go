package engine

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestVectorJSONCompatibility(t *testing.T) {
	inputs := []string{
		`[]`, `[ ]`, `null`, ` [1, -2, 0.125, 4e20, 5E-10] `,
		"\n[\t-0, null, 1.7976931348623157e308, 5e-324, 1e-400]\r",
		`[9007199254740993, -9007199254740993]`, `[1e309]`, `[-1e309]`,
		`["1"]`, `[true]`, `[false]`, `[{}]`, `[[1,2]]`, `["1,2"]`,
		`["\u0031"]`, `[1,null,"bad"]`, ``, `true`, `3`, `{}`, `"[]"`,
		`[01]`, `[+1]`, `[1.]`, `[.1]`, `[NaN]`, `[Infinity]`, `[0x1]`,
		`[1_000]`, `[1,]`, `[,1]`, `[1] []`, `null []`, "\u00a0[1]",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			var want []float64
			oldErr := json.Unmarshal([]byte(input), &want)
			got, err := parseVectorJSON(input)
			if (err != nil) != (oldErr != nil) {
				t.Fatalf("error = %v, JSON decoder error = %v", err, oldErr)
			}
			if err == nil {
				assertVectorCodecBits(t, got, want)
			}
		})
	}
	var nilVector []float64
	for _, want := range [][]float64{nilVector, {}, {0, math.Copysign(0, -1), 1e-8, 1e22}} {
		val, err := evalVecToJSON(ExecEnv{}, &FuncCall{Args: []Expr{&Literal{Val: want}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(want)
		if val != string(encoded) {
			t.Fatalf("VEC_TO_JSON = %q, want %q", val, encoded)
		}
	}
}

func TestVectorJSONErrorCompatibility(t *testing.T) {
	for _, input := range []string{
		`[true]`, `[false]`, `["1"]`, `[[1,2]]`, `[{}]`, `[null,0,true]`,
		`["\u0031"]`, `true`, `1`, `"[]"`, `{}`, `[1,]`, `[,1]`, `[NaN]`,
		`[1e309]`, `[-1e309]`, `["1",1e309]`, `not json`, `[1] []`,
	} {
		t.Run(input, func(t *testing.T) {
			_, gotErr := vecFromValue(input)
			_, wantErr := previousVectorJSONValue(input, false)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("vector conversion error = %v, want %v", gotErr, wantErr)
			}
			ex := &FuncCall{Name: "VEC_FROM_JSON", Args: []Expr{&Literal{Val: input}}}
			_, gotErr = evalVecFromJSON(ExecEnv{}, ex, nil)
			_, wantErr = previousVectorJSONValue(input, true)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("VEC_FROM_JSON error = %v, want %v", gotErr, wantErr)
			}
		})
	}
}

// Preserve the former conversion order as a diagnostic compatibility control.
func previousVectorJSONValue(input string, named bool) ([]float64, error) {
	var arr []float64
	if err := json.Unmarshal([]byte(input), &arr); err != nil {
		var anyArr []any
		if json.Unmarshal([]byte(input), &anyArr) != nil {
			if named {
				return nil, fmt.Errorf("VEC_FROM_JSON: invalid JSON array: %v", err)
			}
			return nil, fmt.Errorf("cannot interpret string as vector: %q", input)
		}
		arr, err = anySliceToFloat64(anyArr)
		if err != nil && named {
			return nil, fmt.Errorf("VEC_FROM_JSON: %v", err)
		}
		return arr, err
	}
	return arr, nil
}

func TestVectorBinaryCodecCompatibility(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	values := []float64{0, math.Copysign(0, -1), 1, -1, math.Pi, math.MaxFloat64,
		math.SmallestNonzeroFloat64, math.MaxFloat32, float64(math.SmallestNonzeroFloat32),
		math.NaN(), math.Inf(1), math.Inf(-1)}
	for i := 0; i < 1024; i++ {
		values = append(values, math.Float64frombits(rng.Uint64()))
	}
	for _, n := range []int{0, 1, 3, 12, 1036} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			input := values[:n]
			blob := make([]byte, len(input)*4)
			want := make([]float64, len(input))
			for i, v := range input {
				bits := math.Float32bits(float32(v))
				binary.LittleEndian.PutUint32(blob[i*4:], bits)
				want[i] = float64(math.Float32frombits(bits))
			}
			encoded, err := vectorToHex(input)
			if err != nil || encoded != hex.EncodeToString(blob) {
				t.Fatalf("HEX encoder = %q, %v", encoded, err)
			}
			for _, text := range []string{encoded, strings.ToUpper(encoded)} {
				got, err := vectorFromHex(text)
				if err != nil {
					t.Fatal(err)
				}
				assertVectorCodecBits(t, got, want)
			}
			for _, value := range []any{blob, " \n" + strings.ToUpper(encoded) + "\t"} {
				got, err := evalVecFromBytes(ExecEnv{}, &FuncCall{Args: []Expr{&Literal{Val: value}}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				assertVectorCodecBits(t, got.([]float64), want)
			}
		})
	}
}

func TestVectorCodecOwnershipAndErrors(t *testing.T) {
	native := []float64{1, 2}
	borrowed, err := vecFromValue(native)
	if err != nil || &borrowed[0] != &native[0] {
		t.Fatalf("native vector was copied: %v", err)
	}
	converted, err := vecFromValue([]any{int(1), int64(2), float64(3), json.Number("4")})
	if err != nil || !reflect.DeepEqual(converted, []float64{1, 2, 3, 4}) {
		t.Fatalf("slice conversion = %v, %v", converted, err)
	}
	for _, value := range []any{nil, 1, []byte{1, 2, 3}, []any{nil}, []any{"1"}, `["1"]`} {
		if _, err := vecFromValue(value); err == nil {
			t.Errorf("accepted invalid vector %v", value)
		}
	}
	for _, value := range []any{nil, 1, []byte{1}, []byte{1, 2, 3}, "0", "00", "000000", "gg", "0000000g", "00000000ff"} {
		if _, err := evalVecFromBytes(ExecEnv{}, &FuncCall{Args: []Expr{&Literal{Val: value}}}, nil); err == nil {
			t.Errorf("accepted invalid binary vector %v", value)
		}
	}
	for _, input := range []string{"00", "gg", "g", "0000000g", "00000000ff"} {
		var wantErr string
		blob, err := hexDecodeString(input)
		if err != nil {
			wantErr = "VEC_FROM_BYTES: invalid hex: " + err.Error()
		} else {
			wantErr = fmt.Sprintf("VEC_FROM_BYTES: byte length %d is not a multiple of 4", len(blob))
		}
		_, err = vectorFromHex(input)
		if err == nil || err.Error() != wantErr {
			t.Fatalf("invalid HEX %q error = %v, want %q", input, err, wantErr)
		}
	}
	blob := []byte{0, 0, 128, 63}
	value, err := evalVecFromBytes(ExecEnv{}, &FuncCall{Args: []Expr{&Literal{Val: blob}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	vector := value.([]float64)
	blob[3] = 0
	if vector[0] != 1 {
		t.Fatal("binary decoding retained mutable input")
	}
	vector[0] = 2
	if blob[3] != 0 {
		t.Fatal("vector mutation changed binary input")
	}
	for c := 0; c <= 255; c++ {
		text := "0000000" + string([]byte{byte(c)})
		decoded, wantErr := hex.DecodeString(text)
		got, err := vectorFromHex(text)
		if (err != nil) != (wantErr != nil) {
			t.Fatalf("HEX byte %x: error = %v, standard decoder error = %v", c, err, wantErr)
		}
		if err == nil {
			want := float64(math.Float32frombits(binary.LittleEndian.Uint32(decoded)))
			assertVectorCodecBits(t, got, []float64{want})
		}
	}
}

func FuzzVectorJSONCompatibility(f *testing.F) {
	for _, s := range []string{`[]`, `null`, `[1,null,-0,1e-400]`, `[[1,2],3]`, `["1,2"]`, `[1e309]`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		var want []float64
		wantErr := json.Unmarshal([]byte(input), &want)
		got, err := parseVectorJSON(input)
		if (err != nil) != (wantErr != nil) {
			t.Fatalf("error = %v, JSON decoder error = %v", err, wantErr)
		}
		if err == nil {
			assertVectorCodecBits(t, got, want)
		}
	})
}

func assertVectorCodecBits(t *testing.T, got, want []float64) {
	t.Helper()
	if (got == nil) != (want == nil) || len(got) != len(want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	for i := range want {
		if math.Float64bits(got[i]) != math.Float64bits(want[i]) {
			t.Fatalf("dimension %d = %x, want %x", i, math.Float64bits(got[i]), math.Float64bits(want[i]))
		}
	}
}

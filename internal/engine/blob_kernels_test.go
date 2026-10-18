package engine

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"unsafe"
)

func TestBlobSourceMatchesLegacyDecoding(t *testing.T) {
	values := []any{nil, []byte(nil), []byte{}, []byte{0, 255}, "", " \t\r\n", "deadbeef", " DEadBEEF ", "abc", "xyz", "café", "  hello  ", "0g", "00 00"}
	rng := rand.New(rand.NewSource(1))
	for range 500 {
		buf := make([]byte, rng.Intn(80))
		for i := range buf {
			buf[i] = byte(rng.Intn(256))
		}
		values = append(values, string(buf))
	}
	for _, v := range values {
		want, err := blobDecode(v)
		if err != nil {
			t.Fatal(err)
		}
		source, err := readBlobSource(v)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, source.len())
		source.copyTo(got, 0)
		if !bytes.Equal(got, want) {
			t.Fatalf("readBlobSource(%#v) = %x; want %x", v, got, want)
		}
		for start := range want {
			part := make([]byte, len(want)-start)
			source.copyTo(part, start)
			if !bytes.Equal(part, want[start:]) || source.at(start) != want[start] {
				t.Fatalf("readBlobSource(%#v) at %d: got %x; want %x", v, start, part, want[start:])
			}
		}
		concat, err := blobConcatValue(v, []byte{0xab})
		wantConcat := append(append([]byte{}, want...), 0xab)
		if err != nil || !bytes.Equal(concat.([]byte), wantConcat) {
			t.Fatalf("concat(%#v, X'ab') = %x, %v; want %x", v, concat, err, wantConcat)
		}
	}
}

func TestBlobKernelsRetainNullAndLegacyStringSemantics(t *testing.T) {
	values := []any{nil, []byte(nil), []byte{}, []byte{0, 255}, []byte("hello"), "", "   ", "00ff", "00FF", "68656c6c6f", "hello", " λ ", "0g"}
	for _, a := range values {
		decodedA, _ := blobDecode(a)
		length, err := blobLengthValue(a)
		if err != nil || (a == nil && length != nil) || (a != nil && length != len(decodedA)) {
			t.Errorf("length(%#v) = %v, %v", a, length, err)
		}
		hexValue, err := blobHexValue(a)
		if err != nil || (a == nil && hexValue != nil) || (a != nil && hexValue != blobHexString(decodedA)) {
			t.Errorf("hex(%#v) = %v, %v", a, hexValue, err)
		}
		for _, b := range values {
			decodedB, _ := blobDecode(b)
			wantEqual := bytes.Equal(decodedA, decodedB)
			if a == nil || b == nil {
				wantEqual = a == nil && b == nil
			}
			got, err := blobEqualValue(a, b)
			if err != nil || got != wantEqual {
				t.Errorf("equal(%#v, %#v) = %v, %v; want %v", a, b, got, err, wantEqual)
			}
			concat, err := blobConcatValue(a, b)
			gotBytes, ok := concat.([]byte)
			wantBytes := append(append([]byte{}, decodedA...), decodedB...)
			if err != nil || !ok || !bytes.Equal(gotBytes, wantBytes) {
				t.Errorf("concat(%#v, %#v) = %#v, %v; want %x", a, b, concat, err, wantBytes)
			}
		}
	}
}

func TestBlobSubstrBoundsAndOwnership(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, input := range []any{[]byte{0, 17, 34, 51}, "00112233", "hello!"} {
		decoded, _ := blobDecode(input)
		for _, tc := range []struct {
			start, length any
			want          []byte
		}{
			{0, 2, decoded[:2]}, {1, maxInt, decoded[1:]}, {maxInt, 1, []byte{}},
			{-1, 2, []byte{}}, {0, 0, []byte{}}, {1.9, 1.9, decoded[1:2]},
		} {
			got, err := blobSubstrValue(input, tc.start, tc.length)
			gotBytes, ok := got.([]byte)
			if err != nil || !ok || !bytes.Equal(gotBytes, tc.want) {
				t.Fatalf("substr(%#v,%v,%v) = %#v, %v; want %x", input, tc.start, tc.length, got, err, tc.want)
			}
		}
	}
	for _, tc := range []struct {
		start, length any
		message       string
	}{
		{0, -1, "BLOB_SUBSTR length must be non-negative"},
		{0, nil, "BLOB_SUBSTR length: expected integer, got <nil>"},
		{nil, 1, "BLOB_SUBSTR start: expected integer, got <nil>"},
		{math.Inf(1), 1, "BLOB_SUBSTR start: integer out of range"},
		{math.NaN(), 1, "BLOB_SUBSTR start: integer out of range"},
		{0, math.Inf(-1), "BLOB_SUBSTR length: integer out of range"},
		{float64(maxInt) + 1, 1, "BLOB_SUBSTR start: integer out of range"},
	} {
		if _, err := blobSubstrValue([]byte{1, 2}, tc.start, tc.length); err == nil || err.Error() != tc.message {
			t.Errorf("substr bounds %v,%v: got %v; want %s", tc.start, tc.length, err, tc.message)
		}
	}
	if got, err := blobSubstrValue(nil, nil, nil); err != nil || got != nil {
		t.Errorf("substr(NULL,NULL,NULL) = %v,%v; want NULL", got, err)
	}
	input := []byte{1, 2, 3, 4}
	part, _ := blobSubstrValue(input, 1, 2)
	concat, _ := blobConcatValue(input, nil)
	part.([]byte)[0], concat.([]byte)[0] = 99, 98
	if !bytes.Equal(input, []byte{1, 2, 3, 4}) {
		t.Fatalf("BLOB result aliases its input: %x", input)
	}
}

func TestBlobHexDoesNotRetainLargePadding(t *testing.T) {
	input := strings.Repeat(" ", 8192) + "00ff" + strings.Repeat(" ", 8192)
	got, err := blobHexValue(input)
	if err != nil || got != "00ff" {
		t.Fatalf("hex(padded) = %v, %v; want 00ff", got, err)
	}
	// Compare backing addresses without modifying either immutable string.
	if unsafe.StringData(got.(string)) == unsafe.StringData(strings.TrimSpace(input)) {
		t.Fatal("small hex result retains the large padded input")
	}
	if got, err := blobHexValue(strings.Repeat(" ", 8192)); err != nil || got != "" {
		t.Fatalf("hex(all padding) = %v, %v; want empty text", got, err)
	}
}

func TestBlobConcatMalformedTextAndLargeInput(t *testing.T) {
	var text, large any = "0g", make([]byte, 1<<20)
	result, err := blobConcatValue(text, large)
	if err != nil {
		t.Fatal(err)
	}
	got := result.([]byte)
	if len(got) != 2+(1<<20) || string(got[:2]) != "0g" {
		t.Fatalf("concat malformed text prefix = %x, length = %d", got[:2], len(got))
	}
	got[2] = 99
	if large.([]byte)[0] != 0 {
		t.Fatal("concat result aliases the large input")
	}
}

func TestBlobBase64AlphabetAndErrorCompatibility(t *testing.T) {
	inputs := []string{"", "Zg==", "3q2+7w==", "3q2-7w==", "__8=", "//8=", "  __8=\n", "++A!", "--A!", "+_A=", "aa", "!!!", "a\r\nbc=", "A==="}
	rng := rand.New(rand.NewSource(2))
	const alphabet = "ABab09+/-_=!\r\n "
	for range 500 {
		buf := make([]byte, rng.Intn(30))
		for i := range buf {
			buf[i] = alphabet[rng.Intn(len(alphabet))]
		}
		inputs = append(inputs, string(buf))
	}
	for _, input := range inputs {
		trimmed := strings.TrimSpace(input)
		want, wantErr := base64.StdEncoding.DecodeString(trimmed)
		if wantErr != nil {
			want, wantErr = base64.URLEncoding.DecodeString(trimmed)
		}
		got, err := blobFromBase64Value(input)
		if wantErr != nil {
			message := fmt.Sprintf("BLOB_FROM_BASE64: %v", wantErr)
			if err == nil || err.Error() != message {
				t.Errorf("base64(%q): got %v; want %s", input, err, message)
			}
		} else if err != nil || !bytes.Equal(got.([]byte), want) {
			t.Errorf("base64(%q) = %x, %v; want %x", input, got, err, want)
		}
	}
}

func TestBlobFunctionsEvaluateAllArgumentsBeforeConversion(t *testing.T) {
	bad := &FuncCall{Name: "BLOB_LENGTH"} // Deliberately wrong arity.
	_, wantErr := evalExpr(ExecEnv{}, bad, nil)
	for _, tc := range []struct {
		handler funcHandler
		args    []Expr
	}{
		{evalBlobSubstr, []Expr{&Literal{Val: false}, &Literal{Val: 0}, bad}},
		{evalBlobConcat, []Expr{&Literal{Val: false}, bad}},
		{evalBlobEqual, []Expr{&Literal{Val: nil}, bad}},
	} {
		if _, err := tc.handler(ExecEnv{}, &FuncCall{Args: tc.args}, nil); err == nil || err.Error() != wantErr.Error() {
			t.Errorf("argument evaluation returned %v; want %v", err, wantErr)
		}
	}
	for _, tc := range []struct {
		call func() (any, error)
		want string
	}{
		{func() (any, error) { return blobSubstrValue(false, nil, nil) }, "BLOB: cannot decode bool"},
		{func() (any, error) { return blobConcatValue(false, 1) }, "BLOB: cannot decode bool"},
		{func() (any, error) { return blobEqualValue(false, 1) }, "BLOB: cannot decode bool"},
	} {
		if _, err := tc.call(); err == nil || err.Error() != tc.want {
			t.Errorf("conversion error = %v; want %s", err, tc.want)
		}
	}
}

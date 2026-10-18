package engine

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

// These controls retain the previous kernels so allocation and timing changes
// can be compared in one process, independently of SQL planning and storage.
func previousBlobLength(v any) (any, error) {
	b, err := blobDecode(v)
	if err != nil {
		return nil, err
	}
	return len(b), nil
}

func previousBlobHex(v any) (any, error) {
	b, err := blobDecode(v)
	if err != nil {
		return nil, err
	}
	return hex.EncodeToString(b), nil
}

func previousBlobSubstr(v any, start, length int) (any, error) {
	b, err := blobDecode(v)
	if err != nil {
		return nil, err
	}
	end := min(start+length, len(b))
	return append([]byte(nil), b[start:end]...), nil
}

func previousBlobConcat(aVal, bVal any) (any, error) {
	a, err := blobDecode(aVal)
	if err != nil {
		return nil, err
	}
	b, err := blobDecode(bVal)
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(a)+len(b))
	copy(result, a)
	copy(result[len(a):], b)
	return result, nil
}

func previousBlobEqual(aVal, bVal any) (any, error) {
	a, err := blobDecode(aVal)
	if err != nil {
		return nil, err
	}
	b, err := blobDecode(bVal)
	if err != nil {
		return nil, err
	}
	if len(a) != len(b) {
		return false, nil
	}
	for i := range a {
		if a[i] != b[i] {
			return false, nil
		}
	}
	return true, nil
}

func previousBlobFromBase64(v any) (any, error) {
	s := v.(string)
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(strings.TrimSpace(s))
	}
	return b, err
}

var blobBenchmarkResult any

func BenchmarkBlobKernels(b *testing.B) {
	data := make([]byte, 32<<10)
	for i := range data {
		data[i] = byte(i)
	}
	other := append([]byte(nil), data...)
	hexText := hex.EncodeToString(data)
	urlBase64 := base64.URLEncoding.EncodeToString(data)
	var dataValue, otherValue, hexValue, urlValue any = data, other, hexText, urlBase64
	var malformedValue, largeValue any = "0g", make([]byte, 16<<20)
	for _, tc := range []struct {
		name          string
		before, after func() (any, error)
	}{
		{"Length/Hex32KiB", func() (any, error) { return previousBlobLength(hexValue) }, func() (any, error) { return blobLengthValue(hexValue) }},
		{"Hex/Hex32KiB", func() (any, error) { return previousBlobHex(hexValue) }, func() (any, error) { return blobHexValue(hexValue) }},
		{"Substr/Hex32KiB_64Bytes", func() (any, error) { return previousBlobSubstr(hexValue, 1024, 64) }, func() (any, error) { return blobSubstrValue(hexValue, 1024, 64) }},
		{"Concat/Hex32KiB", func() (any, error) { return previousBlobConcat(hexValue, hexValue) }, func() (any, error) { return blobConcatValue(hexValue, hexValue) }},
		{"Concat/Bytes32KiB", func() (any, error) { return previousBlobConcat(dataValue, otherValue) }, func() (any, error) { return blobConcatValue(dataValue, otherValue) }},
		{"Concat/MalformedTextAnd16MiB", func() (any, error) { return previousBlobConcat(malformedValue, largeValue) }, func() (any, error) { return blobConcatValue(malformedValue, largeValue) }},
		{"Equal/Bytes32KiB", func() (any, error) { return previousBlobEqual(dataValue, otherValue) }, func() (any, error) { return blobEqualValue(dataValue, otherValue) }},
		{"Equal/Hex32KiB", func() (any, error) { return previousBlobEqual(hexValue, hexValue) }, func() (any, error) { return blobEqualValue(hexValue, hexValue) }},
		{"FromBase64/URL32KiB", func() (any, error) { return previousBlobFromBase64(urlValue) }, func() (any, error) { return blobFromBase64Value(urlValue) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, kernel := range []struct {
				name string
				call func() (any, error)
			}{{"Previous", tc.before}, {"Current", tc.after}} {
				b.Run(kernel.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						result, err := kernel.call()
						if err != nil {
							b.Fatal(err)
						}
						blobBenchmarkResult = result
					}
				})
			}
		})
	}
}

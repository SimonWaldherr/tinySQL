package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

var vectorCodecStringSink string
var vectorCodecVectorSink []float64

func BenchmarkVectorCodecs(b *testing.B) {
	for _, dim := range []int{3, 128, 768, 1536} {
		vec := make([]float64, dim)
		for i := range vec {
			vec[i] = math.Sin(float64(i))
		}
		encodedJSON, _ := json.Marshal(vec)
		jsonText := string(encodedJSON)
		hexText, _ := vectorToHex(vec)
		b.Run(fmt.Sprintf("Dim%d", dim), func(b *testing.B) {
			b.Run("FromJSON/Unmarshal", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var out []float64
					if err := json.Unmarshal([]byte(jsonText), &out); err != nil {
						b.Fatal(err)
					}
					vectorCodecVectorSink = out
				}
			})
			b.Run("FromJSON/Direct", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					vectorCodecVectorSink, err = parseVectorJSON(jsonText)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ToHex/BinaryThenFormat", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					buf := make([]byte, len(vec)*4)
					for i, v := range vec {
						binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(float32(v)))
					}
					vectorCodecStringSink = fmt.Sprintf("%x", buf)
				}
			})
			b.Run("ToHex/Direct", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					vectorCodecStringSink, err = vectorToHex(vec)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("FromHex/BinaryThenDecode", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					buf, err := previousVectorHexDecode(hexText)
					if err != nil {
						b.Fatal(err)
					}
					out := make([]float64, len(buf)/4)
					for i := range out {
						out[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:])))
					}
					vectorCodecVectorSink = out
				}
			})
			b.Run("FromHex/Direct", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var err error
					vectorCodecVectorSink, err = vectorFromHex(hexText)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// Keep the former decoder here so the control does not benefit from the new
// constant nibble lookup.
func previousVectorHexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi, ok1 := previousVectorHexNibble(s[i])
		lo, ok2 := previousVectorHexNibble(s[i+1])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("invalid hex character at position %d", i)
		}
		out[i/2] = hi<<4 | lo
	}
	return out, nil
}

func previousVectorHexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

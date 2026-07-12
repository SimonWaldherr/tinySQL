package pager

import "testing"

func FuzzUnmarshalRow(f *testing.F) {
	f.Add(MarshalRow([]any{[]byte{0x1f, 0x8b, 0x08}, "tile", nil}, nil))
	f.Add([]byte{0, 0})
	f.Add([]byte{1, 0, tagLongBytes, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = UnmarshalRow(data)
	})
}

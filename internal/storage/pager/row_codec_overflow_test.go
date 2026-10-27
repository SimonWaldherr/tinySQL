package pager

import (
	"encoding/binary"
	"testing"
)

func TestRowCodecRejectsOverflowingVectorLength(t *testing.T) {
	// On 32-bit architectures count*8 overflows to zero. No payload follows.
	data := []byte{1, 0, tagVecF64, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(data[3:], 1<<29)
	for name, decode := range map[string]func() error{
		"row":    func() error { _, err := UnmarshalRow(data); return err },
		"column": func() error { _, err := UnmarshalRowColumn(data, 0); return err },
		"bytes column after vector": func() error {
			data := append([]byte(nil), data...)
			data[0] = 2
			data = append(data, tagBytes, 0, 0)
			_, err := unmarshalRowBytesColumn(data, 1, true)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := decode(); err == nil {
				t.Fatal("accepted truncated vector")
			}
		})
	}
}

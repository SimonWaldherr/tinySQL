package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"testing"
)

type failedDiskWriter struct{}

func (failedDiskWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestDiskCompressionReuseAfterFailure(t *testing.T) {
	backend := &DiskBackend{gzip: true, format: diskFormatGob}
	table := benchTable("pool", 32)
	if err := backend.encodeTableInto(failedDiskWriter{}, tableToDisk("default", table)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write failure: %v", err)
	}
	for i := 0; i < 3; i++ {
		table.Rows[0][1] = fmt.Sprintf("fresh-%d", i)
		var encoded bytes.Buffer
		if err := backend.encodeTableInto(&encoded, tableToDisk("default", table)); err != nil {
			t.Fatal(err)
		}
		reader, err := gzip.NewReader(bytes.NewReader(encoded.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		var decoded diskTable
		if err := gob.NewDecoder(reader).Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		// Read through the trailer, checking CRC and complete stream termination.
		if _, err := io.Copy(io.Discard, reader); err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		if decoded.Rows[0][1] != table.Rows[0][1] {
			t.Fatal(decoded.Rows[0])
		}
	}
}

func TestDiskIOPoolOwnership(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, jsonFormat := range []bool{false, true} {
			t.Run(fmt.Sprintf("gzip=%v/json=%v", compressed, jsonFormat), func(t *testing.T) {
				t.Parallel()
				var backend *DiskBackend
				var err error
				if jsonFormat {
					backend, err = NewJSONBackend(t.TempDir(), compressed)
				} else {
					backend, err = NewDiskBackend(t.TempDir(), compressed)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer backend.Close()
				table := benchTable("owned", 100)
				if err := backend.SaveTable("default", table); err != nil {
					t.Fatal(err)
				}
				retained, err := backend.LoadTable("default", "owned")
				if err != nil {
					t.Fatal(err)
				}
				table.Rows[0][1] = "changed"
				table.Version++
				if err := backend.SaveTable("default", table); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 4; i++ {
					got, err := backend.LoadTable("default", "owned")
					if err != nil {
						t.Fatal(err)
					}
					if got.Rows[0][1] != "changed" {
						t.Fatal(got.Rows[0])
					}
				}
				if retained.Rows[0][1] != "user_0" {
					t.Fatalf("retained result changed: %v", retained.Rows[0])
				}
			})
		}
	}
}

func TestDiskIOPoolEncryptedGzip(t *testing.T) {
	backend, err := NewDiskBackend(t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	encryptor, err := NewEncryptor(bytes.Repeat([]byte{0x31}, EncryptionKeySize))
	if err != nil {
		t.Fatal(err)
	}
	backend.SetEncryptor(encryptor)
	table := benchTable("encrypted_pool", 32)
	for i := 0; i < 3; i++ {
		want := fmt.Sprintf("encrypted-version-%d", i)
		table.Rows[0][1] = want
		table.Version++
		if err := backend.SaveTable("default", table); err != nil {
			t.Fatal(err)
		}
		got, err := backend.LoadTable("default", table.Name)
		if err != nil {
			t.Fatal(err)
		}
		if got.Rows[0][1] != want {
			t.Fatal(got.Rows[0])
		}
	}
}

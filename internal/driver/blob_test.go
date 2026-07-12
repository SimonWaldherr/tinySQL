package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"path/filepath"
	"testing"
)

// TestBlobRoundTripThroughDriver covers the public driver boundary without
// relying on string/base64 transport. It intentionally exercises NULL, an
// empty BLOB, a non-empty binary payload and text in distinct columns.
func TestBlobRoundTripThroughDriver(t *testing.T) {
	d := &drv{}
	raw, err := d.Open("mem://?tenant=blob")
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, `CREATE TABLE blobs (id INT, data BLOB, label TEXT)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	payload := []byte{0x1f, 0x8b, 0x08, 0x00, 0xff, 0x00, 0x7f}
	if _, err := c.ExecContext(ctx, `INSERT INTO blobs VALUES (?, ?, ?)`, []driver.NamedValue{
		{Ordinal: 1, Value: int64(1)}, {Ordinal: 2, Value: payload}, {Ordinal: 3, Value: "compressed tile"},
	}); err != nil {
		t.Fatalf("insert payload: %v", err)
	}
	if _, err := c.ExecContext(ctx, `INSERT INTO blobs VALUES (?, ?, ?)`, []driver.NamedValue{
		{Ordinal: 1, Value: int64(2)}, {Ordinal: 2, Value: []byte{}}, {Ordinal: 3, Value: "empty"},
	}); err != nil {
		t.Fatalf("insert empty: %v", err)
	}
	if _, err := c.ExecContext(ctx, `INSERT INTO blobs VALUES (?, ?, ?)`, []driver.NamedValue{
		{Ordinal: 1, Value: int64(3)}, {Ordinal: 2, Value: nil}, {Ordinal: 3, Value: "null"},
	}); err != nil {
		t.Fatalf("insert null: %v", err)
	}
	if _, err := c.ExecContext(ctx, `INSERT INTO blobs VALUES (4, 'text is not a blob', 'bad')`, nil); err == nil {
		t.Fatal("text INSERT into BLOB column unexpectedly succeeded")
	}

	rows, err := c.QueryContext(ctx, `SELECT data, label FROM blobs WHERE id = ?`, []driver.NamedValue{{Ordinal: 1, Value: int64(1)}})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	values := make([]driver.Value, 2)
	if err := rows.Next(values); err != nil {
		t.Fatalf("next: %v", err)
	}
	got, ok := values[0].([]byte)
	if !ok {
		t.Fatalf("BLOB scan type = %T, want []byte", values[0])
	}
	if !bytes.Equal(got, payload) || sha256.Sum256(got) != sha256.Sum256(payload) {
		t.Fatalf("payload changed: got %x want %x", got, payload)
	}
	if values[1] != "compressed tile" {
		t.Fatalf("text changed: %#v", values[1])
	}

	for _, tc := range []struct {
		id   int64
		want []byte
		null bool
	}{{2, []byte{}, false}, {3, nil, true}} {
		rows, err := c.QueryContext(ctx, `SELECT data FROM blobs WHERE id = ?`, []driver.NamedValue{{Ordinal: 1, Value: tc.id}})
		if err != nil {
			t.Fatal(err)
		}
		one := make([]driver.Value, 1)
		if err := rows.Next(one); err != nil {
			t.Fatal(err)
		}
		if tc.null {
			if one[0] != nil {
				t.Fatalf("id %d = %#v, want NULL", tc.id, one[0])
			}
			continue
		}
		b, ok := one[0].([]byte)
		if !ok || !bytes.Equal(b, tc.want) {
			t.Fatalf("id %d = %#v, want empty BLOB", tc.id, one[0])
		}
	}
}

func TestBlobPersistsAcrossDiskAndHybridBackends(t *testing.T) {
	for _, mode := range []string{"disk", "hybrid"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tiny")
			dsn := "file:" + path + "?tenant=blob&mode=" + mode
			first := &drv{}
			raw, err := first.Open(dsn)
			if err != nil {
				t.Fatal(err)
			}
			c := raw.(*conn)
			ctx := context.Background()
			if _, err := c.ExecContext(ctx, `CREATE TABLE tiles (id INT, data BLOB)`, nil); err != nil {
				t.Fatal(err)
			}
			want := []byte{0x1f, 0x8b, 0x08, 0xff, 0x00}
			if _, err := c.ExecContext(ctx, `INSERT INTO tiles VALUES (?, ?)`, []driver.NamedValue{{Ordinal: 1, Value: int64(1)}, {Ordinal: 2, Value: want}}); err != nil {
				t.Fatal(err)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}

			second := &drv{}
			raw, err = second.Open(dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			rows, err := raw.(*conn).QueryContext(ctx, `SELECT data FROM tiles WHERE id = 1`, nil)
			if err != nil {
				t.Fatal(err)
			}
			values := make([]driver.Value, 1)
			if err := rows.Next(values); err != nil {
				t.Fatal(err)
			}
			got, ok := values[0].([]byte)
			if !ok || !bytes.Equal(got, want) {
				t.Fatalf("reopened BLOB = %#v, want %x", values[0], want)
			}
		})
	}
}

func TestBlobLiteralRejectsInvalidHex(t *testing.T) {
	d := &drv{}
	raw, err := d.Open("mem://?tenant=blob-invalid")
	if err != nil {
		t.Fatal(err)
	}
	c := raw.(*conn)
	if _, err := c.ExecContext(context.Background(), `CREATE TABLE blobs (data BLOB)`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(context.Background(), `INSERT INTO blobs VALUES (X'abc')`, nil); err == nil {
		t.Fatal("invalid odd-length BLOB literal unexpectedly succeeded")
	}
}

package driver

import (
	"context"
	"testing"
)

func BenchmarkOpenInMemoryPool(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		db, err := OpenInMemory("Gruppe Süd & Nord")
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestOpenInMemoryPreservesTenant(t *testing.T) {
	for _, tc := range []struct{ tenant, want string }{
		{"", "default"}, {"team&read_only=1", "team&read_only=1"}, {"a+b", "a+b"},
		{"a%25#?=b", "a%25#?=b"}, {" Gruppe Süd ", "Gruppe Süd"},
	} {
		tenant := tc.tenant
		t.Run(tenant, func(t *testing.T) {
			db, err := OpenInMemory(tenant)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.Raw(func(raw any) error {
				actual := raw.(*conn)
				if actual.tenant != tc.want {
					t.Errorf("tenant=%q, want %q", actual.tenant, tc.want)
				}
				if actual.srv.db.IsReadOnly() {
					t.Error("tenant injected a DSN option")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if db, err := OpenInMemory("  "); err == nil || db != nil {
		t.Fatalf("whitespace tenant returned %v, %v", db, err)
	}
}

package driver

import (
	"context"
	"testing"
)

func TestOpenInMemoryEscapesTenant(t *testing.T) {
	for _, tenant := range []string{"team&read_only=1", "a+b", "a%25#?=b", "Gruppe Süd"} {
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
				if actual.tenant != tenant {
					t.Errorf("tenant=%q, want %q", actual.tenant, tenant)
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
}

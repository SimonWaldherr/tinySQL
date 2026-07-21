package storage

import (
	"strings"
	"testing"
)

func TestSuggestSimilar(t *testing.T) {
	cases := []struct {
		name       string
		target     string
		candidates []string
		want       string
	}{
		{"typo match", "usres", []string{"users", "orders"}, "users"},
		{"case insensitive", "USRES", []string{"users"}, "users"},
		{"exact match excluded", "users", []string{"users"}, ""},
		{"too different", "foobarbaz", []string{"users", "orders"}, ""},
		{"no candidates", "usres", nil, ""},
		{"empty target", "", []string{"users"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := suggestSimilar(c.target, c.candidates)
			if got != c.want {
				t.Fatalf("suggestSimilar(%q, %v) = %q, want %q", c.target, c.candidates, got, c.want)
			}
		})
	}
}

func TestGetNoSuchTableSuggestsClosestName(t *testing.T) {
	db := NewDB()
	table := NewTable("users", []Column{{Name: "id", Type: IntType}}, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	_, err := db.Get("default", "usres")
	if err == nil {
		t.Fatal("expected error for missing table")
	}
	if !strings.Contains(err.Error(), `did you mean "users"?`) {
		t.Fatalf("expected suggestion in error, got: %v", err)
	}

	_, err = db.Get("default", "completely_unrelated_name")
	if err == nil {
		t.Fatal("expected error for missing table")
	}
	if strings.Contains(err.Error(), "did you mean") {
		t.Fatalf("expected no suggestion for unrelated name, got: %v", err)
	}
}

func TestDropNoSuchTableSuggestsClosestName(t *testing.T) {
	db := NewDB()
	table := NewTable("orders", []Column{{Name: "id", Type: IntType}}, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err := db.Drop("default", "ordrs")
	if err == nil {
		t.Fatal("expected error dropping missing table")
	}
	if !strings.Contains(err.Error(), `did you mean "orders"?`) {
		t.Fatalf("expected suggestion in error, got: %v", err)
	}
}

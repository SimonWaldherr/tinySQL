package main

import "testing"

// TestIsValidTableName guards against regressing the SQLColumns table-name
// validation: any caller-supplied name that isn't a bare identifier (or one
// schema-qualified with a single dot) must be rejected before it can reach
// the fmt.Sprintf-built "SELECT * FROM %s LIMIT 0" query.
func TestIsValidTableName(t *testing.T) {
	valid := []string{
		"customers",
		"orders",
		"_private",
		"table1",
		"schema.customers",
		"public.orders",
		"a.b",
	}
	for _, name := range valid {
		if !isValidTableName(name) {
			t.Errorf("isValidTableName(%q) = false, want true", name)
		}
	}

	invalid := []string{
		"",
		"customers; DROP TABLE customers;--",
		"customers WHERE 1=1",
		"customers ",
		" customers",
		"customers\"",
		"customers'",
		"cust\"omers",
		"table--comment",
		"a.b.c",
		"1customers",
		"customers/*comment*/",
		"customers)",
		"(select 1)",
	}
	for _, name := range invalid {
		if isValidTableName(name) {
			t.Errorf("isValidTableName(%q) = true, want false", name)
		}
	}
}

// TestQuoteTableName ensures identifiers (and schema-qualified identifiers)
// are quoted per-part so the resulting SQL text cannot be reinterpreted as
// anything other than a single identifier reference.
func TestQuoteTableName(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"customers", `"customers"`},
		{"schema.customers", `"schema"."customers"`},
		{`weird"name`, `"weird""name"`},
	}
	for _, c := range cases {
		if got := quoteTableName(c.name); got != c.want {
			t.Errorf("quoteTableName(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

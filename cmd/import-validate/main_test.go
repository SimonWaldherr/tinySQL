package main

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

func TestValidateRejectsDuplicateEmailAndCommitWritesValidImport(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=import-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	bad, issues, err := a.validateCSV(ctx, "bad.csv", "contacts", `[{"column":"email","required":true,"type":"email","unique":true}]`, strings.NewReader("name,email\nAda,ada@example.test\nBea,ada@example.test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if bad.Status != "rejected" || len(issues) != 1 {
		t.Fatalf("bad import = %#v, %#v", bad, issues)
	}
	good, issues, err := a.validateCSV(ctx, "good.csv", "contacts", `[{"column":"email","required":true,"type":"email","unique":true}]`, strings.NewReader("name,email\nAda,ada@example.test\n"))
	if err != nil || len(issues) != 0 {
		t.Fatalf("good import: %#v %v", issues, err)
	}
	if _, err := a.commit(ctx, strconv.FormatInt(good.ID, 10)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contacts").Scan(&n); err != nil || n != 1 {
		t.Fatalf("contacts: %d, %v", n, err)
	}
}

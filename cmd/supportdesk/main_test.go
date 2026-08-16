package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunShowsSupportDeskWorkflow(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), "password", &out); err != nil {
		t.Fatalf("run support desk: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Open tickets (view): 2 | Audit events (trigger): 3",
		"Knowledge-base coverage (CTE):",
		"Full-text search for \"password\":",
		"Reset your password [Account]",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
}

func TestRunRejectsBlankQuery(t *testing.T) {
	if err := run(context.Background(), "  ", &bytes.Buffer{}); err == nil {
		t.Fatal("expected blank query to fail")
	}
}

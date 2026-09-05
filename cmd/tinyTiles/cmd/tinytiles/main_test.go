//go:build sqliteimport && !js && !wasm && !baremetal

package main

import (
	"bytes"
	"testing"
)

func TestRunHelpVersionAndUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 || stdout.String() != version+"\n" {
		t.Fatalf("version code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if code := run([]string{"help"}, &stdout, &stderr); code != 0 || !bytes.Contains(stdout.Bytes(), []byte("tinytiles import")) {
		t.Fatalf("help code=%d stdout=%q", code, stdout.String())
	}
	stderr.Reset()
	if code := run([]string{"missing"}, &stdout, &stderr); code != 2 || !bytes.Contains(stderr.Bytes(), []byte("unknown command")) {
		t.Fatalf("unknown command code=%d stderr=%q", code, stderr.String())
	}
}

func TestParseSchema(t *testing.T) {
	for _, value := range []string{"auto", "flat", "normalized"} {
		if _, err := parseSchema(value); err != nil {
			t.Fatalf("parse %q: %v", value, err)
		}
	}
	if _, err := parseSchema("invalid"); err == nil {
		t.Fatal("invalid schema accepted")
	}
}

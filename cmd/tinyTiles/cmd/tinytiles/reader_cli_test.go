//go:build !sqliteimport && !js && !wasm && !baremetal

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestReaderOnlyCLILeavesImportCommandsUnavailable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"import", "source.mbtiles", "region.ttiles"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "sqliteimport") {
		t.Fatalf("import code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"tile", "region.ttiles", "8", "x", "1"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "must be integers") {
		t.Fatalf("reader tile code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"validate", "missing.ttiles"}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "artifact is not complete") {
		t.Fatalf("reader validate code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

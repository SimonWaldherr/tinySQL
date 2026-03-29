package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestBuildQueryFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := filepath.Join(os.TempDir(), "tiny_query_files_bin")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, ".")
	cmd.Env = os.Environ()
	if outp, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		t.Fatalf("go build failed: %v\n%s", err, string(outp))
	}
	_ = os.Remove(out)
}

func TestParseDelimiterSpec(t *testing.T) {
	tests := []struct {
		in   string
		want []rune
		err  bool
	}{
		{"auto", nil, false},
		{"", nil, false},
		{"comma", []rune{','}, false},
		{";", []rune{';'}, false},
		{"tab", []rune{'\t'}, false},
		{`\t`, []rune{'\t'}, false},
		{"bad", nil, true},
	}

	for _, tc := range tests {
		got, err := parseDelimiterSpec(tc.in)
		if (err != nil) != tc.err {
			t.Fatalf("parseDelimiterSpec(%q) err=%v wantErr=%v", tc.in, err, tc.err)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("parseDelimiterSpec(%q)=%v want=%v", tc.in, got, tc.want)
		}
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"Users", "users"},
		{"user-data.csv", "user_data_csv"},
		{"123report", "t_123report"},
		{"  ???  ", ""},
	}
	for _, tc := range tests {
		if got := sanitizeTableName(tc.in); got != tc.want {
			t.Fatalf("sanitizeTableName(%q)=%q want=%q", tc.in, got, tc.want)
		}
	}
}

func TestExpandInputPath_Directory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.csv"), []byte("id\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.json"), []byte("[{\"id\":1}]"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "skip.xml"), []byte("<x/>"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, err := expandInputPath(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 supported files, got %d (%v)", len(files), files)
	}
}

func TestCollectLoadJobs_WithExplicitTable(t *testing.T) {
	dir := t.TempDir()
	f1 := filepath.Join(dir, "a.csv")
	f2 := filepath.Join(dir, "b.json")
	if err := os.WriteFile(f1, []byte("id\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f2, []byte("[{\"id\":1}]"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := collectLoadJobs([]string{f1, f2}, "same"); err == nil {
		t.Fatal("expected error when -table used with multiple files")
	}
}

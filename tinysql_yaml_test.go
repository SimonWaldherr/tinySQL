//go:build !tinysql_minimal

package tinysql_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestPublicImportYAMLWrapper(t *testing.T) {
	db := tsql.NewDB()
	if _, err := tsql.ImportYAML(context.Background(), db, "default", "yaml_public", strings.NewReader("- id: 1\n"), &tsql.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportYAML failed: %v", err)
	}
}

func ExampleImportFile_yaml() {
	dir, err := os.MkdirTemp("", "tinysql-import-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "people.yaml")
	data := "- id: 1\n  name: Alice\n- id: 2\n  name: Bob\n"
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		panic(err)
	}

	db := tsql.NewDB()
	result, err := tsql.ImportFile(context.Background(), db, "default", "people", file,
		&tsql.ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		panic(err)
	}

	fmt.Println(result.RowsInserted)
	// Output: 2
}

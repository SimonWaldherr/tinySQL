//go:build !tinysql_minimal

package tinysql_test

import (
	"context"
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

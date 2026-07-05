package importer

import (
	"context"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestImportCSVWrapper(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()

	res, err := ImportCSV(ctx, db, "default", "people", strings.NewReader("id,name\n1,Ada\n"), &ImportOptions{
		CreateTable: true,
		HeaderMode:  "first",
	})
	if err != nil {
		t.Fatalf("ImportCSV: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d", res.RowsInserted)
	}
}

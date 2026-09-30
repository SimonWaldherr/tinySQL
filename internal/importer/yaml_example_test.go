//go:build !tinysql_minimal

package importer

import (
	"context"
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ExampleImportFile_yaml() {
	ctx := context.Background()
	db := storage.NewDB()
	yamlData := "- id: 1\n  name: Alice\n- id: 2\n  name: Bob\n"

	result, err := ImportYAML(ctx, db, "default", "people", strings.NewReader(yamlData),
		&ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		panic(err)
	}

	fmt.Println(result.RowsInserted)
	// Output: 2
}

package tinysql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

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

	db := NewDB()
	result, err := ImportFile(context.Background(), db, "default", "people", file,
		&ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		panic(err)
	}

	fmt.Println(result.RowsInserted)
	// Output: 2
}

func ExampleImportFile_xml() {
	dir, err := os.MkdirTemp("", "tinysql-import-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "people.xml")
	data := `<root><record id="1" name="Alice"/><record id="2" name="Bob"/></root>`
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		panic(err)
	}

	db := NewDB()
	result, err := ImportFile(context.Background(), db, "default", "people", file,
		&ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		panic(err)
	}

	fmt.Println(result.RowsInserted)
	// Output: 2
}

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func Example_importFileToTinySQL_yaml() {
	dir, err := os.MkdirTemp("", "tinysql-migrate-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "people.yaml")
	if err := os.WriteFile(file, []byte("- id: 1\n  name: Alice\n- id: 2\n  name: Bob\n"), 0600); err != nil {
		panic(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()
	if err := importFileToTinySQL(db, ctx, "default", file, "people", true, false); err != nil {
		panic(err)
	}

	table, err := db.Get("default", "people")
	if err != nil {
		panic(err)
	}
	fmt.Println(len(table.Rows))
	// Output: 2
}

func Example_importFileToTinySQL_xml() {
	dir, err := os.MkdirTemp("", "tinysql-migrate-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "people.xml")
	data := `<root><record id="1" name="Alice"/><record id="2" name="Bob"/></root>`
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		panic(err)
	}

	ctx := context.Background()
	db := tinysql.NewDB()
	if err := importFileToTinySQL(db, ctx, "default", file, "people", true, false); err != nil {
		panic(err)
	}

	table, err := db.Get("default", "people")
	if err != nil {
		panic(err)
	}
	fmt.Println(len(table.Rows))
	// Output: 2
}

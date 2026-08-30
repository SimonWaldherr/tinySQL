package main

import (
	"context"
	"fmt"
	"log"

	tinysql "github.com/SimonWaldherr/tinySQL"
	proceduredemo "github.com/SimonWaldherr/tinySQL/demos/procedures"
)

func main() {
	if err := proceduredemo.Register(); err != nil {
		log.Fatal(err)
	}
	db := tinysql.NewDB()
	ctx := context.Background()
	queries := []string{
		`CALL demo_geo_distance(52.5200, 13.4050, 48.1372, 11.5755)`,
		`CALL demo_find_functions('RAG%')`,
		`CALL demo_log_event('demo', 'stored procedure executed safely')`,
		`CALL demo_table_summary()`,
		`SELECT name, read_only, atomic, calls, errors FROM sys.procedures ORDER BY name`,
	}
	for _, query := range queries {
		result, err := tinysql.Execute(ctx, db, "default", tinysql.MustParseSQL(query))
		if err != nil {
			log.Fatalf("%s: %v", query, err)
		}
		fmt.Printf("\n%s\n", query)
		for _, row := range result.Rows {
			fmt.Println(row)
		}
	}
}

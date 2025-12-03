package main

import (
	"context"
	"fmt"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func main() {
	db := tinysql.NewDB()

	p := tinysql.NewParser(`CREATE TABLE test_bool (id INT, flag BOOL)`)
	st, _ := p.ParseStatement()
	if _, err := tinysql.Execute(context.Background(), db, "default", st); err != nil {
		fmt.Println("create err", err)
		return
	}

	p = tinysql.NewParser(`INSERT INTO test_bool VALUES (1, true)`)
	st, _ = p.ParseStatement()
	if _, err := tinysql.Execute(context.Background(), db, "default", st); err != nil {
		fmt.Println("insert1 err", err)
		return
	}

	p = tinysql.NewParser(`INSERT INTO test_bool VALUES (2, false)`)
	st, _ = p.ParseStatement()
	if _, err := tinysql.Execute(context.Background(), db, "default", st); err != nil {
		fmt.Println("insert2 err", err)
		return
	}

	p = tinysql.NewParser(`SELECT * FROM test_bool ORDER BY id`)
	st, _ = p.ParseStatement()
	rs, err := tinysql.Execute(context.Background(), db, "default", st)
	if err != nil {
		fmt.Println("select err", err)
		return
	}

	fmt.Println("Cols:", rs.Cols)
	for i, r := range rs.Rows {
		fmt.Printf("Row %d keys: %v\n", i, r)
		if v, ok := tinysql.GetVal(r, "test_bool.flag"); ok {
			fmt.Printf("GetVal(test_bool.flag) -> %v\n", v)
		}
		if v, ok := tinysql.GetVal(r, "flag"); ok {
			fmt.Printf("GetVal(flag) -> %v\n", v)
		}
	}
}

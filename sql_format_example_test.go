package tinysql_test

import (
	"fmt"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func ExampleBeautifySQL() {
	query := "select id,name from users where status = 'active' and id=42"

	fmt.Println(tinysql.BeautifySQL(query))
	fmt.Println("---")
	fmt.Println(tinysql.MinifySQL(query))

	// Output:
	// SELECT id, name
	// FROM users
	// WHERE status = 'active'
	// AND id = 42
	// ---
	// select id,name from users where status='active' and id=42
}

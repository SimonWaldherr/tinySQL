package tinysql_test

import (
	tinysql "github.com/SimonWaldherr/tinySQL"
	"testing"
)

func TestPublicIndexAdvisor(t *testing.T) {
	db := tinysql.NewDB()
	defer db.Close()
	advisor, err := tinysql.NewIndexAdvisor(db, tinysql.IndexAdvisorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tinysql.ParseSQL("SELECT 1 AS n")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advisor.Execute(t.Context(), "default", stmt); err != nil {
		t.Fatal(err)
	}
	advisor.SetAutoCreate(true)
	if len(advisor.Recommendations()) != 0 {
		t.Fatal("unexpected recommendation")
	}
}

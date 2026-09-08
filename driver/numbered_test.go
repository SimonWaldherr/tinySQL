package driver_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/testutil"
)

func TestNumberedPreparedIsolation(t *testing.T) {
	db := testutil.Open(t)
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			const query = "SELECT $2 AS label, $1 AS id, $2 AS echoed"
			run := func(id int, label string) *sql.Row { return db.QueryRow(query, id, label) }
			if prepared {
				stmt, err := db.Prepare(query)
				if err != nil {
					t.Fatal(err)
				}
				defer stmt.Close()
				run = func(id int, label string) *sql.Row { return stmt.QueryRow(id, label) }
			}
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 40; i++ {
						want := worker*100 + i
						label := fmt.Sprintf("worker '%d' ?", want)
						var id int
						var got, echo string
						if err := run(want, label).Scan(&got, &id, &echo); err != nil || id != want || got != label || echo != label {
							t.Errorf("id=%d label=%q echo=%q err=%v", id, got, echo, err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
	payload := []byte{1, 2, 3}
	var first, second []byte
	if err := db.QueryRow("SELECT $1 AS a, $1 AS b", payload).Scan(&first, &second); err != nil {
		t.Fatal(err)
	}
	payload[0] = 9
	first[1] = 9
	if !bytes.Equal(second, []byte{1, 2, 3}) {
		t.Fatalf("aliased BLOB result: %v", second)
	}
	var literal, value string
	if err := db.QueryRow("SELECT '__tinysql_prepared_param_0__', $1", "bound").Scan(&literal, &value); err != nil || literal != "__tinysql_prepared_param_0__" || value != "bound" {
		t.Fatalf("marker collision: %q %q %v", literal, value, err)
	}
}

package testutil_test

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/testutil"
)

func TestOpenIsolationAndCleanup(t *testing.T) {
	var opened []*sql.DB
	for i := 0; i < 2; i++ {
		t.Run("same schema", func(t *testing.T) {
			db := testutil.Open(t, "CREATE TABLE items (id INT PRIMARY KEY, label TEXT)", fmt.Sprintf("INSERT INTO items VALUES (1, 'item-%d')", i))
			opened = append(opened, db)
			var label string
			if err := db.QueryRow("SELECT label FROM items WHERE id = ?", 1).Scan(&label); err != nil || label != fmt.Sprintf("item-%d", i) {
				t.Fatalf("label=%q err=%v", label, err)
			}
			// Force two physical connections; both must see the same private DB.
			a, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			var n int
			if err := b.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM items").Scan(&n); err != nil || n != 1 {
				t.Fatalf("count=%d err=%v", n, err)
			}
		})
	}
	for _, db := range opened {
		if err := db.Ping(); err == nil {
			t.Fatal("pool remained open after cleanup")
		}
	}
}

func TestOpenParallel(t *testing.T) {
	for i := 0; i < 8; i++ {
		t.Run("isolated", func(t *testing.T) {
			t.Parallel()
			db := testutil.Open(t, "CREATE TABLE items (id INT)", fmt.Sprintf("INSERT INTO items VALUES (%d)", i))
			var id int
			if err := db.QueryRow("SELECT id FROM items").Scan(&id); err != nil || id != i {
				t.Fatalf("id=%d err=%v", id, err)
			}
		})
	}
}

func TestOpenFixtureFailure(t *testing.T) {
	if os.Getenv("TINYSQL_TEST_FIXTURE_FAILURE") == "1" {
		t.Cleanup(func() { fmt.Println("fixture cleanup ran") })
		openFailedFixture(t)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestOpenFixtureFailure$")
	cmd.Env = append(os.Environ(), "TINYSQL_TEST_FIXTURE_FAILURE=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "tinySQL fixture 2:") || !strings.Contains(string(out), "fixture cleanup ran") {
		t.Fatalf("expected fixture failure and cleanup: %v\n%s", err, out)
	}
}

func openFailedFixture(t *testing.T) {
	t.Helper()
	testutil.Open(t, "CREATE TABLE items (id INT)", "THIS IS NOT SQL")
}

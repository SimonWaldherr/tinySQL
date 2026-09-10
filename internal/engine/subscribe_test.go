package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSubscriptionRefreshUnchangedProjection(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	exec := func(query string) {
		t.Helper()
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE items (id INT, payload BLOB, hidden INT)")
	exec("INSERT INTO items VALUES (1, X'01', 0), (2, X'02', 0)")
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse("SELECT id, payload FROM items").(*Select), rows: make(map[int]Row)}
	initial, err := state.refresh(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	initial.Added[0]["payload"].([]byte)[0] = 255
	for _, full := range []bool{false, true} {
		before := state.rows[0]
		exec("UPDATE items SET hidden = hidden + 1 WHERE id = 1")
		if full {
			state.structural = -1
		}
		delta, err := state.refresh(t.Context())
		if err != nil || delta != nil {
			t.Fatalf("unchanged projection: %v, %v", delta, err)
		}
		if reflect.ValueOf(before).Pointer() != reflect.ValueOf(state.rows[0]).Pointer() {
			t.Fatal("unchanged snapshot was replaced")
		}
	}
	exec("UPDATE items SET payload = X'03' WHERE id = 1")
	delta, err := state.refresh(t.Context())
	if err != nil || delta == nil {
		t.Fatalf("update: %v, %v", delta, err)
	}
	if delta.Removed[0]["payload"].([]byte)[0] != 1 {
		t.Fatal("reader mutated snapshot")
	}
	delta.Added[0]["payload"].([]byte)[0] = 254
	delta.Removed[0]["payload"].([]byte)[0] = 253
	exec("DELETE FROM items WHERE id = 1")
	delta, err = state.refresh(t.Context())
	if err != nil || delta == nil {
		t.Fatalf("delete: %v, %v", delta, err)
	}
	if delta.Removed[0]["payload"].([]byte)[0] != 3 {
		t.Fatal("reader mutated updated snapshot")
	}
}

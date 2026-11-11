package storage

import (
	"slices"
	"testing"
)

func TestBufferPoolReleasesEmptyTenants(t *testing.T) {
	for _, evict := range []bool{false, true} {
		name := "remove"
		if evict {
			name = "evict"
		}
		t.Run(name, func(t *testing.T) {
			bp := NewBufferPool(DefaultMemoryPolicy())
			table := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
			table.Rows = [][]any{{1}}
			for _, entry := range [][2]string{{"one", "a"}, {"one", "b"}, {"two", "c"}} {
				if err := bp.Put(entry[0], entry[1], table); err != nil {
					t.Fatal(err)
				}
			}
			for i, entry := range [][2]string{{"one", "a"}, {"one", "b"}, {"two", "c"}} {
				if evict {
					bp.mu.Lock()
					bp.evictLRU(EstimateTableSize(table))
					bp.mu.Unlock()
				} else {
					bp.Remove(entry[0], entry[1])
				}
				wantTenants := []int{2, 1, 0}[i]
				if len(bp.cache) != wantTenants {
					t.Fatalf("after removal %d: %d tenants, want %d", i+1, len(bp.cache), wantTenants)
				}
				if got := bp.GetStats().TablesInMemory; got != 2-i {
					t.Fatalf("resident tables = %d, want %d", got, 2-i)
				}
			}
			if bp.GetMemoryUsage() != 0 || bp.lru.size != 0 {
				t.Fatal("removed tables remain accounted or queued")
			}
			// A tenant removed by either path can be admitted again.
			if err := bp.Put("one", "a", table); err != nil {
				t.Fatal(err)
			}
			if got, ok := bp.Get("one", "a"); !ok || got != table {
				t.Fatal("re-admitted tenant is unavailable")
			}
		})
	}
}

func TestRBACRevocationReleasesDiscardedReferences(t *testing.T) {
	db := NewDB()
	cat := db.Catalog()
	roles := []string{"reader", "temporary", "writer", "TEMPORARY"}
	grants := []Grant{
		{Permission: PermSelect, Schema: "main", Table: "kept"},
		{Permission: PermUpdate, Schema: "main", Table: "discarded"},
		{Permission: PermDelete, Schema: "main", Table: "kept"},
		{Permission: PermUpdate, Schema: "main", Table: "discarded"},
	}
	cat.rbac.users["alice"] = &CatalogUser{Name: "alice", Roles: roles}
	cat.rbac.roles["reader"] = &CatalogRole{Name: "reader", Grants: grants}
	wantRoles := append([]string(nil), roles...)
	wantGrants := append([]Grant(nil), grants...)

	db.LockContentForWrite()
	defer db.UnlockContentForWrite()
	snap := db.SnapshotForStatement()
	if err := cat.RevokeRoleFromUser("alice", "temporary"); err != nil {
		t.Fatal(err)
	}
	if err := cat.RevokePermission("reader", PermUpdate, "main", "discarded"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cat.rbac.users["alice"].Roles, []string{"reader", "writer"}) ||
		!slices.Equal(cat.rbac.roles["reader"].Grants, []Grant{wantGrants[0], wantGrants[2]}) {
		t.Fatal("revocation changed surviving memberships or grants")
	}
	for i := 2; i < len(roles); i++ {
		if roles[i] != "" || grants[i] != (Grant{}) {
			t.Errorf("discarded slot %d retains role %q or grant %v", i, roles[i], grants[i])
		}
	}
	// Tail clearing must not alter the catalog before-image used by rollback.
	db.RestoreStatementSnapshot(snap)
	if !slices.Equal(db.Catalog().rbac.users["alice"].Roles, wantRoles) ||
		!slices.Equal(db.Catalog().rbac.roles["reader"].Grants, wantGrants) {
		t.Fatal("revocation modified the rollback snapshot")
	}
}

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRetrievalPreFilterRejectsNullAllowedIDs(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE null_acl_chunks (id INT PRIMARY KEY, tenant_id INT, body TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO null_acl_chunks VALUES
		(1,7,'needle','[1,0]'),(2,7,'needle','[0,1]'),(3,8,'needle','[0.5,0.5]')`)
	for _, function := range []string{
		`RAG_SEARCH('null_acl_chunks','embedding','[1,0]',3,'%s')`,
		`HYBRID_SEARCH('null_acl_chunks','embedding','body','needle','[1,0]',3,'%s')`,
		`VEC_SEARCH_FILTERED('null_acl_chunks','embedding','[1,0]',3,'%s')`,
		`FTS_SEARCH_FILTERED('null_acl_chunks','needle',3,'%s','body')`,
	} {
		for _, options := range []string{
			`{"pre_filter":{"allowed_row_ids":null}}`,
			`{"pre_filter":{"allowed_row_ids":null,"equals":{"tenant_id":7}}}`,
			`{"PRE_FILTER":{"AlLoWeD_RoW_iDs":null,"equals":{"tenant_id":7}}}`,
		} {
			query := "SELECT id FROM " + fmt.Sprintf(function, options)
			_, err := Execute(t.Context(), db, "default", mustParse(query))
			if err == nil || !strings.Contains(err.Error(), "allowed_row_ids must not be null") {
				t.Errorf("%s: got %v, want explicit NULL ACL rejection", query, err)
			}
		}
		for _, options := range []string{
			`{"pre_filter":{"allowed_row_ids":[]}}`,
			`{"pre_filter":{"allowed_row_ids":[],"equals":{"tenant_id":7}}}`,
		} {
			query := "SELECT id FROM " + fmt.Sprintf(function, options)
			if result := execSQL(t, db, query); len(result.Rows) != 0 {
				t.Errorf("%s: empty ACL returned %v", query, result.Rows)
			}
		}
		// Omitting the ID list still permits an independent metadata boundary.
		query := "SELECT id FROM " + fmt.Sprintf(function, `{"pre_filter":{"equals":{"tenant_id":7}}}`)
		if result := execSQL(t, db, query); len(result.Rows) != 2 {
			t.Errorf("%s: metadata-only restriction returned %v", query, result.Rows)
		}
	}
}

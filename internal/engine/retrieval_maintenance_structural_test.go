package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// structuralStep is one of the statements the random sequence of the differential
// test does not draw: the table is emptied, dropped and created again, scanned by
// a stream that is still open when rows are removed (the writer then swaps in a
// private copy of the table, which carries no search structures), or gets rows
// appended and deleted again before anyone searches (a removal that names only
// rows no structure covers).
func (h *retrievalDiffHarness) structuralStep() string {
	switch h.rng.Intn(4) {
	case 0:
		h.mustExec(`DELETE FROM docs`)
		h.model = map[int]*retrievalDiffRow{}
		h.annMoved = false
		h.insert(10 + h.rng.Intn(30))
		return "delete all"
	case 1:
		h.mustExec(`DROP TABLE docs`)
		h.mustExec(`CREATE TABLE docs (id INT, loaded_at INT, content TEXT, embedding VECTOR)`)
		h.model = map[int]*retrievalDiffRow{}
		h.annMoved = false
		h.insert(20 + h.rng.Intn(30))
		return "drop and create"
	case 2:
		stream, err := ExecuteStream(context.Background(), h.db, h.tenant, mustParse(`SELECT id, content FROM docs`))
		if err != nil {
			h.t.Fatalf("%s: opening a stream: %v", h.label, err)
		}
		stream.Next()
		m := 2 + h.rng.Intn(5)
		for _, id := range h.ids() {
			if id%m == 0 {
				delete(h.model, id)
			}
		}
		h.mustExec(fmt.Sprintf(`DELETE FROM docs WHERE id %% %d = 0`, m))
		stamp := 4000 + h.rng.Intn(1000)
		for _, row := range h.model {
			row.loaded = stamp
		}
		h.mustExec(fmt.Sprintf(`UPDATE docs SET loaded_at = %d`, stamp))
		h.insert(3)
		_ = stream.Close()
		h.annMoved = false
		h.followSeen = vecHNSWFollowCount.Load()
		return fmt.Sprintf("stream open during delete %%%d", m)
	default:
		h.mustExec(`SELECT * FROM FTS_WARM('docs', 'content')`)
		h.mustExec(`SELECT * FROM VEC_WARM('docs', 'embedding')`)
		h.mustExec(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
		first := h.nextID
		k := 2 + h.rng.Intn(6)
		h.insert(k)
		var ids []string
		for id := first; id < h.nextID; id++ {
			delete(h.model, id)
			ids = append(ids, fmt.Sprint(id))
		}
		h.mustExec(`DELETE FROM docs WHERE id IN (` + strings.Join(ids, ",") + `)`)
		return fmt.Sprintf("append %d and delete them again", k)
	}
}

// The differential test again, with the statements above mixed in.
func TestRetrievalMaintenanceMatchesColdBuildAcrossStructuralStatements(t *testing.T) {
	for _, seed := range []int64{21, 22, 23, 24} {
		t.Run(fmt.Sprintf("memory/seed%d", seed), func(t *testing.T) {
			runRetrievalDifferentialOpts(t, seed, 60, false, true)
		})
	}
	for _, seed := range []int64{31, 32} {
		t.Run(fmt.Sprintf("disk/seed%d", seed), func(t *testing.T) {
			runRetrievalDifferentialOpts(t, seed, 60, true, true)
		})
	}
}

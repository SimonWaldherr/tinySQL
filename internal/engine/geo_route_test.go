package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupRouteEdges(t *testing.T, db *storage.DB) {
	t.Helper()
	execSQL(t, db, `CREATE TABLE edges (edge_id TEXT, source TEXT, target TEXT, cost FLOAT64)`)
	rows := [][4]string{
		{"e1", "A", "B", "1"},
		{"e2", "B", "C", "2"},
		{"e3", "A", "C", "10"},
		{"e4", "C", "D", "1"},
	}
	for _, r := range rows {
		execSQL(t, db, `INSERT INTO edges (edge_id, source, target, cost) VALUES ('`+r[0]+`','`+r[1]+`','`+r[2]+`',`+r[3]+`)`)
	}
}

func TestRouteShortestPathPicksCheaperRoute(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)

	rs := execSQL(t, db, `SELECT * FROM ROUTE_SHORTEST_PATH('edges','source','target','cost','A','C')`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected a 3-node path (A,B,C), got %d rows: %v", len(rs.Rows), rs.Rows)
	}
	wantNodes := []string{"A", "B", "C"}
	for i, row := range rs.Rows {
		if row["node_id"] != wantNodes[i] {
			t.Errorf("step %d: node_id = %v, want %v", i, row["node_id"], wantNodes[i])
		}
	}
	if row0 := rs.Rows[0]; row0["edge_id"] != nil || row0["leg_cost"] != nil {
		t.Errorf("start node should have nil edge_id/leg_cost: %v", row0)
	}
	last := rs.Rows[len(rs.Rows)-1]
	total, ok := last["total_cost"].(float64)
	if !ok || total != 3 {
		t.Errorf("total_cost at destination: got %v, want 3 (via A->B->C, not the direct cost-10 edge)", last["total_cost"])
	}
}

func TestRouteDistanceMatchesPathCost(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','D') AS v`)
	f, ok := dist.(float64)
	if !ok || f != 4 { // A->B->C->D = 1+2+1
		t.Errorf("ROUTE_DISTANCE A->D: got %v, want 4", dist)
	}
}

func TestRouteDistanceUnreachable(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	execSQL(t, db, `INSERT INTO edges (edge_id, source, target, cost) VALUES ('e5','X','Y',1)`)
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','Y') AS v`)
	if dist != nil {
		t.Errorf("unreachable target should be NULL, got %v", dist)
	}
}

func TestRouteDistanceUnknownNode(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','NoSuchNode') AS v`)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','NoSuchNode','A') AS v`)
}

func TestRouteDirectionality(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE oneway (source TEXT, target TEXT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO oneway (source, target, cost) VALUES ('X','Y',5)`)

	if dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('oneway','source','target','cost','Y','X') AS v`); dist != nil {
		t.Errorf("directed graph should not allow reverse travel: got %v", dist)
	}
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('oneway','source','target','cost','Y','X','undirected') AS v`)
	if f, ok := dist.(float64); !ok || f != 5 {
		t.Errorf("undirected mode should allow reverse travel: got %v", dist)
	}
}

func TestRouteNegativeWeightRejected(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE bad (source TEXT, target TEXT, cost FLOAT64)`)
	execSQL(t, db, `INSERT INTO bad (source, target, cost) VALUES ('A','B',-1)`)
	execExpectError(t, db, `SELECT ROUTE_DISTANCE('bad','source','target','cost','A','B') AS v`)
}

func TestRouteSameStartAndEnd(t *testing.T) {
	db := storage.NewDB()
	setupRouteEdges(t, db)
	dist := execScalar(t, db, `SELECT ROUTE_DISTANCE('edges','source','target','cost','A','A') AS v`)
	if f, ok := dist.(float64); !ok || f != 0 {
		t.Errorf("distance from a node to itself should be 0, got %v", dist)
	}
}

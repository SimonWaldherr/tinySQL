//go:build !wasm

package main

import (
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestRuntimeMonitorTracksRequestsUsersAndDatabase(t *testing.T) {
	monitor := newRuntimeMonitor()
	monitor.setIdentity("alice", "session-a")
	query := monitor.begin("query")
	failedImport := monitor.begin("import")
	activeStatus := monitor.snapshot(nil, "default", nil)
	activeRequests := activeStatus["requests"].(map[string]interface{})
	if activeRequests["active"] != int64(2) || activeRequests["peakActive"] != int64(2) {
		t.Fatalf("active request status = %#v", activeRequests)
	}
	monitor.finish(query, true, false)
	monitor.finish(failedImport, false, true)
	monitor.setIdentity("bob", "session-b")
	secondUserQuery := monitor.begin("query")
	monitor.finish(secondUserQuery, true, false)

	db := tinysql.NewDB()
	cache := tinysql.NewQueryCache(8)
	status := monitor.snapshot(db, "default", cache)
	requests := status["requests"].(map[string]interface{})
	if requests["active"] != int64(0) || requests["total"] != int64(3) || requests["failed"] != int64(1) || requests["timedOut"] != int64(1) {
		t.Fatalf("request status = %#v", requests)
	}
	identity := status["identity"].(map[string]interface{})
	if identity["userId"] != "bob" || identity["distinctUsers"] != 2 {
		t.Fatalf("identity = %#v", identity)
	}
	users := status["users"].([]interface{})
	if len(users) != 2 {
		t.Fatalf("users = %#v", users)
	}
	userTotals := map[string]int64{}
	for _, rawUser := range users {
		user := rawUser.(map[string]interface{})
		userTotals[user["userId"].(string)] = user["totalRequests"].(int64)
	}
	if userTotals["alice"] != 2 || userTotals["bob"] != 1 {
		t.Fatalf("user totals = %#v", userTotals)
	}
}

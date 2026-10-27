package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

func TestTimeoutCreatesCriticalCheckoutAcrossMidnight(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=worklog-timeout")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db, timeout: 10 * time.Hour, location: time.UTC}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	work, err := a.activityTypeByName(ctx, db, workTypeName)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 15, 20, 0, 0, 0, time.UTC)
	if _, err := a.createStamp(ctx, stampInput{UserID: 1, TypeID: work.ID}, start); err != nil {
		t.Fatal(err)
	}
	created, err := a.autoCheckout(ctx, start.Add(10*time.Hour+time.Minute))
	if err != nil || created != 1 {
		t.Fatalf("auto checkout = %d, %v", created, err)
	}
	entries, err := a.entriesForUser(ctx, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].TypeName != offTypeName || !entries[0].Critical || entries[0].OccurredAt != start.Add(10*time.Hour).Unix() {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestReportSplitsNightShiftAtMidnight(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=worklog-report")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db, timeout: 12 * time.Hour, location: time.UTC}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	work, _ := a.activityTypeByName(ctx, db, workTypeName)
	off, _ := a.activityTypeByName(ctx, db, offTypeName)
	start := time.Date(2026, 8, 15, 20, 0, 0, 0, time.UTC)
	if _, err := a.createStamp(ctx, stampInput{UserID: 1, TypeID: work.ID}, start); err != nil {
		t.Fatal(err)
	}
	if _, err := a.createStamp(ctx, stampInput{UserID: 1, TypeID: off.ID}, start.Add(8*time.Hour)); err != nil {
		t.Fatal(err)
	}
	report, err := a.report(ctx, 1, start.Add(9*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 2 || report[0].Hours != 4 || report[1].Hours != 4 {
		t.Fatalf("report = %#v", report)
	}
}

func TestReportRangeIncludesSelectedDayOfOvernightShift(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=worklog-range")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db, timeout: 12 * time.Hour, location: time.UTC}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	work, err := a.activityTypeByName(ctx, db, workTypeName)
	if err != nil {
		t.Fatal(err)
	}
	off, err := a.activityTypeByName(ctx, db, offTypeName)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 15, 20, 0, 0, 0, time.UTC)
	if _, err := a.createStamp(ctx, stampInput{UserID: 1, TypeID: work.ID}, start); err != nil {
		t.Fatal(err)
	}
	if _, err := a.createStamp(ctx, stampInput{UserID: 1, TypeID: off.ID}, start.Add(8*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"user_id=1&from=2026-08-16&to=2026-08-16", 200},
		{"user_id=1&from=2026-08-17&to=2026-08-16", 400},
		{"user_id=1&from=2026-02-30", 400},
		{"user_id=999", 404}, {"user_id=0", 400},
	} {
		res := httptest.NewRecorder()
		a.handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/report?"+tc.query, nil))
		if res.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.query, res.Code, res.Body.String())
		}
		if tc.status == 200 {
			var body struct {
				Days []dayTotal `json:"days"`
			}
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Days) != 1 || body.Days[0].Date != "2026-08-16" || body.Days[0].Hours != 4 {
				t.Fatalf("report: %#v", body.Days)
			}
		}
	}
}

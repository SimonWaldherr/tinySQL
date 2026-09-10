package engine

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestGeoTimeWindowsAfterReopen(t *testing.T) {
	for _, mode := range []storage.StorageMode{storage.ModeDisk, storage.ModePagedIndex} {
		t.Run(mode.String(), func(t *testing.T) {
			cfg := storage.StorageConfig{Mode: mode, Path: filepath.Join(t.TempDir(), "db"), MaxMemoryBytes: 8 << 20}
			db, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, q := range []string{
				`CREATE TABLE sales (branch TEXT, time INT, cents INT)`,
				`CREATE INDEX sales_window ON sales(branch,time)`,
				`INSERT INTO sales VALUES ('a',9,100),('a',10,200),('a',10,300),('a',19,400),('a',20,500),('b',10,900)`,
				`CREATE TABLE logs (service TEXT, time INT, message TEXT)`,
				`CREATE INDEX log_window ON logs(service,time)`,
				`INSERT INTO logs VALUES ('api',-1,'old'),('api',0,'start'),('api',10,'ready'),('db',10,'ready')`,
				`CREATE TABLE poi (layer TEXT, lat FLOAT, lon FLOAT)`,
				`CREATE INDEX poi_window ON poi(layer,lat,lon)`,
				`INSERT INTO poi VALUES ('water',48.0,11.0),('water',48.5,11.5),('water',49.0,12.0),('shop',48.5,11.5)`,
			} {
				rangeExec(t, db, q)
			}
			queries := []string{
				`SELECT branch,SUM(cents) AS total,COUNT(*) AS count FROM sales WHERE branch='a' AND time>=10 AND time<20 GROUP BY branch`,
				`SELECT time,message FROM logs WHERE service='api' AND time>=0 AND time<10 ORDER BY time`,
				`SELECT lat,lon FROM poi WHERE layer='water' AND lat>=48.0 AND lat<49.0 AND lon>=11.0 AND lon<=11.5 ORDER BY lat`,
			}
			want := make([][]Row, len(queries))
			for i, q := range queries {
				want[i] = rangeExec(t, db, q).Rows
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for i, q := range queries {
				got := rangeExec(t, db, q).Rows
				gotJSON, err := storage.JSONMarshal(got)
				if err != nil {
					t.Fatal(err)
				}
				wantJSON, err := storage.JSONMarshal(want[i])
				if err != nil {
					t.Fatal(err)
				}
				if string(gotJSON) != string(wantJSON) {
					t.Fatalf("%s: %v != %v", q, got, want[i])
				}
			}
			rangeExec(t, db, `INSERT INTO sales VALUES ('a',15,700)`)
			got := rangeExec(t, db, queries[0]).Rows
			if len(got) != 1 || fmt.Sprint(got[0]["total"]) != "1600" || fmt.Sprint(got[0]["count"]) != "4" {
				t.Fatalf("new sale missing: %v", got)
			}
		})
	}
}

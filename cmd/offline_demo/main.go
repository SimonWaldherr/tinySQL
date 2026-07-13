// Command offline_demo is a small local-first tinySQL example.
//
// It creates a compact POI database, optionally persists it as a snapshot,
// reopens that snapshot on later runs, and serves a read-only text search. No
// network service, external database, or application-specific runtime is
// required.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

type config struct {
	snapshot string
	query    string
	json     bool
	rebuild  bool
	readOnly bool
}

type poi struct {
	ID       int     `json:"id"`
	Name     string  `json:"name"`
	Category string  `json:"category"`
	City     string  `json:"city"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
}

func main() {
	var cfg config
	flag.StringVar(&cfg.snapshot, "snapshot", "", "Optional snapshot path; created on first run and reused afterwards")
	flag.StringVar(&cfg.query, "query", "München", "Search name, city, or category")
	flag.BoolVar(&cfg.json, "json", false, "Write stable JSON instead of a text table")
	flag.BoolVar(&cfg.rebuild, "rebuild", false, "Ignore an existing snapshot and rebuild the sample dataset")
	flag.BoolVar(&cfg.readOnly, "read-only", true, "Reject writes after loading or creating the dataset")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := run(ctx, cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "offline_demo:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, out io.Writer) error {
	db, source, err := openPOIDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	if cfg.readOnly {
		db.SetReadOnly(true)
	}
	results, err := searchPOIs(ctx, db, cfg.query)
	if err != nil {
		return err
	}

	if cfg.json {
		return json.NewEncoder(out).Encode(struct {
			Source   string `json:"source"`
			ReadOnly bool   `json:"read_only"`
			Query    string `json:"query"`
			Results  []poi  `json:"results"`
		}{source, db.IsReadOnly(), cfg.query, results})
	}

	fmt.Fprintf(out, "Offline POI demo (%s, read-only=%t)\n", source, db.IsReadOnly())
	fmt.Fprintf(out, "Search: %q — %d result(s)\n\n", cfg.query, len(results))
	if len(results) == 0 {
		return nil
	}
	fmt.Fprintln(out, "ID  NAME                 CATEGORY       CITY")
	fmt.Fprintln(out, "--  -------------------  -------------  ----------")
	for _, p := range results {
		fmt.Fprintf(out, "%-2d  %-19s  %-13s  %s\n", p.ID, p.Name, p.Category, p.City)
	}
	return nil
}

func openPOIDatabase(ctx context.Context, cfg config) (*tinysql.DB, string, error) {
	if cfg.snapshot != "" && !cfg.rebuild {
		if _, err := os.Stat(cfg.snapshot); err == nil {
			db, err := tinysql.LoadFromFile(cfg.snapshot)
			if err != nil {
				return nil, "", fmt.Errorf("load snapshot: %w", err)
			}
			return db, "snapshot", nil
		} else if !os.IsNotExist(err) {
			return nil, "", fmt.Errorf("stat snapshot: %w", err)
		}
	}

	db := tinysql.NewDB()
	if err := seedPOIs(ctx, db); err != nil {
		return nil, "", err
	}
	if cfg.snapshot != "" {
		if err := tinysql.SaveToFile(db, cfg.snapshot); err != nil {
			return nil, "", fmt.Errorf("save snapshot: %w", err)
		}
		return db, "new snapshot", nil
	}
	return db, "in-memory dataset", nil
}

func seedPOIs(ctx context.Context, db *tinysql.DB) error {
	for _, sql := range []string{
		`CREATE TABLE poi (id INTEGER PRIMARY KEY, name TEXT, category TEXT, city TEXT, lat REAL, lon REAL)`,
		`INSERT INTO poi VALUES (1, 'Marienplatz', 'sight', 'München', 48.1372, 11.5756)`,
		`INSERT INTO poi VALUES (2, 'Deutsches Museum', 'museum', 'München', 48.1303, 11.5838)`,
		`INSERT INTO poi VALUES (3, 'Englischer Garten', 'park', 'München', 48.1642, 11.6050)`,
		`INSERT INTO poi VALUES (4, 'Brandenburger Tor', 'sight', 'Berlin', 52.5163, 13.3777)`,
		`INSERT INTO poi VALUES (5, 'Alte Pinakothek', 'museum', 'München', 48.1480, 11.5707)`,
	} {
		if _, err := tinysql.ExecSQL(ctx, db, "default", sql); err != nil {
			return fmt.Errorf("seed POIs: %w", err)
		}
	}
	return nil
}

func searchPOIs(ctx context.Context, db *tinysql.DB, query string) ([]poi, error) {
	needle := strings.TrimSpace(query)
	if needle == "" {
		return nil, fmt.Errorf("query must not be empty")
	}
	// The demo builds a literal from a local CLI argument; production callers
	// should use the database/sql driver's bound parameters instead.
	needle = strings.ReplaceAll(needle, "'", "''")
	pattern := "'%" + needle + "%'"
	sql := fmt.Sprintf(`SELECT id, name, category, city, lat, lon
FROM poi
WHERE name LIKE %s OR city LIKE %s OR category LIKE %s
ORDER BY name`, pattern, pattern, pattern)
	rs, err := tinysql.ExecSQL(ctx, db, "default", sql)
	if err != nil {
		return nil, err
	}
	result := make([]poi, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		result = append(result, poi{
			ID:       intValue(row, "id"),
			Name:     stringValue(row, "name"),
			Category: stringValue(row, "category"),
			City:     stringValue(row, "city"),
			Lat:      floatValue(row, "lat"),
			Lon:      floatValue(row, "lon"),
		})
	}
	return result, nil
}

func stringValue(row tinysql.Row, column string) string {
	v, _ := tinysql.GetVal(row, column)
	return fmt.Sprint(v)
}

func intValue(row tinysql.Row, column string) int {
	v, _ := tinysql.GetVal(row, column)
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func floatValue(row tinysql.Row, column string) float64 {
	v, _ := tinysql.GetVal(row, column)
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

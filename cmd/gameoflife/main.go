// gameoflife runs Conway's rules inside tinySQL. Go only seeds and renders.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func query(ctx context.Context, db *tinysql.DB, sql string) (*tinysql.ResultSet, error) {
	stmt, err := tinysql.NewParser(sql).ParseStatement()
	if err != nil {
		return nil, err
	}
	return tinysql.Execute(ctx, db, "default", stmt)
}

func createBoard(ctx context.Context, db *tinysql.DB, width, height int, live map[[2]int]bool) error {
	if width < 3 || height < 3 {
		return fmt.Errorf("board dimensions must be at least 3")
	}
	if _, err := query(ctx, db, `CREATE TABLE cells (x INT, y INT, alive INT)`); err != nil {
		return err
	}
	var values []string
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			alive := 0
			if live[[2]int{x, y}] {
				alive = 1
			}
			values = append(values, fmt.Sprintf("(%d,%d,%d)", x, y, alive))
		}
	}
	if _, err := query(ctx, db, "INSERT INTO cells VALUES "+strings.Join(values, ",")); err != nil {
		return err
	}
	// Eight offsets generate neighboring contributions in O(8 * cells) before
	// aggregation; modulo implements the same toroidal board as the source demo.
	_, err := query(ctx, db, fmt.Sprintf(`CREATE VIEW neighbor_counts AS
 WITH offsets AS (
 SELECT -1 AS dx, -1 AS dy UNION ALL SELECT 0, -1 UNION ALL SELECT 1, -1
 UNION ALL SELECT -1, 0 UNION ALL SELECT 1, 0
 UNION ALL SELECT -1, 1 UNION ALL SELECT 0, 1 UNION ALL SELECT 1, 1
 ), contributions AS (
 SELECT (c.x + o.dx + %d) %% %d AS nx,
        (c.y + o.dy + %d) %% %d AS ny, c.alive AS alive
 FROM cells c CROSS JOIN offsets o
 ) SELECT nx AS x, ny AS y, SUM(alive) AS neighbor_count
 FROM contributions GROUP BY nx, ny`, width, width, height, height))
	if err != nil {
		return err
	}
	_, err = query(ctx, db, `CREATE VIEW next_generation AS
 SELECT c.x AS x, c.y AS y,
 CASE WHEN nc.neighbor_count = 3 OR (c.alive = 1 AND nc.neighbor_count = 2)
 THEN 1 ELSE 0 END AS alive
 FROM cells c LEFT JOIN neighbor_counts nc ON c.x = nc.x AND c.y = nc.y`)
	return err
}

func registerStep() error {
	return tinysql.RegisterStoredProcedureWithOptions("life_step", tinysql.StoredProcedureOptions{Atomic: true, Parameters: []tinysql.StoredProcedureParameter{}}, func(pc tinysql.ProcedureContext, _ []any) (*tinysql.ResultSet, error) {
		for _, sql := range []string{`CREATE TEMP TABLE next_cells AS SELECT x, y, alive FROM next_generation`, `DELETE FROM cells`, `INSERT INTO cells (x, y, alive) SELECT x, y, alive FROM next_cells`, `DROP TABLE next_cells`} {
			if _, err := pc.ExecuteSQL(sql); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
}

func run() error {
	width := flag.Int("width", 50, "board width (3..200)")
	height := flag.Int("height", 30, "board height (3..200)")
	steps := flag.Int("steps", 100, "generations to run")
	seed := flag.Int64("seed", 1, "random seed")
	interval := flag.Duration("interval", 100*time.Millisecond, "pause between generations")
	flag.Parse()
	if *width < 3 || *height < 3 || *width > 200 || *height > 200 || *steps < 0 || *interval < 0 {
		return fmt.Errorf("invalid dimensions, steps or interval")
	}
	rng := rand.New(rand.NewSource(*seed))
	live := map[[2]int]bool{}
	for y := 0; y < *height; y++ {
		for x := 0; x < *width; x++ {
			live[[2]int{x, y}] = rng.Float64() < 0.4
		}
	}
	db := tinysql.NewDB()
	ctx := context.Background()
	if err := registerStep(); err != nil {
		return err
	}
	if err := createBoard(ctx, db, *width, *height, live); err != nil {
		return err
	}
	for step := 0; step <= *steps; step++ {
		rs, err := query(ctx, db, `SELECT alive FROM cells ORDER BY y, x`)
		if err != nil {
			return err
		}
		var out strings.Builder
		fmt.Fprintf(&out, "\x1b[H\x1b[2JGeneration %d\n", step)
		for i, row := range rs.Rows {
			if fmt.Sprint(row["alive"]) == "1" {
				out.WriteString("█")
			} else {
				out.WriteByte(' ')
			}
			if (i+1)%*width == 0 {
				out.WriteByte('\n')
			}
		}
		fmt.Print(out.String())
		if step == *steps {
			break
		}
		if _, err := query(ctx, db, `CALL life_step()`); err != nil {
			return err
		}
		time.Sleep(*interval)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

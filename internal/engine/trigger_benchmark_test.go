package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// BenchmarkTriggerBatchInsert isolates the steady-state row-trigger path:
// trigger definitions and their parsed bodies are already cached, while every
// source row still fires an AFTER INSERT trigger which performs one nested
// INSERT. executeInsert is used directly so statement-level rollback snapshot
// growth does not obscure trigger dispatch and body-execution costs.
func BenchmarkTriggerBatchInsert(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE events (id INT, amount INT)`,
		`CREATE TABLE audit (id INT, amount INT)`,
		`CREATE TRIGGER copy_event AFTER INSERT ON events FOR EACH ROW BEGIN
			INSERT INTO audit VALUES (NEW.id, NEW.amount);
		END`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			b.Fatal(err)
		}
	}

	const rowsPerStatement = 100
	values := make([]string, rowsPerStatement)
	for i := range values {
		values[i] = fmt.Sprintf("(%d, %d)", i, i*10)
	}
	stmt := mustParse(`INSERT INTO events VALUES ` + strings.Join(values, ", ")).(*Insert)
	env := ExecEnv{ctx: ctx, tenant: "default", db: db}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := executeInsert(env, stmt); err != nil {
			b.Fatal(err)
		}
	}
}

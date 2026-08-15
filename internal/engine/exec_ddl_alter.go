// ALTER TABLE.
package engine

func executeAlterTable(env ExecEnv, s *AlterTable) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}

	if s.AddColumn != nil {
		// Added through storage rather than by appending to t.Cols here:
		// storage.Table owns the column-name index that ColIndex answers
		// from, and appending to Cols alone leaves a column no lookup can
		// resolve. Table.AddColumn also rejects a duplicate name before it
		// mutates anything, and backfills existing rows. See its doc comment.
		if err := t.AddColumn(*s.AddColumn); err != nil {
			return nil, err
		}
		t.InvalidateStats()
		// Deliberately no db.Put: t is the live table and has just been
		// mutated in place. Put treats an existing table as a conflict, so
		// calling it here made ALTER TABLE ADD COLUMN fail unconditionally,
		// after having already widened the schema and every row.
		//
		// Durability comes from where every other in-place schema change gets
		// it — executeCreateIndex does exactly this. The version bump is what
		// makes the statement visible to the write-ahead log's change diff,
		// and the full-table sentinel makes the record it writes carry the new
		// column list rather than a row delta.
		t.Version++
		t.MarkDirtyFrom(-1)
	}

	return nil, nil
}

// ALTER TABLE.
package engine

import (
	"fmt"
)

func executeAlterTable(env ExecEnv, s *AlterTable) (*ResultSet, error) {
	// Get the table
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}

	// Handle ADD COLUMN
	if s.AddColumn != nil {
		// Check if column already exists
		for _, col := range t.Cols {
			if col.Name == s.AddColumn.Name {
				return nil, fmt.Errorf("column %q already exists", s.AddColumn.Name)
			}
		}

		// Add the new column to table schema
		t.Cols = append(t.Cols, *s.AddColumn)

		// Add NULL values for existing rows
		for i := range t.Rows {
			t.Rows[i] = append(t.Rows[i], nil)
		}
		t.InvalidateStats()

		// Update the table
		if err := env.db.Put(env.tenant, t); err != nil {
			return nil, fmt.Errorf("alter table: %w", err)
		}
	}

	return nil, nil
}

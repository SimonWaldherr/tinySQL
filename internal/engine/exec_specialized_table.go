package engine

import (
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Specialized modules are physical SQL tables with workload-specific schemas
// and indexes. Preparing the complete table before Put keeps index creation
// errors from publishing a partially configured table.
func executeCreateSpecializedTable(env ExecEnv, s *CreateTable) (*ResultSet, error) {
	var cols []storage.Column
	switch s.Using {
	case "keyvalue":
		cols = []storage.Column{
			{Name: "key", Type: storage.TextType, Constraint: storage.PrimaryKey, NotNull: true},
			{Name: "value", Type: storage.BlobType},
		}
	case "document":
		cols = []storage.Column{
			{Name: "id", Type: storage.TextType, Constraint: storage.PrimaryKey, NotNull: true},
			{Name: "document", Type: storage.JsonType, NotNull: true, StrictJSON: true},
		}
	case "timeseries":
		cols = []storage.Column{
			{Name: "series", Type: storage.TextType, NotNull: true},
			{Name: "time", Type: storage.IntType, NotNull: true},
			{Name: "value", Type: storage.FloatType, NotNull: true},
		}
	default:
		return nil, fmt.Errorf("unknown virtual table module %q", s.Using)
	}
	if len(s.FTSColumns) != 0 {
		if len(s.FTSColumns) != len(cols) {
			return nil, fmt.Errorf("%s expects %d column names, got %d", s.Using, len(cols), len(s.FTSColumns))
		}
		seen := make(map[string]bool, len(cols))
		for i, name := range s.FTSColumns {
			key := strings.ToLower(name)
			if key == "" || seen[key] {
				return nil, fmt.Errorf("invalid or duplicate column name %q", name)
			}
			seen[key] = true
			cols[i].Name = name
		}
	}
	table := storage.NewTable(s.Name, cols, s.IsTemp)
	if s.Using == "keyvalue" || s.Using == "document" {
		// Persist a sorted point-access structure so restart does not require
		// rebuilding the derived primary-key hash before the first SELECT.
		if err := table.CreateSecondaryIndex("__profile_key", []string{cols[0].Name}, true); err != nil {
			return nil, err
		}
	}
	if s.Using == "timeseries" {
		// An internal table-local index, persisted with the table. Equal timestamps
		// are allowed; the access path narrows a series and then a time interval.
		if err := table.CreateSecondaryIndex("__timeseries_series_time", []string{cols[0].Name, cols[1].Name}, false); err != nil {
			return nil, err
		}
	}
	return nil, env.db.Put(env.tenant, table)
}

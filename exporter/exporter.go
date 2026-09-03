// Package exporter exposes stable result export helpers backed by tinySQL's
// internal exporter implementation.
package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
	ie "github.com/SimonWaldherr/tinySQL/internal/exporter"
)

// Options controls exporter behavior.
type Options = ie.Options

// TableManifest is the portable table schema and data-fingerprint document.
type TableManifest = ie.TableManifest

// ManifestColumn describes one column in a TableManifest.
type ManifestColumn = ie.ManifestColumn

// ExportCSV writes ResultSet rows as CSV to w.
func ExportCSV(w io.Writer, rs *tinysql.ResultSet, opts Options) error {
	return ie.ExportCSV(w, rs, opts)
}

// ExportTSV writes ResultSet rows as tab-separated values to w.
func ExportTSV(w io.Writer, rs *tinysql.ResultSet, opts Options) error {
	opts.CSVDelimiter = '\t'
	return ie.ExportCSV(w, rs, opts)
}

// ExportJSON writes ResultSet rows as a JSON array of objects.
func ExportJSON(w io.Writer, rs *tinysql.ResultSet, opts Options) error {
	return ie.ExportJSON(w, rs, opts)
}

// ExportTableJSON queries one table and writes it as a JSON array. The table
// identifier is quoted before parsing, so names from application input cannot
// change the query structure.
func ExportTableJSON(ctx context.Context, w io.Writer, db *tinysql.DB, tenant, tableName string, opts Options) error {
	if db == nil {
		return fmt.Errorf("exporter: nil DB")
	}
	if strings.TrimSpace(tableName) == "" {
		return fmt.Errorf("exporter: empty table name")
	}
	sql := `SELECT * FROM "` + strings.ReplaceAll(tableName, `"`, `""`) + `"`
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		return err
	}
	rs, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		return err
	}
	if err := decodeTableJSONColumns(db, tenant, tableName, rs); err != nil {
		return err
	}
	return ie.ExportJSON(w, rs, opts)
}

func decodeTableJSONColumns(db *tinysql.DB, tenant, tableName string, rs *tinysql.ResultSet) error {
	table, err := db.Get(tenant, tableName)
	if err != nil {
		return err
	}
	for _, column := range table.Cols {
		if column.Type != tinysql.JsonType {
			continue
		}
		key := strings.ToLower(column.Name)
		for _, row := range rs.Rows {
			text, ok := row[key].(string)
			if !ok {
				continue
			}
			var decoded any
			decoder := json.NewDecoder(strings.NewReader(text))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err != nil {
				return fmt.Errorf("exporter: decode JSON column %q: %w", column.Name, err)
			}
			row[key] = decoded
		}
	}
	return nil
}

// ExportNDJSON streams one JSON object per ResultSet row to w.
func ExportNDJSON(w io.Writer, rs *tinysql.ResultSet, opts Options) error {
	return ie.ExportNDJSON(w, rs, opts)
}

// ExportSQL writes ResultSet rows as INSERT statements for tableName.
func ExportSQL(w io.Writer, rs *tinysql.ResultSet, tableName string) error {
	return ie.ExportSQL(w, rs, tableName)
}

// ExportXML writes ResultSet rows as simple XML.
func ExportXML(w io.Writer, rs *tinysql.ResultSet) error {
	return ie.ExportXML(w, rs)
}

// ExportGOB writes ResultSet rows and column order as a GOB payload.
func ExportGOB(w io.Writer, rs *tinysql.ResultSet) error {
	return ie.ExportGOB(w, rs)
}

// ExportXLSX writes ResultSet rows as a single-sheet Excel workbook to w.
func ExportXLSX(w io.Writer, rs *tinysql.ResultSet, opts Options) error {
	return ie.ExportXLSX(w, rs, opts)
}

// ExportGeoJSON writes ResultSet rows as an RFC 7946 GeoJSON
// FeatureCollection. geomCol names the geometry column; pass "" to
// auto-detect it.
func ExportGeoJSON(w io.Writer, rs *tinysql.ResultSet, geomCol string, opts Options) error {
	return ie.ExportGeoJSON(w, rs, geomCol, opts)
}

// ExportTopoJSON writes ResultSet rows as a TopoJSON v3 Topology. geomCol
// names the geometry column (pass "" to auto-detect it); objectName names
// the single "objects" key (pass "" for the default, "collection").
func ExportTopoJSON(w io.Writer, rs *tinysql.ResultSet, geomCol, objectName string, opts Options) error {
	return ie.ExportTopoJSON(w, rs, geomCol, objectName, opts)
}

// ExportTableManifest writes a versioned table schema and data fingerprint.
func ExportTableManifest(w io.Writer, db *tinysql.DB, tenant, tableName string) error {
	return ie.ExportTableManifest(w, db, tenant, tableName)
}

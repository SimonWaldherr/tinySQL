// Package exporter exposes stable result export helpers backed by tinySQL's
// internal exporter implementation.
package exporter

import (
	"io"

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

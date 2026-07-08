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

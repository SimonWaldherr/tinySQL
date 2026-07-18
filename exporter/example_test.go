package exporter_test

import (
	"bytes"
	"fmt"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/exporter"
)

// jsonExampleResult gives the example a named package-level fixture while
// keeping the usage of ExportJSON concise.
var jsonExampleResult = &tinysql.ResultSet{
	Cols: []string{"id", "name"},
	Rows: []tinysql.Row{{"id": 1, "name": "Ada"}},
}

func ExampleExportJSON() {
	var out bytes.Buffer
	if err := exporter.ExportJSON(&out, jsonExampleResult, exporter.Options{}); err != nil {
		panic(err)
	}
	fmt.Print(out.String())

	// Output:
	// [{"id":1,"name":"Ada"}]
}

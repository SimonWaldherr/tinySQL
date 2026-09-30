//go:build tinysql_minimal

package importer

import "errors"

// decodeYAMLRecords is unavailable in tinysql_minimal builds, which drop the
// gopkg.in/yaml.v3 dependency to keep browser WASM bundles small.
func decodeYAMLRecords([]byte) ([]map[string]any, error) {
	return nil, errors.New("YAML import is unavailable in tinysql_minimal builds")
}

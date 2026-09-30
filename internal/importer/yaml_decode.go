//go:build !tinysql_minimal

package importer

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// decodeYAMLRecords accepts a YAML list of mappings or a single mapping.
func decodeYAMLRecords(all []byte) ([]map[string]any, error) {
	var records []map[string]any
	if err := yaml.Unmarshal(all, &records); err != nil {
		var single map[string]any
		if err2 := yaml.Unmarshal(all, &single); err2 != nil {
			return nil, fmt.Errorf("unsupported YAML structure: %v / %v", err, err2)
		}
		records = append(records, single)
	}
	return records, nil
}

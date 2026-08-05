package tiles

import "encoding/json"

func cloneJSONMapChecked(in map[string]any) (map[string]any, error) {
	if len(in) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func cloneJSONMap(in map[string]any) map[string]any {
	out, err := cloneJSONMapChecked(in)
	if err != nil {
		// Artifact manifests are decoded from JSON. Returning an empty value is
		// safer than leaking a caller-owned map if a future implementation ever
		// breaks that invariant.
		return nil
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

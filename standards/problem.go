package standards

// Problem is an RFC 9457 Problem Details response. Error is a compatibility
// extension for older tinySQL/dbweb clients that read JSON responses via
// data.error.
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"`
	Error    string `json:"error,omitempty"`
}

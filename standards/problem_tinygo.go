//go:build tinygo.wasm || baremetal

package standards

// NewProblem remains available on TinyGo without importing net/http. The
// fallback titles cover the common HTTP statuses used by tinySQL integrations.
func NewProblem(status int, title, detail, instance string) Problem {
	if title == "" {
		title = tinyGoStatusText(status)
	}
	return Problem{
		Type:     "about:blank",
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: instance,
		Error:    detail,
	}
}

func tinyGoStatusText(status int) string {
	switch status {
	case 200:
		return "OK"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 409:
		return "Conflict"
	case 422:
		return "Unprocessable Content"
	case 429:
		return "Too Many Requests"
	case 500:
		return "Internal Server Error"
	case 503:
		return "Service Unavailable"
	default:
		return "HTTP Error"
	}
}

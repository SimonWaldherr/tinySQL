// Package errutil provides error-preserving cleanup helpers.
package errutil

import (
	"errors"
	"fmt"
	"io"
)

// Close closes a resource and adds any failure to the named return error.
// Use it with defer so cleanup also runs on early returns and panics.
func Close(closer io.Closer, err *error, operation string) {
	if closeErr := closer.Close(); closeErr != nil {
		wrapped := fmt.Errorf("%s: %w", operation, closeErr)
		if *err == nil {
			*err = wrapped
		} else {
			*err = errors.Join(*err, wrapped)
		}
	}
}

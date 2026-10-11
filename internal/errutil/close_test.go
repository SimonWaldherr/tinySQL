package errutil

import (
	"errors"
	"testing"
)

type testCloser struct{ err error }

func (c testCloser) Close() error { return c.err }

func TestClosePreservesBothErrors(t *testing.T) {
	primary := errors.New("read failed")
	cleanup := errors.New("close failed")
	for _, prior := range []error{nil, primary} {
		for _, closeErr := range []error{nil, cleanup} {
			err := prior
			Close(testCloser{closeErr}, &err, "close stream")
			if closeErr == nil && err != prior {
				t.Fatalf("successful close changed error: %v", err)
			}
			if prior != nil && !errors.Is(err, prior) {
				t.Fatalf("lost primary error: %v", err)
			}
			if closeErr != nil && !errors.Is(err, closeErr) {
				t.Fatalf("lost close error: %v", err)
			}
			if prior == nil && closeErr != nil && err.Error() != "close stream: close failed" {
				t.Fatalf("missing close context: %v", err)
			}
		}
	}
}

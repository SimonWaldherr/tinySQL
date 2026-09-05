//go:build !js || !wasm

package offline

import (
	"context"
	"errors"
)

// IndexedDBStore is available only in GOOS=js/GOARCH=wasm builds.
type IndexedDBStore struct{}

func NewIndexedDBStore(context.Context, string) (*IndexedDBStore, error) {
	return nil, errors.New("IndexedDBStore is available only in GOOS=js GOARCH=wasm builds")
}

//go:build js && wasm

package offline

import "errors"

// FileStore is unavailable in a browser. Use IndexedDBStore instead.
type FileStore struct{}

func NewFileStore(string) (*FileStore, error) {
	return nil, errors.New("FileStore is unavailable in WebAssembly; use IndexedDBStore")
}

func (*FileStore) SetMaxTileSize(int64) error {
	return errors.New("FileStore is unavailable in WebAssembly; use IndexedDBStore")
}

//go:build js && wasm

package offline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall/js"
)

// IndexedDBStore persists revisioned tile responses in the browser's
// IndexedDB. Its methods wait on asynchronous browser requests, so call Sync
// from a goroutine (the supplied WASM API already does this) rather than
// blocking a JavaScript event callback directly.
type IndexedDBStore struct {
	mu sync.RWMutex
	db js.Value
}

func NewIndexedDBStore(ctx context.Context, name string) (*IndexedDBStore, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "tinytiles"
	}
	if err := validateIdentifier("IndexedDB name", name, 256); err != nil {
		return nil, err
	}
	indexedDB := js.Global().Get("indexedDB")
	if indexedDB.Type() == js.TypeUndefined || indexedDB.IsNull() {
		return nil, errors.New("IndexedDB is unavailable in this browser context")
	}
	request := indexedDB.Call("open", name, ProtocolVersion)
	upgrade := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		database := args[0].Get("target").Get("result")
		objectNames := database.Get("objectStoreNames")
		if !objectNames.Call("contains", "tiles").Bool() {
			database.Call("createObjectStore", "tiles", js.ValueOf(map[string]any{"keyPath": "key"}))
		}
		if !objectNames.Call("contains", "manifests").Bool() {
			database.Call("createObjectStore", "manifests", js.ValueOf(map[string]any{"keyPath": "key"}))
		}
		return nil
	})
	request.Set("onupgradeneeded", upgrade)
	defer func() {
		request.Set("onupgradeneeded", js.Null())
		upgrade.Release()
	}()
	database, err := awaitIDBRequest(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("open IndexedDB: %w", err)
	}
	return &IndexedDBStore{db: database}, nil
}

func (s *IndexedDBStore) GetManifest(ctx context.Context, dataset string) (Manifest, bool, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, false, err
	}
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return Manifest{}, false, err
	}
	record, found, err := s.get(ctx, "manifests", persistentManifestKey(dataset))
	if err != nil || !found {
		return Manifest{}, found, err
	}
	var manifest Manifest
	if err := json.Unmarshal([]byte(record.Get("json").String()), &manifest); err != nil {
		return Manifest{}, false, fmt.Errorf("decode IndexedDB manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, false, err
	}
	if manifest.Dataset != dataset {
		return Manifest{}, false, errors.New("IndexedDB manifest dataset does not match its key")
	}
	return manifest, true, nil
}

func (s *IndexedDBStore) PutManifest(ctx context.Context, manifest Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	record := js.Global().Get("Object").New()
	record.Set("key", persistentManifestKey(manifest.Dataset))
	record.Set("json", string(encoded))
	return s.put(ctx, "manifests", record)
}

func (s *IndexedDBStore) GetTile(ctx context.Context, dataset, revision string, key TileKey) (Tile, bool, error) {
	if err := ctx.Err(); err != nil {
		return Tile{}, false, err
	}
	if err := validateCacheKey(dataset, revision, key); err != nil {
		return Tile{}, false, err
	}
	record, found, err := s.get(ctx, "tiles", persistentTileKey(dataset, revision, key))
	if err != nil || !found {
		return Tile{}, found, err
	}
	dataValue := record.Get("data")
	if dataValue.Type() == js.TypeUndefined || dataValue.IsNull() {
		return Tile{}, false, errors.New("IndexedDB tile record has no data")
	}
	data := make([]byte, dataValue.Get("byteLength").Int())
	js.CopyBytesToGo(data, dataValue)
	tile := Tile{
		Data:            data,
		ContentType:     record.Get("contentType").String(),
		ContentEncoding: record.Get("contentEncoding").String(),
		ETag:            record.Get("etag").String(),
		Checksum:        record.Get("checksum").String(),
	}
	if err := verifyTile(tile); err != nil {
		return Tile{}, false, err
	}
	return tile, true, nil
}

func (s *IndexedDBStore) PutTile(ctx context.Context, dataset, revision string, key TileKey, tile Tile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCacheKey(dataset, revision, key); err != nil {
		return err
	}
	if err := verifyTile(tile); err != nil {
		return err
	}
	data := js.Global().Get("Uint8Array").New(len(tile.Data))
	js.CopyBytesToJS(data, tile.Data)
	record := js.Global().Get("Object").New()
	record.Set("key", persistentTileKey(dataset, revision, key))
	record.Set("data", data)
	record.Set("contentType", tile.ContentType)
	record.Set("contentEncoding", tile.ContentEncoding)
	record.Set("etag", tile.ETag)
	record.Set("checksum", tile.Checksum)
	return s.put(ctx, "tiles", record)
}

func (s *IndexedDBStore) DeleteRevision(ctx context.Context, dataset, revision string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return err
	}
	if err := validateIdentifier("revision", revision, 512); err != nil {
		return err
	}
	transaction, store, err := s.transaction("tiles", "readwrite")
	if err != nil {
		return err
	}
	waiter := newIDBTransactionWaiter(transaction)
	defer waiter.release()
	prefix := "tile:" + cacheComponent(dataset) + ":" + cacheComponent(revision) + ":"
	keyRange := js.Global().Get("IDBKeyRange")
	if keyRange.Type() == js.TypeUndefined || keyRange.IsNull() {
		return errors.New("IDBKeyRange is unavailable")
	}
	request := store.Call("openCursor", keyRange.Call("bound", prefix, prefix+"\uffff"))
	done := make(chan error, 1)
	success := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			select {
			case done <- errors.New("IndexedDB cursor has no event"):
			default:
			}
			return nil
		}
		cursor := args[0].Get("target").Get("result")
		if cursor.Type() == js.TypeUndefined || cursor.IsNull() {
			select {
			case done <- nil:
			default:
			}
			return nil
		}
		cursor.Call("delete")
		cursor.Call("continue")
		return nil
	})
	failure := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		select {
		case done <- idbError(request):
		default:
		}
		return nil
	})
	request.Set("onsuccess", success)
	request.Set("onerror", failure)
	defer clearIDBRequestHandlers(request, success, failure)
	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return waiter.wait(ctx)
}

// Close releases the browser database handle. A closed store rejects future
// calls; pending operations should be cancelled through their context first.
func (s *IndexedDBStore) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.db.Type() != js.TypeUndefined && !s.db.IsNull() {
		s.db.Call("close")
		s.db = js.Undefined()
	}
	s.mu.Unlock()
}

func (s *IndexedDBStore) get(ctx context.Context, name, key string) (js.Value, bool, error) {
	transaction, store, err := s.transaction(name, "readonly")
	if err != nil {
		return js.Undefined(), false, err
	}
	request := store.Call("get", key)
	value, err := awaitIDBRequest(ctx, request)
	if err != nil {
		return js.Undefined(), false, err
	}
	if value.Type() == js.TypeUndefined || value.IsNull() {
		return js.Undefined(), false, nil
	}
	// Keep the transaction value referenced until the request result is copied.
	_ = transaction
	return value, true, nil
}

func (s *IndexedDBStore) put(ctx context.Context, name string, record js.Value) error {
	transaction, store, err := s.transaction(name, "readwrite")
	if err != nil {
		return err
	}
	waiter := newIDBTransactionWaiter(transaction)
	defer waiter.release()
	request := store.Call("put", record)
	if _, err := awaitIDBRequest(ctx, request); err != nil {
		return err
	}
	return waiter.wait(ctx)
}

func (s *IndexedDBStore) transaction(name, mode string) (js.Value, js.Value, error) {
	if s == nil {
		return js.Undefined(), js.Undefined(), errors.New("IndexedDB store is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	database := s.db
	if database.Type() == js.TypeUndefined || database.IsNull() {
		return js.Undefined(), js.Undefined(), errors.New("IndexedDB store is closed")
	}
	transaction := database.Call("transaction", name, mode)
	return transaction, transaction.Call("objectStore", name), nil
}

type idbRequestResult struct {
	value js.Value
	err   error
}

func awaitIDBRequest(ctx context.Context, request js.Value) (js.Value, error) {
	done := make(chan idbRequestResult, 1)
	success := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			select {
			case done <- idbRequestResult{err: errors.New("IndexedDB request has no event")}:
			default:
			}
			return nil
		}
		select {
		case done <- idbRequestResult{value: args[0].Get("target").Get("result")}:
		default:
		}
		return nil
	})
	failure := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		select {
		case done <- idbRequestResult{err: idbError(request)}:
		default:
		}
		return nil
	})
	request.Set("onsuccess", success)
	request.Set("onerror", failure)
	defer clearIDBRequestHandlers(request, success, failure)
	select {
	case result := <-done:
		return result.value, result.err
	case <-ctx.Done():
		return js.Undefined(), ctx.Err()
	}
}

func clearIDBRequestHandlers(request js.Value, success, failure js.Func) {
	request.Set("onsuccess", js.Null())
	request.Set("onerror", js.Null())
	success.Release()
	failure.Release()
}

type idbTransactionWaiter struct {
	transaction js.Value
	done        chan error
	success     js.Func
	failure     js.Func
	abort       js.Func
}

func newIDBTransactionWaiter(transaction js.Value) *idbTransactionWaiter {
	waiter := &idbTransactionWaiter{transaction: transaction, done: make(chan error, 1)}
	waiter.success = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		select {
		case waiter.done <- nil:
		default:
		}
		return nil
	})
	waiter.failure = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		select {
		case waiter.done <- idbError(transaction):
		default:
		}
		return nil
	})
	waiter.abort = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		select {
		case waiter.done <- errors.New("IndexedDB transaction aborted"):
		default:
		}
		return nil
	})
	transaction.Set("oncomplete", waiter.success)
	transaction.Set("onerror", waiter.failure)
	transaction.Set("onabort", waiter.abort)
	return waiter
}

func (w *idbTransactionWaiter) wait(ctx context.Context) error {
	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *idbTransactionWaiter) release() {
	w.transaction.Set("oncomplete", js.Null())
	w.transaction.Set("onerror", js.Null())
	w.transaction.Set("onabort", js.Null())
	w.success.Release()
	w.failure.Release()
	w.abort.Release()
}

func idbError(value js.Value) error {
	err := value.Get("error")
	if err.Type() != js.TypeUndefined && !err.IsNull() {
		return errors.New(err.String())
	}
	return errors.New("IndexedDB operation failed")
}

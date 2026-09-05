//go:build js && wasm

// tinytiles-wasm exposes a Promise-based browser API around the IndexedDB
// offline cache. It intentionally does not open a server .ttiles artifact in
// the browser; tiles are synchronized as revisioned HTTP responses instead.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	"github.com/Karte-Bayern/tinyTiles/offline"
)

var version = "0.1.0-dev"

var browserCache struct {
	sync.Mutex
	store        *offline.IndexedDBStore
	synchronizer *offline.Synchronizer
	syncMu       sync.Mutex
}

var retainedFunctions []js.Func

func main() {
	api := js.Global().Get("Object").New()
	bind := func(name string, function func(js.Value, []js.Value) any) {
		wrapped := js.FuncOf(function)
		retainedFunctions = append(retainedFunctions, wrapped)
		api.Set(name, wrapped)
	}
	bind("open", jsOpen)
	bind("close", jsClose)
	bind("status", jsStatus)
	bind("sync", jsSync)
	bind("get", jsGet)
	api.Set("version", version)
	js.Global().Set("tinyTiles", api)
	println("tinyTiles WASM offline cache initialized")
	select {}
}

func jsOpen(_ js.Value, args []js.Value) any {
	name := "tinytiles"
	if len(args) > 0 && args[0].Type() == js.TypeString && args[0].String() != "" {
		name = args[0].String()
	}
	return promise(func(resolve, reject js.Value) {
		browserCache.syncMu.Lock()
		defer browserCache.syncMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		store, err := offline.NewIndexedDBStore(ctx, name)
		if err != nil {
			reject.Invoke(jsError(err))
			return
		}
		browserCache.Lock()
		if browserCache.store != nil {
			browserCache.store.Close()
		}
		browserCache.store = store
		browserCache.synchronizer = &offline.Synchronizer{Store: store}
		browserCache.Unlock()
		resolve.Invoke(jsJSON(map[string]any{"opened": true, "name": name, "version": version}))
	})
}

func jsClose(_ js.Value, _ []js.Value) any {
	return promise(func(resolve, _ js.Value) {
		browserCache.syncMu.Lock()
		defer browserCache.syncMu.Unlock()
		browserCache.Lock()
		if browserCache.store != nil {
			browserCache.store.Close()
		}
		browserCache.store = nil
		browserCache.synchronizer = nil
		browserCache.Unlock()
		resolve.Invoke(jsJSON(map[string]any{"closed": true}))
	})
}

func jsStatus(_ js.Value, _ []js.Value) any {
	browserCache.Lock()
	opened := browserCache.store != nil
	browserCache.Unlock()
	return jsJSON(map[string]any{"opened": opened, "version": version})
}

// sync(manifestURL, request) downloads only the requested keys/ranges into a
// new immutable revision and resolves after its manifest becomes active.
func jsSync(_ js.Value, args []js.Value) any {
	if len(args) == 0 || args[0].Type() != js.TypeString || args[0].String() == "" {
		return rejectedPromise(fmt.Errorf("tinyTiles.sync requires a manifest URL"))
	}
	request := offline.SyncRequest{}
	if len(args) > 1 && !args[1].IsNull() && args[1].Type() != js.TypeUndefined {
		if err := decodeJS(args[1], &request); err != nil {
			return rejectedPromise(fmt.Errorf("decode sync request: %w", err))
		}
	}
	manifestURL := args[0].String()
	return promise(func(resolve, reject js.Value) {
		browserCache.syncMu.Lock()
		defer browserCache.syncMu.Unlock()
		browserCache.Lock()
		synchronizer := browserCache.synchronizer
		browserCache.Unlock()
		if synchronizer == nil {
			reject.Invoke(jsError(fmt.Errorf("offline cache is not open; call tinyTiles.open first")))
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		synchronizer.Fetcher = &offline.HTTPFetcher{ManifestURL: manifestURL}
		result, err := synchronizer.Sync(ctx, request)
		if err != nil {
			reject.Invoke(jsError(err))
			return
		}
		resolve.Invoke(jsJSON(result))
	})
}

// get(dataset, z, x, y) resolves to {found, data: Uint8Array, ...}. It reads
// the currently published local revision only, never a stale cache namespace.
func jsGet(_ js.Value, args []js.Value) any {
	if len(args) != 4 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeNumber || args[2].Type() != js.TypeNumber || args[3].Type() != js.TypeNumber {
		return rejectedPromise(fmt.Errorf("tinyTiles.get requires dataset, z, x and y"))
	}
	if args[1].Float() != float64(args[1].Int()) || args[2].Float() != float64(args[2].Int()) || args[3].Float() != float64(args[3].Int()) {
		return rejectedPromise(fmt.Errorf("tinyTiles.get coordinates must be integers"))
	}
	dataset := args[0].String()
	key := offline.TileKey{Z: args[1].Int(), X: args[2].Int(), Y: args[3].Int()}
	return promise(func(resolve, reject js.Value) {
		browserCache.Lock()
		store := browserCache.store
		browserCache.Unlock()
		if store == nil {
			reject.Invoke(jsError(fmt.Errorf("offline cache is not open; call tinyTiles.open first")))
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		manifest, found, err := store.GetManifest(ctx, dataset)
		if err != nil {
			reject.Invoke(jsError(err))
			return
		}
		if !found {
			resolve.Invoke(jsJSON(map[string]any{"found": false}))
			return
		}
		tile, found, err := store.GetTile(ctx, manifest.Dataset, manifest.Revision, key)
		if err != nil {
			reject.Invoke(jsError(err))
			return
		}
		if !found {
			resolve.Invoke(jsJSON(map[string]any{"found": false, "revision": manifest.Revision}))
			return
		}
		data := js.Global().Get("Uint8Array").New(len(tile.Data))
		js.CopyBytesToJS(data, tile.Data)
		payload := js.Global().Get("Object").New()
		payload.Set("found", true)
		payload.Set("revision", manifest.Revision)
		payload.Set("data", data)
		payload.Set("contentType", tile.ContentType)
		payload.Set("contentEncoding", tile.ContentEncoding)
		payload.Set("checksum", tile.Checksum)
		resolve.Invoke(payload)
	})
}

func promise(work func(resolve, reject js.Value)) js.Value {
	executor := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 2 {
			return nil
		}
		go work(args[0], args[1])
		return nil
	})
	result := js.Global().Get("Promise").New(executor)
	executor.Release()
	return result
}

func rejectedPromise(err error) js.Value {
	return js.Global().Get("Promise").Call("reject", jsError(err))
}

func decodeJS(value js.Value, target any) error {
	encoded := js.Global().Get("JSON").Call("stringify", value)
	if encoded.Type() != js.TypeString {
		return fmt.Errorf("sync request is not JSON serializable")
	}
	return json.Unmarshal([]byte(encoded.String()), target)
}

func jsJSON(value any) js.Value {
	encoded, err := json.Marshal(value)
	if err != nil {
		return jsError(err)
	}
	return js.Global().Get("JSON").Call("parse", string(encoded))
}

func jsError(err error) js.Value {
	return js.Global().Get("Error").New(err.Error())
}

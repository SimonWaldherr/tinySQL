package engine

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVectorIndexBuildWaitHonorsCancellation(t *testing.T) {
	for _, mode := range []string{"ivf", "hnsw"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				table := storage.NewTable("controlled_index_build", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
				table.Rows = [][]any{{[]float64{1, 0}}}
				cache := buildVecColumnCache(table, 0, true)
				key := vecIndexCacheKey{source: table, tenant: "cancel", table: table.Name, metric: "cosine"}
				call := &vecIndexBuildCall{done: make(chan struct{})}
				if mode == "ivf" {
					vecIVFCacheMu.Lock()
					vecIVFBuilds[key] = call
					vecIVFCacheMu.Unlock()
				} else {
					vecHNSWCacheMu.Lock()
					vecHNSWBuilds[key] = call
					vecHNSWCacheMu.Unlock()
				}
				defer func() {
					if mode == "ivf" {
						vecIVFCacheMu.Lock()
						delete(vecIVFBuilds, key)
						vecIVFCacheMu.Unlock()
					} else {
						vecHNSWCacheMu.Lock()
						delete(vecHNSWBuilds, key)
						vecHNSWCacheMu.Unlock()
					}
					close(call.done)
					synctest.Wait()
				}()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				go func() {
					var err error
					if mode == "ivf" {
						_, err = getVecIVFIndex(ctx, "cancel", table, 0, "cosine", 2, cache)
					} else {
						_, err = getVecHNSWIndex(ctx, "cancel", table, 0, "cosine", 2, cache)
					}
					result <- err
				}()
				synctest.Wait() // The request is waiting on the controlled builder.
				cancel()
				synctest.Wait()
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("canceled waiter returned %v", err)
					}
				default:
					t.Error("canceled request is still waiting for the index build")
				}
				select {
				case <-call.done:
					t.Fatal("canceling a waiter must not complete another request's build")
				default:
				}
			})
		})
	}
}

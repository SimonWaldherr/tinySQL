//go:build tinygo.wasm || baremetal

package storage

import (
	"math/big"

	"github.com/google/uuid"
)

func init() {
	// TinyGo's gob implementation treats pointer and value registrations for
	// the same concrete type as conflicting names. Register the concrete value
	// forms used by tinySQL snapshots exactly once.
	safeGobRegister(diskTable{})
	safeGobRegister(Table{})
	safeGobRegister([]float64{})
	safeGobRegister([]any{})
	safeGobRegister(big.Rat{})
	safeGobRegister(uuid.UUID{})
}

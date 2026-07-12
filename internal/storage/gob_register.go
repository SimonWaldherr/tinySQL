//go:build !tinygo.wasm && !baremetal

package storage

import (
	"math/big"

	"github.com/google/uuid"
)

func init() {
	// Register common storage types used in serialized snapshots. Full Go's
	// gob supports both value and pointer registrations.
	safeGobRegister(diskTable{})
	safeGobRegister(&diskTable{})
	safeGobRegister(Table{})
	safeGobRegister(&Table{})
	safeGobRegister([]float64{})
	safeGobRegister([]byte{})
	safeGobRegister([]any{})
	safeGobRegister(big.Rat{})
	safeGobRegister(&big.Rat{})
	safeGobRegister(uuid.UUID{})
}

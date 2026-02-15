package storage

import (
	"github.com/google/uuid"
)

// ParseUUID parses a UUID string into uuid.UUID.
func ParseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

// UUIDToBytes returns the 16-byte representation of a uuid.UUID.
func UUIDToBytes(u uuid.UUID) []byte {
	return u[:]
}

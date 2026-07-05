// Package storage: encryption at rest for disk-based backends.
//
// What: AES-256-GCM authenticated encryption of table files (ModeDisk,
// ModeJSON) and the backend catalog file. GCM was chosen over a plain
// block cipher mode specifically because it's authenticated — decrypting
// with the wrong key, or decrypting data that was altered after being
// written, fails closed with an error instead of silently returning
// garbage or (worse) subtly-wrong-but-plausible data. That gives
// encryption at rest a side benefit of the same tamper-detection property
// the audit log provides for statement history, applied to the data
// files themselves.
//
// Why Argon2id for passphrase-based keys: PBKDF2 (also available in
// golang.org/x/crypto) is checkable purely with CPU, which makes brute-force
// guessing cheap on GPUs/ASICs at scale. Argon2id is memory-hard — an
// attacker also needs the configured amount of RAM per guess, which is the
// current best-practice recommendation (OWASP, and the Password Hashing
// Competition it won) for deriving a key from a human-chosen passphrase.
package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

// EncryptionKeySize is the required AES-256 key size in bytes.
const EncryptionKeySize = 32

// EncryptionSaltSize is the recommended salt size for DeriveKeyFromPassphrase.
const EncryptionSaltSize = 16

// DeriveKeyFromPassphrase derives a 32-byte AES-256 key from a passphrase
// using Argon2id. salt must be a fixed, persisted value of at least
// EncryptionSaltSize bytes (see NewEncryptionSalt) — reusing a random salt
// on every call would derive a different key each time and permanently
// lock the caller out of data encrypted under the previous key.
//
// The cost parameters (time=1, memory=64 MiB, threads=4) follow OWASP's
// Argon2id baseline recommendation: expensive enough to resist large-scale
// offline guessing, cheap enough to run once at database-open time rather
// than per-operation.
func DeriveKeyFromPassphrase(passphrase string, salt []byte) []byte {
	return argon2.IDKey([]byte(passphrase), salt, 1, 64*1024, 4, EncryptionKeySize)
}

// NewEncryptionSalt returns a fresh, cryptographically random salt sized
// for DeriveKeyFromPassphrase. Persist it (e.g. in the backend's manifest)
// — the same salt must be supplied on every subsequent open for the same
// passphrase to derive the same key.
func NewEncryptionSalt() ([]byte, error) {
	salt := make([]byte, EncryptionSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("generate encryption salt: %w", err)
	}
	return salt, nil
}

// Encryptor performs authenticated AES-256-GCM encryption/decryption of
// byte blobs, used to encrypt table files at rest.
type Encryptor struct {
	gcm cipher.AEAD
}

// NewEncryptor creates an Encryptor from a 32-byte AES-256 key (typically
// from DeriveKeyFromPassphrase, or any caller-supplied 32 cryptographically
// random bytes for key-file-based setups).
func NewEncryptor(key []byte) (*Encryptor, error) {
	if len(key) != EncryptionKeySize {
		return nil, fmt.Errorf("encryption key must be %d bytes (AES-256), got %d", EncryptionKeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM mode: %w", err)
	}
	return &Encryptor{gcm: gcm}, nil
}

// Encrypt returns nonce||ciphertext||tag as a single self-contained blob
// safe to write directly to disk (Decrypt expects exactly this layout). A
// fresh random nonce is generated per call, which is required for GCM's
// security guarantees — reusing a nonce under the same key breaks
// confidentiality — and cheap enough to pay per file write.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	return e.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt reverses Encrypt. It returns an error — rather than corrupted or
// wrong plaintext — if blob is truncated, was encrypted under a different
// key, or was altered after encryption: GCM's authentication tag check
// fails closed on any of those.
func (e *Encryptor) Decrypt(blob []byte) ([]byte, error) {
	ns := e.gcm.NonceSize()
	if len(blob) < ns {
		return nil, errors.New("ciphertext too short to contain a nonce")
	}
	nonce, ciphertext := blob[:ns], blob[ns:]
	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w (wrong key, or the data was corrupted/tampered with)", err)
	}
	return plaintext, nil
}

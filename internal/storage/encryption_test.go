// Tests for AES-256-GCM encryption at rest (encryption.go) and its wiring
// into DiskBackend (backend_disk.go).
package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptorRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, EncryptionKeySize)
	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	plaintext := []byte("classified: launch codes are 00000000")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("ciphertext must not contain the plaintext verbatim")
	}
	got, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("round trip mismatch: got %q, want %q", got, plaintext)
	}
}

func TestEncryptorRejectsWrongKey(t *testing.T) {
	key1 := bytes.Repeat([]byte{0x01}, EncryptionKeySize)
	key2 := bytes.Repeat([]byte{0x02}, EncryptionKeySize)
	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	ciphertext, err := enc1.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := enc2.Decrypt(ciphertext); err == nil {
		t.Fatal("expected decryption with the wrong key to fail")
	}
}

func TestEncryptorRejectsTamperedCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte{0x07}, EncryptionKeySize)
	enc, _ := NewEncryptor(key)
	ciphertext, err := enc.Encrypt([]byte("do not modify me"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	tampered := append([]byte{}, ciphertext...)
	tampered[len(tampered)-1] ^= 0xFF // flip bits in the authentication tag
	if _, err := enc.Decrypt(tampered); err == nil {
		t.Fatal("expected GCM to reject a tampered ciphertext")
	}
}

func TestEncryptorRejectsWrongKeySize(t *testing.T) {
	if _, err := NewEncryptor([]byte("too short")); err == nil {
		t.Fatal("expected NewEncryptor to reject a non-32-byte key")
	}
}

func TestEncryptorProducesDifferentCiphertextEachTime(t *testing.T) {
	// A fresh random nonce per Encrypt call means encrypting the same
	// plaintext twice under the same key must not produce identical
	// ciphertext — otherwise an observer could tell two files hold the
	// same content without ever decrypting them.
	key := bytes.Repeat([]byte{0x09}, EncryptionKeySize)
	enc, _ := NewEncryptor(key)
	c1, _ := enc.Encrypt([]byte("same plaintext"))
	c2, _ := enc.Encrypt([]byte("same plaintext"))
	if bytes.Equal(c1, c2) {
		t.Error("expected two encryptions of the same plaintext to differ (nonce reuse)")
	}
}

func TestDeriveKeyFromPassphraseDeterministic(t *testing.T) {
	salt, err := NewEncryptionSalt()
	if err != nil {
		t.Fatalf("NewEncryptionSalt: %v", err)
	}
	k1 := DeriveKeyFromPassphrase("correct horse battery staple", salt)
	k2 := DeriveKeyFromPassphrase("correct horse battery staple", salt)
	if !bytes.Equal(k1, k2) {
		t.Error("expected the same passphrase+salt to derive the same key")
	}
	if len(k1) != EncryptionKeySize {
		t.Errorf("expected a %d-byte key, got %d", EncryptionKeySize, len(k1))
	}

	k3 := DeriveKeyFromPassphrase("a different passphrase", salt)
	if bytes.Equal(k1, k3) {
		t.Error("expected a different passphrase to derive a different key")
	}

	salt2, _ := NewEncryptionSalt()
	k4 := DeriveKeyFromPassphrase("correct horse battery staple", salt2)
	if bytes.Equal(k1, k4) {
		t.Error("expected a different salt to derive a different key from the same passphrase")
	}
}

func TestDiskBackendEncryptedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x11}, EncryptionKeySize)
	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}

	backend, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatalf("NewDiskBackend: %v", err)
	}
	backend.SetEncryptor(enc)

	table := NewTable("secrets", []Column{
		{Name: "id", Type: IntType},
		{Name: "val", Type: StringType},
	}, false)
	table.Rows = [][]any{{1, "nuclear launch codes"}, {2, "the nuclear football combination"}}

	if err := backend.SaveTable("default", table); err != nil {
		t.Fatalf("SaveTable: %v", err)
	}

	loaded, err := backend.LoadTable("default", "secrets")
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if loaded == nil || len(loaded.Rows) != 2 {
		t.Fatalf("expected 2 rows back, got %+v", loaded)
	}
	if loaded.Rows[0][1] != "nuclear launch codes" {
		t.Errorf("unexpected row content: %+v", loaded.Rows[0])
	}
}

func TestDiskBackendEncryptedFileIsNotPlaintextOnDisk(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x22}, EncryptionKeySize)
	enc, _ := NewEncryptor(key)

	backend, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatalf("NewDiskBackend: %v", err)
	}
	backend.SetEncryptor(enc)

	table := NewTable("secrets", []Column{{Name: "val", Type: StringType}}, false)
	secretMarker := "TOP-SECRET-MARKER-STRING"
	table.Rows = [][]any{{secretMarker}}
	if err := backend.SaveTable("default", table); err != nil {
		t.Fatalf("SaveTable: %v", err)
	}

	found := false
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr == nil && bytes.Contains(data, []byte(secretMarker)) {
			found = true
		}
		return nil
	})
	if found {
		t.Fatal("found the secret marker string in plaintext on disk — encryption at rest is not working")
	}
}

func TestDiskBackendWrongKeyFailsToLoad(t *testing.T) {
	dir := t.TempDir()
	rightKey := bytes.Repeat([]byte{0x33}, EncryptionKeySize)
	wrongKey := bytes.Repeat([]byte{0x44}, EncryptionKeySize)

	rightEnc, _ := NewEncryptor(rightKey)
	backend, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatalf("NewDiskBackend: %v", err)
	}
	backend.SetEncryptor(rightEnc)
	table := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	table.Rows = [][]any{{1}}
	if err := backend.SaveTable("default", table); err != nil {
		t.Fatalf("SaveTable: %v", err)
	}

	// Reopen with the wrong key — a realistic scenario (lost/wrong key file).
	wrongEnc, _ := NewEncryptor(wrongKey)
	backend2, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatalf("NewDiskBackend (reopen): %v", err)
	}
	backend2.SetEncryptor(wrongEnc)
	if _, err := backend2.LoadTable("default", "t"); err == nil {
		t.Fatal("expected LoadTable with the wrong key to fail, not silently return garbage")
	}
}

func TestOpenDBWithEncryptionKeyEndToEnd(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{0x55}, EncryptionKeySize)

	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dir, EncryptionKey: key})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	table := NewTable("t", []Column{{Name: "id", Type: IntType}}, false)
	table.Rows = [][]any{{42}}
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with the same key: must read back correctly.
	db2, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dir, EncryptionKey: key})
	if err != nil {
		t.Fatalf("reopen OpenDB: %v", err)
	}
	defer db2.Close()
	loaded, err := db2.Get("default", "t")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(loaded.Rows) != 1 || loaded.Rows[0][0] != 42 {
		t.Errorf("unexpected data after reopen: %+v", loaded.Rows)
	}
}

func TestOpenDBRejectsWrongSizeEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	_, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dir, EncryptionKey: []byte("too-short")})
	if err == nil {
		t.Fatal("expected OpenDB to reject a wrong-size EncryptionKey immediately")
	}
}

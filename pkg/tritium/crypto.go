package tritium

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
)

// Values are sealed with AES-256-GCM before they leave the process, so nodes
// and stores only ever hold ciphertext. Key names stay in the clear, since
// the cluster routes on them, and are bound to the ciphertext as additional
// data so a value can't be replayed under another name.
//
// Wire format, for clients in other languages:
//
//	"TE1" || 12-byte nonce || AES-256-GCM(key, nonce, plaintext, aad = key name)
//
// The GCM tag is the last 16 bytes of the ciphertext, as usual.

// KeySize is the encryption key length in bytes: AES-256.
const KeySize = 32

const (
	magic     = "TE1"
	nonceSize = 12
)

var (
	// ErrNotEncrypted is returned when a client with a key reads a value that
	// was stored without one.
	ErrNotEncrypted = errors.New("tritium: value is not encrypted")
	// ErrDecrypt is returned for a wrong key or a tampered value.
	ErrDecrypt = errors.New("tritium: decryption failed")
)

type box struct {
	aead cipher.AEAD
}

func newBox(key []byte) (*box, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("tritium: encryption key must be %d bytes, got %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &box{aead: aead}, nil
}

func (b *box) seal(name string, plaintext []byte) []byte {
	out := make([]byte, len(magic)+nonceSize, len(magic)+nonceSize+len(plaintext)+b.aead.Overhead())
	copy(out, magic)
	rand.Read(out[len(magic):])
	return b.aead.Seal(out, out[len(magic):], plaintext, []byte(name))
}

func (b *box) open(name string, data []byte) ([]byte, error) {
	if len(data) < len(magic)+nonceSize+b.aead.Overhead() || string(data[:len(magic)]) != magic {
		return nil, ErrNotEncrypted
	}
	nonce, ct := data[len(magic):len(magic)+nonceSize], data[len(magic)+nonceSize:]
	plaintext, err := b.aead.Open(nil, nonce, ct, []byte(name))
	if err != nil {
		return nil, ErrDecrypt
	}
	return plaintext, nil
}

// ParseKey decodes a KeySize key given as hex or base64.
func ParseKey(s string) ([]byte, error) {
	k, err := hex.DecodeString(s)
	if err != nil || len(k) != KeySize {
		if k, err = base64.StdEncoding.DecodeString(s); err != nil || len(k) != KeySize {
			return nil, fmt.Errorf("tritium: key must be %d bytes as hex or base64", KeySize)
		}
	}
	if bytes.Equal(k, make([]byte, KeySize)) {
		return nil, errors.New("tritium: the key is all zeros")
	}
	return k, nil
}

// KeyFromPassphrase derives a key with PBKDF2-SHA256. The salt need not be
// secret, but every client that shares the data must use the same one.
func KeyFromPassphrase(passphrase, salt string) []byte {
	key, err := pbkdf2.Key(sha256.New, passphrase, []byte(salt), 600_000, KeySize)
	if err != nil {
		panic(err) // only for a zero iteration count or key length, which are constants here
	}
	return key
}

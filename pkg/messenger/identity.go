// Package messenger is a secure one-to-one messenger on top of tritium.
//
// Identity is an Ed25519 signing key and an X25519 agreement key. Users
// find each other through a signed bundle published under id:<name>, and
// verify each other by comparing fingerprints out of band; the server is
// never trusted to vouch for a key.
//
// A session starts with an X3DH-style agreement (identity, signed prekey,
// ephemeral) and runs a Double Ratchet from there: every message has its
// own key, and every change of direction mixes in a fresh Diffie-Hellman,
// so a captured state reads nothing sent before it and, once the peer has
// answered again, nothing after it either. The signed prekey rotates weekly
// and retired ones are forgotten after a grace period. Mailboxes are named
// by secrets derived from the session, so nodes cannot tell who is talking
// to whom; first contact goes to a mailbox derived from the recipient's
// public identity, with the sender's identity sealed so the node sees only
// an ephemeral key; ratchet headers are encrypted, so a node cannot count
// messages per direction either. Plaintexts are padded so message sizes
// leak little.
//
// Each message is a tritium key with a TTL, indexed by send time in a
// sorted set per mailbox. Everything expires.
package messenger

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	bundleLabel    = "tritium-messenger-v1 bundle"
	prekeyLifetime = 7 * 24 * time.Hour  // how long one prekey is published before a new one replaces it
	prekeyGrace    = 30 * 24 * time.Hour // how long a replaced prekey still answers hellos made against it
)

var (
	ErrBadBundle     = errors.New("messenger: bundle is malformed or its prekey signature is invalid")
	ErrUnknownPrekey = errors.New("messenger: hello uses a prekey this identity no longer holds")
)

// Identity is a user's private keys: the Ed25519 key that signs the
// published bundle, the X25519 key used in session agreement, and the signed
// prekeys that let others start a session while the user is offline. The
// newest prekey is the published one; replaced ones answer hellos made
// against earlier bundles until their grace period ends.
type Identity struct {
	Name      string
	signing   ed25519.PrivateKey
	agreement *ecdh.PrivateKey
	prekeys   []prekey // newest first
}

type prekey struct {
	key     *ecdh.PrivateKey
	created time.Time
}

func newPrekey(now time.Time) (prekey, error) {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	return prekey{k, now}, err
}

func NewIdentity(name string) (*Identity, error) {
	_, signing, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	agreement, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	pk, err := newPrekey(time.Now())
	if err != nil {
		return nil, err
	}
	return &Identity{Name: name, signing: signing, agreement: agreement, prekeys: []prekey{pk}}, nil
}

// rotate replaces the published prekey once it has served prekeyLifetime and
// forgets replaced ones past prekeyGrace. It reports whether anything
// changed, so the caller knows to store the identity and republish.
func (id *Identity) rotate(now time.Time) bool {
	changed := false
	if now.Sub(id.prekeys[0].created) >= prekeyLifetime {
		pk, err := newPrekey(now)
		if err != nil {
			return false
		}
		id.prekeys = append([]prekey{pk}, id.prekeys...)
		changed = true
	}
	// A prekey's grace runs from when its successor replaced it.
	keep := len(id.prekeys)
	for keep > 1 && now.Sub(id.prekeys[keep-2].created) >= prekeyGrace {
		keep--
	}
	if keep < len(id.prekeys) {
		id.prekeys = id.prekeys[:keep]
		changed = true
	}
	return changed
}

// prekeyFor returns the private half of a prekey we published, or nil once
// it has been forgotten.
func (id *Identity) prekeyFor(pub []byte) *ecdh.PrivateKey {
	for _, pk := range id.prekeys {
		if bytes.Equal(pk.key.PublicKey().Bytes(), pub) {
			return pk.key
		}
	}
	return nil
}

// Bundle is the public half of an identity, signed, as published under
// id:<name>.
type Bundle struct {
	Name      string `json:"name"`
	Signing   []byte `json:"signing"`    // Ed25519 public key
	Agreement []byte `json:"agreement"`  // X25519 public key
	Prekey    []byte `json:"prekey"`     // X25519 public key
	PrekeySig []byte `json:"prekey_sig"` // Ed25519 signature over the name and both public keys
}

// signed is what PrekeySig covers: every public field, so no key or name
// can be swapped for another and still verify.
func (b Bundle) signed() []byte {
	out := append([]byte(bundleLabel), b.Name...)
	out = append(out, 0)
	out = append(out, b.Agreement...)
	return append(out, b.Prekey...)
}

func (id *Identity) Bundle() Bundle {
	b := Bundle{
		Name:      id.Name,
		Signing:   id.signing.Public().(ed25519.PublicKey),
		Agreement: id.agreement.PublicKey().Bytes(),
		Prekey:    id.prekeys[0].key.PublicKey().Bytes(),
	}
	b.PrekeySig = ed25519.Sign(id.signing, b.signed())
	return b
}

func (id *Identity) Fingerprint() string { return id.Bundle().Fingerprint() }

// Verify checks the bundle's shape and that its name and keys were signed
// together by the identity it claims.
func (b Bundle) Verify() error {
	if b.Name == "" || len(b.Signing) != ed25519.PublicKeySize || len(b.Agreement) != 32 || len(b.Prekey) != 32 {
		return ErrBadBundle
	}
	if !ed25519.Verify(ed25519.PublicKey(b.Signing), b.signed(), b.PrekeySig) {
		return ErrBadBundle
	}
	return nil
}

// Fingerprint is the safety number: eight groups of five characters that
// two people compare out of band to be sure no one substituted a key. It
// covers the long-term keys only, so prekey rotation does not change it.
func (b Bundle) Fingerprint() string {
	sum := sha256.Sum256(append(append([]byte{}, b.Signing...), b.Agreement...))
	s := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:40]
	groups := make([]string, 0, 8)
	for i := 0; i < len(s); i += 5 {
		groups = append(groups, s[i:i+5])
	}
	return strings.Join(groups, "-")
}

type identityJSON struct {
	Name      string       `json:"name"`
	Signing   []byte       `json:"signing"`          // Ed25519 seed
	Agreement []byte       `json:"agreement"`        // X25519 private key
	Prekeys   []prekeyJSON `json:"prekeys"`          // newest first
	Prekey    []byte       `json:"prekey,omitempty"` // identities stored before prekeys rotated
}

type prekeyJSON struct {
	Key     []byte    `json:"key"` // X25519 private key
	Created time.Time `json:"created"`
}

// MarshalJSON serializes the private identity for storage. Keep it secret.
func (id *Identity) MarshalJSON() ([]byte, error) {
	j := identityJSON{Name: id.Name, Signing: id.signing.Seed(), Agreement: id.agreement.Bytes()}
	for _, pk := range id.prekeys {
		j.Prekeys = append(j.Prekeys, prekeyJSON{pk.key.Bytes(), pk.created})
	}
	return json.Marshal(j)
}

func (id *Identity) UnmarshalJSON(data []byte) error {
	var j identityJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	if len(j.Signing) != ed25519.SeedSize {
		return fmt.Errorf("messenger: identity: bad signing seed length %d", len(j.Signing))
	}
	agreement, err := ecdh.X25519().NewPrivateKey(j.Agreement)
	if err != nil {
		return fmt.Errorf("messenger: identity: %w", err)
	}
	if len(j.Prekeys) == 0 && j.Prekey != nil {
		j.Prekeys = []prekeyJSON{{j.Prekey, time.Now()}}
	}
	if len(j.Prekeys) == 0 {
		return errors.New("messenger: identity: no prekey")
	}
	var prekeys []prekey
	for _, p := range j.Prekeys {
		k, err := ecdh.X25519().NewPrivateKey(p.Key)
		if err != nil {
			return fmt.Errorf("messenger: identity: %w", err)
		}
		prekeys = append(prekeys, prekey{k, p.Created})
	}
	*id = Identity{Name: j.Name, signing: ed25519.NewKeyFromSeed(j.Signing), agreement: agreement, prekeys: prekeys}
	return nil
}

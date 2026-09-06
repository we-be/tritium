// Package messenger is a secure one-to-one messenger on top of tritium.
//
// Identity is an Ed25519 signing key and an X25519 agreement key. Users
// find each other through a signed bundle published under id:<name>, and
// verify each other by comparing fingerprints out of band; the server is
// never trusted to vouch for a key.
//
// A session starts with an X3DH-style agreement (identity, signed prekey,
// ephemeral) whose root secret seeds two symmetric hash ratchets, one per
// direction, so every message has its own key and a captured key reveals
// nothing sent before it. Mailboxes are named by secrets derived from the
// session, so nodes cannot tell who is talking to whom; first contact goes
// to a mailbox derived from the recipient's public identity.
//
// Each message is a tritium key with a TTL, indexed by send time in a
// sorted set per mailbox. Everything expires.
package messenger

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const prekeyLabel = "tritium-messenger-v1 prekey"

var ErrBadBundle = errors.New("messenger: bundle is malformed or its prekey signature is invalid")

// Identity is a user's private keys: the Ed25519 key that signs the
// published bundle, the X25519 key used in session agreement, and the
// current signed prekey that lets others start a session while the user is
// offline.
type Identity struct {
	Name      string
	signing   ed25519.PrivateKey
	agreement *ecdh.PrivateKey
	prekey    *ecdh.PrivateKey
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
	prekey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{Name: name, signing: signing, agreement: agreement, prekey: prekey}, nil
}

// Bundle is the public half of an identity, signed, as published under
// id:<name>.
type Bundle struct {
	Name      string `json:"name"`
	Signing   []byte `json:"signing"`    // Ed25519 public key
	Agreement []byte `json:"agreement"`  // X25519 public key
	Prekey    []byte `json:"prekey"`     // X25519 public key
	PrekeySig []byte `json:"prekey_sig"` // Ed25519 signature of prekeyLabel || Prekey
}

func (id *Identity) Bundle() Bundle {
	prekey := id.prekey.PublicKey().Bytes()
	return Bundle{
		Name:      id.Name,
		Signing:   id.signing.Public().(ed25519.PublicKey),
		Agreement: id.agreement.PublicKey().Bytes(),
		Prekey:    prekey,
		PrekeySig: ed25519.Sign(id.signing, append([]byte(prekeyLabel), prekey...)),
	}
}

func (id *Identity) Fingerprint() string { return id.Bundle().Fingerprint() }

// Verify checks the bundle's shape and that the prekey was signed by the
// identity it claims.
func (b Bundle) Verify() error {
	if len(b.Signing) != ed25519.PublicKeySize || len(b.Agreement) != 32 || len(b.Prekey) != 32 {
		return ErrBadBundle
	}
	if !ed25519.Verify(ed25519.PublicKey(b.Signing), append([]byte(prekeyLabel), b.Prekey...), b.PrekeySig) {
		return ErrBadBundle
	}
	return nil
}

// Fingerprint is the safety number: eight groups of five characters that
// two people compare out of band to be sure no one substituted a key.
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
	Name      string `json:"name"`
	Signing   []byte `json:"signing"`   // Ed25519 seed
	Agreement []byte `json:"agreement"` // X25519 private key
	Prekey    []byte `json:"prekey"`    // X25519 private key
}

// MarshalJSON serializes the private identity for storage. Keep it secret.
func (id *Identity) MarshalJSON() ([]byte, error) {
	return json.Marshal(identityJSON{id.Name, id.signing.Seed(), id.agreement.Bytes(), id.prekey.Bytes()})
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
	prekey, err := ecdh.X25519().NewPrivateKey(j.Prekey)
	if err != nil {
		return fmt.Errorf("messenger: identity: %w", err)
	}
	*id = Identity{Name: j.Name, signing: ed25519.NewKeyFromSeed(j.Signing), agreement: agreement, prekey: prekey}
	return nil
}

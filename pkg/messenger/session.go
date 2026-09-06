package messenger

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

const (
	protocol       = "tritium-messenger-v1"
	maxSkip        = 1000 // messages a receiver will derive keys for in one gap
	maxSkippedKeys = 2000 // keys kept for messages that never arrived; oldest go first
)

var (
	ErrReplay      = errors.New("messenger: message already received")
	ErrTooFarAhead = errors.New("messenger: too many messages missing before this one")
	ErrDecrypt     = errors.New("messenger: decryption failed")
)

// chain is one direction of a symmetric hash ratchet. Each step yields a
// fresh message key and replaces the chain key, so a captured chain key
// reveals nothing sent before it.
type chain struct {
	Key []byte `json:"key"`
	N   uint32 `json:"n"`
}

func (c *chain) step() []byte {
	mk := mac(c.Key, 1)
	c.Key = mac(c.Key, 2)
	c.N++
	return mk
}

func mac(key []byte, label byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte{label})
	return m.Sum(nil)
}

// Session is everything shared with one peer.
type Session struct {
	Peer          Bundle            `json:"peer"`
	Send          chain             `json:"send"`
	Recv          chain             `json:"recv"`
	Skipped       map[uint32][]byte `json:"skipped,omitempty"` // keys for messages that arrived out of order
	Outbox        string            `json:"outbox"`            // mailbox we post to
	Inbox         string            `json:"inbox"`             // mailbox we poll
	Cursor        float64           `json:"cursor"`            // inbox score last consumed
	Seen          []string          `json:"seen,omitempty"`    // ids consumed at Cursor
	Hello         *helloHeader      `json:"hello,omitempty"`   // our opening keys, sent until the peer answers
	PeerEphemeral []byte            `json:"peer_ephemeral,omitempty"`
}

// helloHeader rides on every message the initiator sends until the peer
// answers, so the peer can derive the session even if earlier copies expired.
type helloHeader struct {
	Bundle    Bundle `json:"bundle"`
	Ephemeral []byte `json:"ek"`
}

// initiate runs the initiator's side of the agreement against peer's bundle.
func initiate(me *Identity, peer Bundle) (*Session, error) {
	ek, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	peerIK, err := ecdh.X25519().NewPublicKey(peer.Agreement)
	if err != nil {
		return nil, err
	}
	peerSPK, err := ecdh.X25519().NewPublicKey(peer.Prekey)
	if err != nil {
		return nil, err
	}
	dh1, err := me.agreement.ECDH(peerSPK)
	if err != nil {
		return nil, err
	}
	dh2, err := ek.ECDH(peerIK)
	if err != nil {
		return nil, err
	}
	dh3, err := ek.ECDH(peerSPK)
	if err != nil {
		return nil, err
	}
	s, err := derive(dh1, dh2, dh3, me.Bundle(), peer, true)
	if err != nil {
		return nil, err
	}
	s.Hello = &helloHeader{Bundle: me.Bundle(), Ephemeral: ek.PublicKey().Bytes()}
	return s, nil
}

// respond runs the responder's side from an initiator's hello header.
func respond(me *Identity, h helloHeader) (*Session, error) {
	if err := h.Bundle.Verify(); err != nil {
		return nil, err
	}
	peerIK, err := ecdh.X25519().NewPublicKey(h.Bundle.Agreement)
	if err != nil {
		return nil, err
	}
	peerEK, err := ecdh.X25519().NewPublicKey(h.Ephemeral)
	if err != nil {
		return nil, err
	}
	dh1, err := me.prekey.ECDH(peerIK)
	if err != nil {
		return nil, err
	}
	dh2, err := me.agreement.ECDH(peerEK)
	if err != nil {
		return nil, err
	}
	dh3, err := me.prekey.ECDH(peerEK)
	if err != nil {
		return nil, err
	}
	s, err := derive(dh1, dh2, dh3, h.Bundle, me.Bundle(), false)
	if err != nil {
		return nil, err
	}
	s.PeerEphemeral = h.Ephemeral
	return s, nil
}

// derive turns the three shared secrets into chain keys and mailbox names.
// The initiator's send chain is the responder's receive chain and vice versa.
func derive(dh1, dh2, dh3 []byte, initiator, responder Bundle, amInitiator bool) (*Session, error) {
	secret := append(append(append([]byte{}, dh1...), dh2...), dh3...)
	okm, err := hkdf.Key(sha256.New, secret, make([]byte, 32), protocol+" root", 96)
	if err != nil {
		return nil, err
	}
	toResponder, toInitiator, seed := okm[:32], okm[32:64], okm[64:]
	iFP, rFP := initiator.Fingerprint(), responder.Fingerprint()
	mbxToResponder, mbxToInitiator := mailbox(seed, iFP+">"+rFP), mailbox(seed, rFP+">"+iFP)

	s := &Session{Skipped: map[uint32][]byte{}}
	if amInitiator {
		s.Peer = responder
		s.Send, s.Recv = chain{Key: toResponder}, chain{Key: toInitiator}
		s.Outbox, s.Inbox = mbxToResponder, mbxToInitiator
	} else {
		s.Peer = initiator
		s.Send, s.Recv = chain{Key: toInitiator}, chain{Key: toResponder}
		s.Outbox, s.Inbox = mbxToInitiator, mbxToResponder
	}
	return s, nil
}

func mailbox(seed []byte, info string) string {
	k, err := hkdf.Key(sha256.New, seed, nil, protocol+" mailbox "+info, 16)
	if err != nil {
		panic(err) // constant lengths; cannot fail
	}
	return "mbx:" + hex.EncodeToString(k)
}

// seal encrypts plaintext with the next sending key. aad binds the
// ciphertext to where it is stored and its position in the chain.
func (s *Session) seal(plaintext, aad []byte) ([]byte, error) {
	return aead(s.Send.step(), plaintext, aad, true)
}

// open decrypts message n, deriving and keeping keys for any messages
// skipped on the way so they can still be read when they arrive.
func (s *Session) open(n uint32, ct, aad []byte) ([]byte, error) {
	var mk []byte
	switch {
	case n < s.Recv.N:
		if mk = s.Skipped[n]; mk == nil {
			return nil, ErrReplay
		}
	case n-s.Recv.N > maxSkip:
		return nil, ErrTooFarAhead
	default:
		for s.Recv.N < n {
			skipped := s.Recv.N // read before step advances it; Go leaves the order unspecified otherwise
			s.Skipped[skipped] = s.Recv.step()
		}
		mk = s.Recv.step()
	}
	pt, err := aead(mk, ct, aad, false)
	if err != nil {
		s.Skipped[n] = mk // a genuine copy may still turn up
		s.pruneSkipped()
		return nil, ErrDecrypt
	}
	delete(s.Skipped, n)
	s.pruneSkipped()
	return pt, nil
}

// pruneSkipped forgets the oldest skipped keys once there are too many, so
// a peer or a hostile node cannot make the session hoard keys forever.
func (s *Session) pruneSkipped() {
	for len(s.Skipped) > maxSkippedKeys {
		oldest := ^uint32(0)
		for n := range s.Skipped {
			oldest = min(oldest, n)
		}
		delete(s.Skipped, oldest)
	}
}

// aead seals or opens with AES-256-GCM under a key and nonce derived from
// the message key. Message keys are never reused, so a derived nonce is safe.
func aead(mk, in, aad []byte, encrypt bool) ([]byte, error) {
	km, err := hkdf.Key(sha256.New, mk, nil, protocol+" message", 44)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(km[:32])
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if encrypt {
		return g.Seal(nil, km[32:], in, aad), nil
	}
	return g.Open(nil, km[32:], in, aad)
}

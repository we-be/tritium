package messenger

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"time"
)

const (
	protocol       = "tritium-messenger-v2"
	padBlock       = 160  // plaintexts are padded to a multiple of this, so a size reveals only its bucket
	maxSkip        = 1000 // messages a receiver will derive keys for in one gap
	maxSkippedKeys = 2000 // keys kept for messages that never arrived; oldest go first
)

var (
	ErrReplay      = errors.New("messenger: message already received")
	ErrTooFarAhead = errors.New("messenger: too many messages missing before this one")
	ErrDecrypt     = errors.New("messenger: decryption failed")
	ErrNotYet      = errors.New("messenger: nothing received from the peer yet, so nothing to send with")
)

// Session is everything shared with one peer: a Double Ratchet. The root key
// advances with a fresh Diffie-Hellman every time the conversation changes
// direction, and each direction's chain key advances per message, so a
// captured state reads nothing sent before it and, once the peer has
// answered again, nothing sent after it either.
type Session struct {
	Peer          Bundle            `json:"peer"`
	Root          []byte            `json:"rk"`
	Ratchet       []byte            `json:"dhs"`           // our current ratchet private key
	PeerRatchet   []byte            `json:"dhr,omitempty"` // the peer's current ratchet public key
	SendChain     []byte            `json:"cks,omitempty"`
	RecvChain     []byte            `json:"ckr,omitempty"`
	Ns            uint32            `json:"ns"`
	Nr            uint32            `json:"nr"`
	PN            uint32            `json:"pn"`                // length of our previous sending chain
	Skipped       map[string][]byte `json:"skipped,omitempty"` // message keys for messages that arrived out of order, by ratchet key and number
	Outbox        string            `json:"outbox"`            // mailbox we post to
	Inbox         string            `json:"inbox"`             // mailbox we poll
	Seen          []string          `json:"seen,omitempty"`    // inbox entries read by the last Receive, deleted by the next
	Hello         *helloHeader      `json:"hello,omitempty"`   // our opening keys, sent until the peer answers
	PeerEphemeral []byte            `json:"peer_ephemeral,omitempty"`
	Touched       time.Time         `json:"touched,omitzero"` // last send or successful receive; Prune uses it
}

// helloHeader is what first contact carries: the initiator's bundle, its
// ephemeral key, and which of the peer's prekeys it agreed against. On the
// wire it travels sealed under a key only the recipient can derive, so a
// node sees an ephemeral public key and nothing about who is writing.
type helloHeader struct {
	Bundle       Bundle `json:"bundle"`
	Ephemeral    []byte `json:"ek"`
	EphemeralKey []byte `json:"ek_priv,omitempty"` // ours, kept to seal repeats; never sent
	Prekey       []byte `json:"spk"`               // the peer's prekey this hello was made against
}

// ratchetHeader rides on every message, authenticated with it.
type ratchetHeader struct {
	DH []byte `json:"dh"` // the sender's current ratchet public key
	PN uint32 `json:"pn"` // messages in the sender's previous chain
	N  uint32 `json:"n"`  // this message's number in the current chain
}

func (h ratchetHeader) bytes() []byte {
	out := make([]byte, 0, len(h.DH)+8)
	out = append(out, h.DH...)
	out = binary.BigEndian.AppendUint32(out, h.PN)
	return binary.BigEndian.AppendUint32(out, h.N)
}

// initiate runs the initiator's side of the agreement against peer's bundle
// and takes the first ratchet step against the peer's prekey.
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
	dhs, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	s.Ratchet, s.PeerRatchet = dhs.Bytes(), peer.Prekey
	shared, err := dhs.ECDH(peerSPK)
	if err != nil {
		return nil, err
	}
	s.Root, s.SendChain = kdfRoot(s.Root, shared)
	s.Hello = &helloHeader{Bundle: me.Bundle(), Ephemeral: ek.PublicKey().Bytes(), EphemeralKey: ek.Bytes(), Prekey: peer.Prekey}
	return s, nil
}

// respond runs the responder's side from an initiator's hello header. The
// prekey the initiator used becomes our first ratchet key; the first message
// we open ratchets past it.
func respond(me *Identity, h helloHeader) (*Session, error) {
	if err := h.Bundle.Verify(); err != nil {
		return nil, err
	}
	spk := me.prekeyFor(h.Prekey)
	if spk == nil {
		return nil, ErrUnknownPrekey
	}
	peerIK, err := ecdh.X25519().NewPublicKey(h.Bundle.Agreement)
	if err != nil {
		return nil, err
	}
	peerEK, err := ecdh.X25519().NewPublicKey(h.Ephemeral)
	if err != nil {
		return nil, err
	}
	dh1, err := spk.ECDH(peerIK)
	if err != nil {
		return nil, err
	}
	dh2, err := me.agreement.ECDH(peerEK)
	if err != nil {
		return nil, err
	}
	dh3, err := spk.ECDH(peerEK)
	if err != nil {
		return nil, err
	}
	s, err := derive(dh1, dh2, dh3, h.Bundle, me.Bundle(), false)
	if err != nil {
		return nil, err
	}
	s.Ratchet = spk.Bytes()
	s.PeerEphemeral = h.Ephemeral
	return s, nil
}

// derive turns the three shared secrets into the root key and the mailbox
// names. The initiator posts to the responder's inbox and vice versa.
func derive(dh1, dh2, dh3 []byte, initiator, responder Bundle, amInitiator bool) (*Session, error) {
	secret := append(append(append([]byte{}, dh1...), dh2...), dh3...)
	okm, err := hkdf.Key(sha256.New, secret, make([]byte, 32), protocol+" root", 64)
	if err != nil {
		return nil, err
	}
	root, seed := okm[:32], okm[32:]
	iFP, rFP := initiator.Fingerprint(), responder.Fingerprint()
	mbxToResponder, mbxToInitiator := mailbox(seed, iFP+">"+rFP), mailbox(seed, rFP+">"+iFP)

	s := &Session{Root: root, Skipped: map[string][]byte{}}
	if amInitiator {
		s.Peer = responder
		s.Outbox, s.Inbox = mbxToResponder, mbxToInitiator
	} else {
		s.Peer = initiator
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

// kdfRoot mixes a fresh Diffie-Hellman output into the root key and starts a
// new chain from it.
func kdfRoot(root, shared []byte) (newRoot, chain []byte) {
	okm, err := hkdf.Key(sha256.New, shared, root, protocol+" ratchet", 64)
	if err != nil {
		panic(err) // constant lengths; cannot fail
	}
	return okm[:32], okm[32:]
}

// kdfChain yields the next message key and advances the chain.
func kdfChain(chain *[]byte) (mk []byte) {
	mk = mac(*chain, 1)
	*chain = mac(*chain, 2)
	return mk
}

func mac(key []byte, label byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte{label})
	return m.Sum(nil)
}

func (s *Session) ratchetKey() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().NewPrivateKey(s.Ratchet)
}

// seal pads and encrypts plaintext with the next sending key. aad binds the
// ciphertext to where it is stored; the header is bound with it.
func (s *Session) seal(plaintext, aad []byte) (ratchetHeader, []byte, error) {
	if s.SendChain == nil {
		return ratchetHeader{}, nil, ErrNotYet
	}
	dhs, err := s.ratchetKey()
	if err != nil {
		return ratchetHeader{}, nil, err
	}
	h := ratchetHeader{DH: dhs.PublicKey().Bytes(), PN: s.PN, N: s.Ns}
	mk := kdfChain(&s.SendChain)
	s.Ns++
	ct, err := aead(mk, pad(plaintext), append(aad, h.bytes()...), true)
	return h, ct, err
}

// open decrypts a message, ratcheting when the peer's key changed and
// keeping keys for anything skipped on the way. All state moves on a copy
// and is committed only when the ciphertext verifies: a forged header must
// never leave a real session unable to read its peer.
func (s *Session) open(h ratchetHeader, ct, aad []byte) ([]byte, error) {
	aad = append(aad, h.bytes()...)
	if mk, ok := s.Skipped[skipKey(h.DH, h.N)]; ok {
		pt, err := aead(mk, ct, aad, false)
		if err != nil {
			return nil, ErrDecrypt
		}
		delete(s.Skipped, skipKey(h.DH, h.N))
		return unpad(pt)
	}
	t := *s
	pending := map[string][]byte{}
	if !bytes.Equal(h.DH, t.PeerRatchet) {
		if t.RecvChain != nil { // finish the peer's previous chain first
			if err := t.skipTo(h.PN, pending); err != nil {
				return nil, err
			}
		}
		if err := t.dhRatchet(h.DH); err != nil {
			return nil, err
		}
	} else if h.N < t.Nr {
		return nil, ErrReplay
	}
	if err := t.skipTo(h.N, pending); err != nil {
		return nil, err
	}
	mk := kdfChain(&t.RecvChain)
	t.Nr++
	pt, err := aead(mk, ct, aad, false)
	if err != nil {
		return nil, ErrDecrypt
	}
	*s = t
	maps.Copy(s.Skipped, pending)
	s.pruneSkipped()
	return unpad(pt)
}

// skipTo derives and sets aside the keys for messages Nr..n-1 of the current
// receiving chain, so they can still be read when they arrive.
func (t *Session) skipTo(n uint32, pending map[string][]byte) error {
	if n > t.Nr && n-t.Nr > maxSkip {
		return ErrTooFarAhead
	}
	for t.Nr < n {
		if t.RecvChain == nil {
			return ErrDecrypt
		}
		pending[skipKey(t.PeerRatchet, t.Nr)] = kdfChain(&t.RecvChain)
		t.Nr++
	}
	return nil
}

// dhRatchet adopts the peer's new ratchet key: a fresh receiving chain from
// it, then a fresh key pair of our own and a fresh sending chain.
func (t *Session) dhRatchet(peerKey []byte) error {
	dhs, err := t.ratchetKey()
	if err != nil {
		return err
	}
	pub, err := ecdh.X25519().NewPublicKey(peerKey)
	if err != nil {
		return ErrDecrypt
	}
	shared, err := dhs.ECDH(pub)
	if err != nil {
		return ErrDecrypt
	}
	t.PN, t.Ns, t.Nr, t.PeerRatchet = t.Ns, 0, 0, peerKey
	t.Root, t.RecvChain = kdfRoot(t.Root, shared)
	next, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	shared, err = next.ECDH(pub)
	if err != nil {
		return ErrDecrypt
	}
	t.Ratchet = next.Bytes()
	t.Root, t.SendChain = kdfRoot(t.Root, shared)
	return nil
}

func skipKey(dh []byte, n uint32) string {
	return hex.EncodeToString(dh[:min(8, len(dh))]) + "/" + hex.EncodeToString(binary.BigEndian.AppendUint32(nil, n))
}

// pruneSkipped forgets the oldest skipped keys once there are too many, so a
// peer or a hostile node cannot make the session hoard keys forever. Oldest
// is by key order, which sorts by ratchet key then number: good enough for
// a bound, exact order does not matter.
func (s *Session) pruneSkipped() {
	for len(s.Skipped) > maxSkippedKeys {
		oldest := ""
		for k := range s.Skipped {
			if oldest == "" || k < oldest {
				oldest = k
			}
		}
		delete(s.Skipped, oldest)
	}
}

// sealHello encrypts the hello for the recipient under DH(ek, IK_recipient),
// a key only the two of them can derive, bound to the mailbox it lands in.
func (h *helloHeader) seal(recipient Bundle, mailbox string) ([]byte, error) {
	ek, err := ecdh.X25519().NewPrivateKey(h.EphemeralKey)
	if err != nil {
		return nil, err
	}
	ik, err := ecdh.X25519().NewPublicKey(recipient.Agreement)
	if err != nil {
		return nil, err
	}
	shared, err := ek.ECDH(ik)
	if err != nil {
		return nil, err
	}
	pt, err := json.Marshal(helloHeader{Bundle: h.Bundle, Prekey: h.Prekey})
	if err != nil {
		return nil, err
	}
	return aead(helloKey(shared), pt, []byte(mailbox), true)
}

// unsealHello is the recipient's side: with its identity key and the
// ephemeral key from the envelope it recovers who is writing.
func unsealHello(me *Identity, ephemeral, sealed []byte, mailbox string) (helloHeader, error) {
	ek, err := ecdh.X25519().NewPublicKey(ephemeral)
	if err != nil {
		return helloHeader{}, ErrDecrypt
	}
	shared, err := me.agreement.ECDH(ek)
	if err != nil {
		return helloHeader{}, ErrDecrypt
	}
	pt, err := aead(helloKey(shared), sealed, []byte(mailbox), false)
	if err != nil {
		return helloHeader{}, ErrDecrypt
	}
	var h helloHeader
	if err := json.Unmarshal(pt, &h); err != nil {
		return helloHeader{}, ErrDecrypt
	}
	h.Ephemeral = ephemeral
	return h, nil
}

func helloKey(shared []byte) []byte {
	k, err := hkdf.Key(sha256.New, shared, nil, protocol+" hello", 32)
	if err != nil {
		panic(err) // constant lengths; cannot fail
	}
	return k
}

// pad appends 0x80 and zeros up to the next multiple of padBlock.
func pad(pt []byte) []byte {
	out := make([]byte, (len(pt)+padBlock)/padBlock*padBlock)
	copy(out, pt)
	out[len(pt)] = 0x80
	return out
}

func unpad(pt []byte) ([]byte, error) {
	i := len(pt) - 1
	for i >= 0 && pt[i] == 0 {
		i--
	}
	if i < 0 || pt[i] != 0x80 {
		return nil, ErrDecrypt
	}
	return pt[:i], nil
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

package messenger

import (
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
	protocol       = "tritium-messenger-v3"
	padBlock       = 160  // plaintexts are padded to a multiple of this, so a size reveals only its bucket
	maxSkip        = 1000 // messages a receiver will derive keys for in one gap
	maxSkippedKeys = 2000 // keys kept for messages that never arrived; oldest go first
)

// What a message is refused for.
var (
	ErrReplay      = errors.New("messenger: message already received")
	ErrTooFarAhead = errors.New("messenger: too many messages missing before this one")
	ErrDecrypt     = errors.New("messenger: decryption failed")
	ErrNotYet      = errors.New("messenger: nothing received from the peer yet, so nothing to send with")
)

const sessionFormat = 4 // bumped with the wire format; older sessions are dropped on restore

// Session is everything shared with one peer: a Double Ratchet with header
// encryption. The root key advances with a fresh Diffie-Hellman every time
// the conversation changes direction, and each direction's chain key
// advances per message, so a captured state reads nothing sent before it
// and, once the peer has answered again, nothing sent after it either. The
// ratchet header travels encrypted under a per-direction header key that
// advances with the root, so a node cannot even count messages per chain.
type Session struct {
	Format        int                `json:"fmt"`            // the wire format it speaks; an older one is dropped on restore
	Peer          Bundle             `json:"peer"`           // who it is with
	Root          []byte             `json:"rk"`             // root key: advanced by every Diffie-Hellman ratchet step
	Ratchet       []byte             `json:"dhs"`            // our current ratchet private key
	PeerRatchet   []byte             `json:"dhr,omitempty"`  // the peer's current ratchet public key
	SendChain     []byte             `json:"cks,omitempty"`  // chain key for what we send, advanced per message
	RecvChain     []byte             `json:"ckr,omitempty"`  // chain key for what we receive
	SendHeader    []byte             `json:"hks,omitempty"`  // header key for what we send
	RecvHeader    []byte             `json:"hkr,omitempty"`  // header key for what we receive
	NextSend      []byte             `json:"nhks,omitempty"` // header keys for after the next ratchet step
	NextRecv      []byte             `json:"nhkr,omitempty"`
	Ns            uint32             `json:"ns"`                       // messages sent in the current chain
	Nr            uint32             `json:"nr"`                       // messages received in the current chain
	PN            uint32             `json:"pn"`                       // length of our previous sending chain
	Skipped       map[string]skipped `json:"skipped,omitempty"`        // message keys for messages that arrived out of order, by header key and number
	Outbox        string             `json:"outbox"`                   // mailbox we post to
	Inbox         string             `json:"inbox"`                    // mailbox we poll
	Seen          []string           `json:"seen,omitempty"`           // inbox entries read by the last Receive, deleted by the next
	Hello         *helloHeader       `json:"hello,omitempty"`          // our opening keys, sent until the peer answers
	PeerEphemeral []byte             `json:"peer_ephemeral,omitempty"` // the ephemeral key the peer's hello carried, so the same hello resent is known
	Touched       time.Time          `json:"touched,omitzero"`         // last send or successful receive; Prune uses it
	Checked       bool               `json:"checked,omitempty"`        // whether Verified has been settled against id:<name>
	Verified      bool               `json:"verified,omitempty"`       // Peer.Name is published under Peer's fingerprint
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

// skipped is a message key set aside for a message that has not arrived,
// with the header key that will identify it when it does.
type skipped struct {
	HK []byte `json:"hk"`
	MK []byte `json:"mk"`
}

// ratchetHeader rides on every message, encrypted under the header key and
// authenticated with the ciphertext.
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

func parseHeader(b []byte) (ratchetHeader, bool) {
	if len(b) != 40 {
		return ratchetHeader{}, false
	}
	return ratchetHeader{DH: b[:32], PN: binary.BigEndian.Uint32(b[32:36]), N: binary.BigEndian.Uint32(b[36:40])}, true
}

// hencrypt seals a header under a header key. Header keys serve a whole
// chain, so the nonce is random and travels in front.
func hencrypt(hk []byte, h ratchetHeader) ([]byte, error) {
	block, err := aes.NewCipher(hk)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, g.NonceSize())
	rand.Read(nonce)
	return g.Seal(nonce, nonce, h.bytes(), nil), nil
}

func hdecrypt(hk, eh []byte) (ratchetHeader, bool) {
	if hk == nil {
		return ratchetHeader{}, false
	}
	block, err := aes.NewCipher(hk)
	if err != nil {
		return ratchetHeader{}, false
	}
	g, err := cipher.NewGCM(block)
	if err != nil || len(eh) < g.NonceSize() {
		return ratchetHeader{}, false
	}
	pt, err := g.Open(nil, eh[:g.NonceSize()], eh[g.NonceSize():], nil)
	if err != nil {
		return ratchetHeader{}, false
	}
	return parseHeader(pt)
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
	s.SendHeader = s.NextSend // the agreement's first header key guards our first chain
	s.Root, s.SendChain, s.NextSend = kdfRoot(s.Root, shared)
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
	s.NextSend, s.NextRecv = s.NextRecv, s.NextSend // mirror of the initiator's view
	s.PeerEphemeral = h.Ephemeral
	return s, nil
}

// derive turns the three shared secrets into the root key and the mailbox
// names. The initiator posts to the responder's inbox and vice versa.
func derive(dh1, dh2, dh3 []byte, initiator, responder Bundle, amInitiator bool) (*Session, error) {
	secret := append(append(append([]byte{}, dh1...), dh2...), dh3...)
	okm, err := hkdf.Key(sha256.New, secret, make([]byte, 32), protocol+" root", 128)
	if err != nil {
		return nil, err
	}
	root, seed, hka, nhkb := okm[:32], okm[32:64], okm[64:96], okm[96:]
	iFP, rFP := initiator.Fingerprint(), responder.Fingerprint()
	mbxToResponder, mbxToInitiator := mailbox(seed, iFP+">"+rFP), mailbox(seed, rFP+">"+iFP)

	// as the initiator sees them: hka guards its first chain, nhkb the responder's first
	s := &Session{Format: sessionFormat, Root: root, NextSend: hka, NextRecv: nhkb, Skipped: map[string]skipped{}}
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
// new chain from it, with the header key that chain's successor will use.
func kdfRoot(root, shared []byte) (newRoot, chain, nextHeader []byte) {
	okm, err := hkdf.Key(sha256.New, shared, root, protocol+" ratchet", 96)
	if err != nil {
		panic(err) // constant lengths; cannot fail
	}
	return okm[:32], okm[32:64], okm[64:]
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

// seal pads and encrypts plaintext with the next sending key, and the header
// under the sending header key. aad binds the ciphertext to where it is
// stored; the encrypted header is bound with it.
func (s *Session) seal(plaintext, aad []byte) (encHeader, ct []byte, err error) {
	if s.SendChain == nil || s.SendHeader == nil {
		return nil, nil, ErrNotYet
	}
	dhs, err := s.ratchetKey()
	if err != nil {
		return nil, nil, err
	}
	h := ratchetHeader{DH: dhs.PublicKey().Bytes(), PN: s.PN, N: s.Ns}
	if encHeader, err = hencrypt(s.SendHeader, h); err != nil {
		return nil, nil, err
	}
	mk := kdfChain(&s.SendChain)
	s.Ns++
	ct, err = aead(mk, pad(plaintext), append(aad, encHeader...), true)
	return encHeader, ct, err
}

// open decrypts a message: the header first, under the current receiving
// header key (same chain) or the next one (the peer ratcheted), then the
// body, ratcheting and keeping keys for anything skipped on the way. All
// state moves on a copy and is committed only when the ciphertext
// verifies: a forged header must never leave a real session unable to
// read its peer.
func (s *Session) open(encHeader, ct, aad []byte) (pt []byte, n uint32, err error) {
	aad = append(aad, encHeader...)
	// a message skipped earlier: its header opens under a header key we set a key aside for
	tried := map[string]bool{}
	for _, sk := range s.Skipped {
		id := hex.EncodeToString(sk.HK[:8])
		if tried[id] {
			continue
		}
		tried[id] = true
		h, ok := hdecrypt(sk.HK, encHeader)
		if !ok {
			continue
		}
		e, ok := s.Skipped[skipKey(sk.HK, h.N)]
		if !ok { // that chain, but not a message we set a key aside for: the live path decides
			continue
		}
		pt, err := aead(e.MK, ct, aad, false)
		if err != nil {
			return nil, 0, ErrDecrypt
		}
		delete(s.Skipped, skipKey(sk.HK, h.N))
		pt, err = unpad(pt)
		return pt, h.N, err
	}
	t := *s
	pending := map[string]skipped{}
	h, ok := hdecrypt(t.RecvHeader, encHeader)
	if ok {
		if h.N < t.Nr {
			return nil, 0, ErrReplay
		}
	} else {
		if h, ok = hdecrypt(t.NextRecv, encHeader); !ok {
			return nil, 0, ErrDecrypt
		}
		if t.RecvChain != nil { // finish the peer's previous chain first
			if err := t.skipTo(h.PN, pending); err != nil {
				return nil, 0, err
			}
		}
		if err := t.dhRatchet(h.DH); err != nil {
			return nil, 0, err
		}
	}
	if err := t.skipTo(h.N, pending); err != nil {
		return nil, 0, err
	}
	mk := kdfChain(&t.RecvChain)
	t.Nr++
	pt, err = aead(mk, ct, aad, false)
	if err != nil {
		return nil, 0, ErrDecrypt
	}
	*s = t
	maps.Copy(s.Skipped, pending)
	s.pruneSkipped()
	pt, err = unpad(pt)
	return pt, h.N, err
}

// skipTo derives and sets aside the keys for messages Nr..n-1 of the current
// receiving chain, so they can still be read when they arrive.
func (t *Session) skipTo(n uint32, pending map[string]skipped) error {
	if n > t.Nr && n-t.Nr > maxSkip {
		return ErrTooFarAhead
	}
	for t.Nr < n {
		if t.RecvChain == nil {
			return ErrDecrypt
		}
		pending[skipKey(t.RecvHeader, t.Nr)] = skipped{HK: t.RecvHeader, MK: kdfChain(&t.RecvChain)}
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
	t.SendHeader, t.RecvHeader = t.NextSend, t.NextRecv
	t.Root, t.RecvChain, t.NextRecv = kdfRoot(t.Root, shared)
	next, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	shared, err = next.ECDH(pub)
	if err != nil {
		return ErrDecrypt
	}
	t.Ratchet = next.Bytes()
	t.Root, t.SendChain, t.NextSend = kdfRoot(t.Root, shared)
	return nil
}

func skipKey(hk []byte, n uint32) string {
	return hex.EncodeToString(hk[:min(8, len(hk))]) + "/" + hex.EncodeToString(binary.BigEndian.AppendUint32(nil, n))
}

// pruneSkipped forgets the oldest skipped keys once there are too many, so a
// peer or a hostile node cannot make the session hoard keys forever. Oldest
// is by key order, which sorts by header key then number: good enough for
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

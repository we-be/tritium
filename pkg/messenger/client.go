package messenger

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/we-be/tritium/pkg/tritium"
)

const (
	bundleTTL  = 30 * 24 * 3600 // seconds a published bundle lives without a refresh
	fetchBatch = 100
	version    = 2 // envelope format: padded plaintext, hello names the prekey it used
)

var ErrNameTaken = errors.New("messenger: name is registered to another identity")

// Client is one user's messenger: their identity, their sessions, and a
// tritium connection. The tritium client must not have its own Key set;
// the messenger does its own sealing.
type Client struct {
	t         *tritium.Client
	id        *Identity
	TTL       int                 // seconds a message lives on the server; 0 uses the node's default
	sessions  map[string]*Session // by peer fingerprint
	helloSeen []string            // hello mailbox entries read by the last Receive, deleted by the next
}

func New(t *tritium.Client, id *Identity) *Client {
	return &Client{t: t, id: id, sessions: map[string]*Session{}}
}

// Message is a decrypted message from a peer.
type Message struct {
	From Bundle
	Time time.Time
	Body []byte
}

// envelope is the stored form of a message.
type envelope struct {
	V     int          `json:"v"`
	Hello *helloHeader `json:"hello,omitempty"`
	N     uint32       `json:"n"`
	CT    []byte       `json:"ct"`
}

// Publish registers the identity's bundle under id:<name>, rotating the
// prekey first when it is due. The first identity to claim a name on a node
// keeps it; a bundle expires after bundleTTL unless republished. Call it
// regularly and store the Identity afterwards, since rotation changes it.
func (c *Client) Publish() error {
	c.id.rotate(time.Now())
	b, err := json.Marshal(c.id.Bundle())
	if err != nil {
		return err
	}
	key := "id:" + c.id.Name
	v, err := c.t.Do("SET", key, string(b), "EX", strconv.Itoa(bundleTTL), "NX")
	if err != nil {
		return err
	}
	if v != nil {
		return nil
	}
	existing, err := c.Lookup(c.id.Name)
	if err != nil {
		return err
	}
	if existing.Fingerprint() != c.id.Fingerprint() {
		return ErrNameTaken
	}
	_, err = c.t.Do("SET", key, string(b), "EX", strconv.Itoa(bundleTTL))
	return err
}

// Lookup fetches and verifies the bundle published under name. Verify the
// fingerprint with its owner before trusting it.
func (c *Client) Lookup(name string) (Bundle, error) {
	v, err := c.t.Do("GET", "id:"+name)
	if err != nil {
		return Bundle{}, err
	}
	raw, ok := v.([]byte)
	if !ok {
		return Bundle{}, fmt.Errorf("messenger: no identity named %q", name)
	}
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil || b.Name != name {
		return Bundle{}, ErrBadBundle
	}
	if err := b.Verify(); err != nil {
		return Bundle{}, err
	}
	return b, nil
}

// Send delivers body to peer, opening a session on first contact.
func (c *Client) Send(peer Bundle, body []byte) error {
	if err := peer.Verify(); err != nil {
		return err
	}
	fp := peer.Fingerprint()
	s := c.sessions[fp]
	if s == nil {
		var err error
		if s, err = initiate(c.id, peer); err != nil {
			return err
		}
		c.sessions[fp] = s
	}
	// Until the peer answers, everything goes to their hello mailbox with our
	// opening keys attached; after that, to the session's private mailbox.
	mailbox := s.Outbox
	if s.Hello != nil {
		mailbox = helloMailbox(peer)
	}

	now := time.Now()
	id := messageID(now)
	key := "msg:" + mailbox + ":" + id
	plaintext := make([]byte, 8+len(body))
	binary.BigEndian.PutUint64(plaintext, uint64(now.UnixMilli()))
	copy(plaintext[8:], body)

	env := envelope{V: version, Hello: s.Hello, N: s.Send.N}
	ct, err := s.seal(plaintext, aad(key, env.N))
	if err != nil {
		return err
	}
	env.CT = ct
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	set := []string{"SET", key, string(data)}
	if c.TTL > 0 {
		set = append(set, "EX", strconv.Itoa(c.TTL))
	}
	if _, err := c.t.Do(set...); err != nil {
		return err
	}
	if _, err := c.t.Do("ZADD", mailbox, strconv.FormatInt(now.UnixMilli(), 10), id); err != nil {
		return err
	}
	if c.TTL > 0 { // the index must outlive the messages it names; the node's default may not
		_, err = c.t.Do("EXPIRE", mailbox, strconv.Itoa(c.TTL), "GT")
	}
	return err
}

// Receive collects new messages from the hello mailbox and every session's
// inbox, oldest first, one batch per mailbox. Messages that cannot be
// opened are dropped and logged. What one call reads, the next call deletes
// from the server, so store State between calls and a crash loses nothing.
func (c *Client) Receive() ([]Message, error) {
	var out []Message
	items, err := c.fetch(helloMailbox(c.id.Bundle()), &c.helloSeen)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if m, ok := c.openHello(it); ok {
			out = append(out, m)
		}
	}
	for _, s := range c.sessions {
		items, err := c.fetch(s.Inbox, &s.Seen)
		if err != nil {
			return out, err
		}
		for _, it := range items {
			if m, ok := c.openWith(s, it); ok {
				out = append(out, m)
			}
		}
	}
	slices.SortFunc(out, func(a, b Message) int { return a.Time.Compare(b.Time) })
	return out, nil
}

type item struct {
	key  string
	data []byte
}

// fetch loads a mailbox's unread entries and deletes the ones read last
// time, which the caller has had a chance to persist since.
func (c *Client) fetch(mailbox string, seen *[]string) ([]item, error) {
	v, err := c.t.Do("ZRANGEBYSCORE", mailbox, "-inf", "+inf", "LIMIT", "0", strconv.Itoa(fetchBatch+len(*seen)))
	if err != nil {
		return nil, err
	}
	arr, _ := v.([]any)
	var read, fresh []string
	for _, e := range arr {
		id, _ := e.([]byte)
		if slices.Contains(*seen, string(id)) {
			read = append(read, string(id))
		} else {
			fresh = append(fresh, string(id))
		}
	}
	if len(read) > 0 {
		c.t.Do(append([]string{"ZREM", mailbox}, read...)...)
		c.t.Do(append([]string{"DEL"}, messageKeys(mailbox, read)...)...)
	}
	*seen = nil
	if len(fresh) == 0 {
		return nil, nil
	}
	keys := messageKeys(mailbox, fresh)
	v, err = c.t.Do(append([]string{"MGET"}, keys...)...)
	if err != nil {
		return nil, err
	}
	vals, ok := v.([]any)
	if !ok || len(vals) != len(keys) {
		return nil, fmt.Errorf("messenger: unexpected MGET reply %T", v)
	}
	*seen = fresh
	var items []item
	for i, val := range vals {
		if data, _ := val.([]byte); data != nil { // an entry whose message expired is just cleaned up next time
			items = append(items, item{key: keys[i], data: data})
		}
	}
	return items, nil
}

func messageKeys(mailbox string, ids []string) []string {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = "msg:" + mailbox + ":" + id
	}
	return keys
}

// openHello handles a message from our hello mailbox: it carries the
// sender's opening keys, so it can start a session or repeat one.
func (c *Client) openHello(it item) (Message, bool) {
	var env envelope
	if err := json.Unmarshal(it.data, &env); err != nil || env.V != version || env.Hello == nil {
		slog.Warn("messenger: malformed hello", "key", it.key)
		return Message{}, false
	}
	h := *env.Hello
	if err := h.Bundle.Verify(); err != nil {
		slog.Warn("messenger: hello with bad bundle", "key", it.key)
		return Message{}, false
	}
	fp := h.Bundle.Fingerprint()
	cur := c.sessions[fp]
	if cur != nil && bytes.Equal(cur.PeerEphemeral, h.Ephemeral) {
		return c.openWith(cur, it) // the session this hello already started
	}
	fresh, err := respond(c.id, h)
	if err != nil {
		slog.Warn("messenger: hello agreement failed", "key", it.key, "err", err)
		return Message{}, false
	}
	// Only a message that decrypts proves the sender holds the identity key,
	// so nothing is adopted before that; otherwise anyone with a public
	// bundle could replace a live session with a dead one.
	m, ok := c.openWith(fresh, it)
	if !ok {
		return Message{}, false
	}
	// Adopt the peer's session unless we opened one to them at the same time
	// and ours wins the tie: the lower fingerprint's initiation survives.
	if cur == nil || cur.Hello == nil || fp < c.id.Fingerprint() {
		c.sessions[fp] = fresh
	}
	return m, true
}

// openWith decrypts an envelope with s and marks the session answered.
func (c *Client) openWith(s *Session, it item) (Message, bool) {
	var env envelope
	if err := json.Unmarshal(it.data, &env); err != nil || env.V != version {
		slog.Warn("messenger: malformed message", "key", it.key)
		return Message{}, false
	}
	pt, err := s.open(env.N, env.CT, aad(it.key, env.N))
	if err != nil {
		slog.Warn("messenger: dropped message", "key", it.key, "err", err)
		return Message{}, false
	}
	if len(pt) < 8 {
		return Message{}, false
	}
	s.Hello = nil // they answered; the private mailbox is live
	return Message{
		From: s.Peer,
		Time: time.UnixMilli(int64(binary.BigEndian.Uint64(pt))),
		Body: pt[8:],
	}, true
}

// Sessions lists the peers we have sessions with.
func (c *Client) Sessions() []Bundle {
	out := make([]Bundle, 0, len(c.sessions))
	for _, s := range c.sessions {
		out = append(out, s.Peer)
	}
	return out
}

type state struct {
	Sessions  map[string]*Session `json:"sessions"`
	HelloSeen []string            `json:"hello_seen,omitempty"`
}

// State serializes sessions and read positions for storage. It contains
// chain keys; keep it as secret as the identity.
func (c *Client) State() ([]byte, error) {
	return json.Marshal(state{c.sessions, c.helloSeen})
}

func (c *Client) Restore(data []byte) error {
	var st state
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	if st.Sessions == nil {
		st.Sessions = map[string]*Session{}
	}
	for _, s := range st.Sessions {
		if s.Skipped == nil {
			s.Skipped = map[uint32][]byte{}
		}
	}
	c.sessions, c.helloSeen = st.Sessions, st.HelloSeen
	return nil
}

// helloMailbox is where first contact for a bundle's owner lands. Anyone
// with the bundle can compute it, so it reveals that someone wrote to the
// owner, but not who.
func helloMailbox(b Bundle) string {
	sum := sha256.Sum256(append([]byte("hello:"), b.Signing...))
	return "hello:" + hex.EncodeToString(sum[:16])
}

func messageID(now time.Time) string {
	var r [4]byte
	rand.Read(r[:])
	return strconv.FormatInt(now.UnixMilli(), 10) + "-" + hex.EncodeToString(r[:])
}

func aad(key string, n uint32) []byte {
	return []byte(key + "#" + strconv.FormatUint(uint64(n), 10))
}

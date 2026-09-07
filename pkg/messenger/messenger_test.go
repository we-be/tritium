package messenger

import (
	"bytes"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/internal/server"
	"github.com/we-be/tritium/pkg/tritium"
)

type world struct {
	t          *testing.T
	addr       string
	alice, bob *Client
	raw        *tritium.Client
}

// name scopes a user name to the test, since a real TRITIUM_RESP_ADDR is
// shared by every test and names are first come, first served.
func (w *world) name(base string) string { return base + "." + w.t.Name() }

func setup(t *testing.T) *world {
	t.Helper()
	srv, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })
	w := &world{t: t, addr: srv.Addr()}
	w.alice = w.user("alice")
	w.bob = w.user("bob")
	w.raw = w.conn()
	return w
}

func (w *world) conn() *tritium.Client {
	w.t.Helper()
	c, err := tritium.NewClient(&tritium.ClientOptions{Address: w.addr})
	if err != nil {
		w.t.Fatal(err)
	}
	w.t.Cleanup(func() { c.Close() })
	return c
}

func (w *world) user(name string) *Client {
	w.t.Helper()
	id, err := NewIdentity(w.name(name))
	if err != nil {
		w.t.Fatal(err)
	}
	c := New(w.conn(), id)
	if err := c.Publish(); err != nil {
		w.t.Fatal(err)
	}
	return c
}

func (w *world) lookup(c *Client, name string) Bundle {
	w.t.Helper()
	b, err := c.Lookup(w.name(name))
	if err != nil {
		w.t.Fatal(err)
	}
	return b
}

func (w *world) ids(mailbox string) []string {
	w.t.Helper()
	v, err := w.raw.Do("ZRANGEBYSCORE", mailbox, "-inf", "+inf")
	if err != nil {
		w.t.Fatal(err)
	}
	var ids []string
	for _, e := range v.([]any) {
		ids = append(ids, string(e.([]byte)))
	}
	return ids
}

func (w *world) send(from *Client, to Bundle, body string) {
	w.t.Helper()
	if err := from.Send(to, []byte(body)); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) receive(c *Client, want ...string) {
	w.t.Helper()
	msgs, err := c.Receive()
	if err != nil {
		w.t.Fatal(err)
	}
	var got []string
	for _, m := range msgs {
		got = append(got, string(m.Body))
	}
	if len(got) != len(want) {
		w.t.Fatalf("received %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			w.t.Fatalf("received %q, want %q", got, want)
		}
	}
}

func TestConversation(t *testing.T) {
	w := setup(t)
	bob := w.lookup(w.alice, "bob")
	if bob.Fingerprint() != w.bob.id.Fingerprint() {
		t.Fatal("looked-up bundle has the wrong fingerprint")
	}
	w.send(w.alice, bob, "hi bob")
	w.send(w.alice, bob, "you there?")
	msgs, err := w.bob.Receive()
	if err != nil || len(msgs) != 2 || msgs[0].From.Fingerprint() != w.alice.id.Fingerprint() {
		t.Fatalf("bob received %v, %v", msgs, err)
	}
	alice := w.lookup(w.bob, "alice")
	w.send(w.bob, alice, "yep")
	w.receive(w.alice, "yep")
	if w.alice.sessions[bob.Fingerprint()].Hello != nil {
		t.Fatal("alice's session still unanswered after bob replied")
	}
	w.send(w.alice, bob, "cool")
	w.receive(w.bob, "cool")
	w.receive(w.bob) // what the last call read, this one deletes

	// Mailboxes are consumed, and nothing on the server names either party.
	for _, mbx := range []string{helloMailbox(bob), w.alice.sessions[bob.Fingerprint()].Outbox} {
		if n, _ := w.raw.Do("ZCARD", mbx); n != int64(0) {
			t.Fatalf("mailbox %s still holds %v entries", mbx, n)
		}
	}

	bad := bob
	bad.Prekey = bytes.Clone(bob.Prekey)
	bad.Prekey[0] ^= 1
	if err := w.alice.Send(bad, []byte("x")); !errors.Is(err, ErrBadBundle) {
		t.Fatalf("tampered bundle accepted: %v", err)
	}
	mixed := bob // bob's signing key and prekey with someone else's agreement key
	mixed.Agreement = w.alice.id.Bundle().Agreement
	if err := mixed.Verify(); !errors.Is(err, ErrBadBundle) {
		t.Fatalf("bundle with a swapped agreement key verified: %v", err)
	}
	eve, _ := NewIdentity(w.name("bob"))
	if err := New(w.conn(), eve).Publish(); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("name squatting allowed: %v", err)
	}
}

// A missing message is skipped without losing the ones after it, a
// tampered message is dropped, and a replayed one is dropped.
func TestSkipTamperReplay(t *testing.T) {
	w := setup(t)
	bob := w.lookup(w.alice, "bob")
	for _, body := range []string{"one", "two", "three"} {
		w.send(w.alice, bob, body)
	}
	hello := helloMailbox(bob)
	w.raw.Do("DEL", "msg:"+hello+":"+w.ids(hello)[0])
	w.receive(w.bob, "two", "three")
	if n := len(w.bob.sessions[w.alice.id.Fingerprint()].Skipped); n != 1 {
		t.Fatalf("%d keys kept for the missing message, want 1", n)
	}
	w.receive(w.bob)

	w.send(w.alice, bob, "four")
	key := "msg:" + hello + ":" + w.ids(hello)[0]
	raw, _ := w.raw.Do("GET", key)
	var env envelope
	json.Unmarshal(raw.([]byte), &env)
	env.CT[0] ^= 1
	tampered, _ := json.Marshal(env)
	w.raw.Do("SET", key, string(tampered))
	w.receive(w.bob)
	w.receive(w.bob)

	w.send(w.alice, bob, "five")
	w.receive(w.bob, "five")
	w.bob.helloSeen = nil // forget we read it while it is still on the server, so only the ratchet can catch the replay
	w.receive(w.bob)
	w.receive(w.bob)
	if n := len(w.ids(hello)); n != 0 {
		t.Fatalf("hello mailbox still holds %d entries", n)
	}
}

// Padding hides message length within a bucket.
func TestPadding(t *testing.T) {
	w := setup(t)
	s, _ := initiate(w.alice.id, w.bob.id.Bundle())
	_, short, _ := s.seal([]byte("x"), nil)
	_, long, _ := s.seal(bytes.Repeat([]byte("x"), padBlock-9), nil)
	if len(short) != len(long) {
		t.Fatalf("ciphertext lengths %d and %d differ within one bucket", len(short), len(long))
	}
	if _, err := unpad(make([]byte, padBlock)); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("all-zero padding accepted: %v", err)
	}
}

// A hello forged from a public bundle (no identity key) must not disturb
// the real session with that peer.
func TestForgedHello(t *testing.T) {
	w := setup(t)
	bob := w.lookup(w.alice, "bob")
	w.send(w.alice, bob, "real")
	w.receive(w.bob, "real")
	before := w.bob.sessions[w.alice.id.Fingerprint()].Inbox

	forged, err := initiate(w.bob.id, bob) // a fresh ephemeral against bob's prekey...
	if err != nil {
		t.Fatal(err)
	}
	forged.Hello.Bundle = w.alice.id.Bundle() // ...claiming to be alice, without her keys
	mallory := New(w.conn(), w.bob.id)
	mallory.sessions[bob.Fingerprint()] = forged
	w.send(mallory, bob, "forged")

	w.receive(w.bob)
	if w.bob.sessions[w.alice.id.Fingerprint()].Inbox != before {
		t.Fatal("forged hello replaced the real session")
	}
	w.send(w.alice, bob, "still here")
	w.receive(w.bob, "still here")
}

func TestStateRoundTrip(t *testing.T) {
	w := setup(t)
	bob := w.lookup(w.alice, "bob")
	w.send(w.alice, bob, "before")
	w.receive(w.bob, "before")
	w.send(w.bob, w.lookup(w.bob, "alice"), "ack")
	w.receive(w.alice, "ack")

	idJSON, err := json.Marshal(w.bob.id)
	if err != nil {
		t.Fatal(err)
	}
	var id Identity
	if err := json.Unmarshal(idJSON, &id); err != nil || id.Fingerprint() != w.bob.id.Fingerprint() {
		t.Fatalf("identity did not survive JSON: %v", err)
	}
	st, err := w.bob.State()
	if err != nil {
		t.Fatal(err)
	}
	bob2 := New(w.conn(), &id)
	if err := bob2.Restore(st); err != nil {
		t.Fatal(err)
	}
	w.send(w.alice, bob, "after")
	w.receive(bob2, "after")

	// A message is deleted only by the Receive after the one that read it, so
	// state stored between them never loses a message.
	if st, err = bob2.State(); err != nil {
		t.Fatal(err)
	}
	w.send(w.alice, bob, "again")
	w.receive(bob2, "again")
	bob3 := New(w.conn(), &id)
	if err := bob3.Restore(st); err != nil {
		t.Fatal(err)
	}
	w.receive(bob3, "again")
}

// A rotated prekey keeps the fingerprint, hellos made against the old one
// still open sessions during its grace period, and not after.
func TestPrekeyRotation(t *testing.T) {
	w := setup(t)
	old := w.lookup(w.alice, "bob")
	w.bob.id.prekeys[0].created = time.Now().Add(-prekeyLifetime)
	if err := w.bob.Publish(); err != nil {
		t.Fatal(err)
	}
	cur := w.lookup(w.alice, "bob")
	if bytes.Equal(cur.Prekey, old.Prekey) || cur.Fingerprint() != old.Fingerprint() {
		t.Fatal("prekey did not rotate, or rotating it changed the fingerprint")
	}
	idJSON, _ := json.Marshal(w.bob.id)
	var restored Identity
	if err := json.Unmarshal(idJSON, &restored); err != nil || restored.prekeyFor(old.Prekey) == nil {
		t.Fatalf("retired prekey did not survive JSON: %v", err)
	}
	w.send(w.alice, old, "against the old prekey")
	w.receive(w.bob, "against the old prekey")

	w.bob.id.prekeys[0].created = time.Now().Add(-prekeyGrace)
	w.bob.id.rotate(time.Now())
	if w.bob.id.prekeyFor(old.Prekey) != nil {
		t.Fatal("prekey kept past its grace period")
	}
	carol, dave := w.user("carol"), w.user("dave")
	w.send(carol, old, "too late")
	w.send(dave, w.lookup(dave, "bob"), "in time")
	w.receive(w.bob, "in time")
}

// Both sides open a session to each other before either has read anything;
// they must still understand each other and converge on one session.
func TestSimultaneousHello(t *testing.T) {
	w := setup(t)
	bob, alice := w.lookup(w.alice, "bob"), w.lookup(w.bob, "alice")
	w.send(w.alice, bob, "a1")
	w.send(w.bob, alice, "b1")
	w.receive(w.alice, "b1")
	w.receive(w.bob, "a1")
	for i := range 3 {
		w.send(w.alice, bob, "a"+strconv.Itoa(i+2))
		w.receive(w.bob, "a"+strconv.Itoa(i+2))
		w.send(w.bob, alice, "b"+strconv.Itoa(i+2))
		w.receive(w.alice, "b"+strconv.Itoa(i+2))
	}
	a, b := w.alice.sessions[bob.Fingerprint()], w.bob.sessions[alice.Fingerprint()]
	if a.Outbox != b.Inbox || a.Inbox != b.Outbox || a.Hello != nil || b.Hello != nil {
		t.Fatalf("sessions did not converge: alice %s/%s bob %s/%s", a.Outbox, a.Inbox, b.Outbox, b.Inbox)
	}
}

// A throwaway identity asks a served name and gets exactly its reply; a pinned
// fingerprint that does not match is refused before anything is sent; the
// server forgets the throwaway session once it goes idle.
func TestAsk(t *testing.T) {
	w := setup(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { // bob serves until told to stop; the client is not goroutine-safe, so nothing else touches him meanwhile
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
			msgs, _ := w.bob.Receive()
			for _, m := range msgs {
				w.bob.Reply(m.From.Fingerprint(), append([]byte("re:"), m.Body...))
			}
		}
	})
	reply, err := Ask(w.conn(), w.name("bob"), w.bob.id.Fingerprint(), []byte("ping"), 5*time.Second, 60)
	if err != nil || string(reply) != "re:ping" {
		t.Fatalf("Ask = %q, %v", reply, err)
	}
	if _, err := Ask(w.conn(), w.name("bob"), w.alice.id.Fingerprint(), []byte("x"), time.Second, 60); !errors.Is(err, ErrFingerprint) {
		t.Fatalf("wrong pin accepted: %v", err)
	}
	close(stop)
	wg.Wait()
	if n := len(w.bob.Sessions()); n != 1 {
		t.Fatalf("bob holds %d sessions, want the one asker", n)
	}
	if w.bob.Prune(time.Hour) != 0 || w.bob.Prune(0) != 1 || len(w.bob.Sessions()) != 0 {
		t.Fatal("idle session pruning went wrong")
	}
}

// A captured session state stops reading the conversation as soon as the
// peer has answered again: every turn mixes in a fresh Diffie-Hellman.
func TestRatchetHeals(t *testing.T) {
	w := setup(t)
	bob, alice := w.lookup(w.alice, "bob"), w.lookup(w.bob, "alice")
	w.send(w.alice, bob, "1")
	w.receive(w.bob, "1")
	w.send(w.bob, alice, "2")
	w.receive(w.alice, "2")
	stolen, _ := w.bob.State() // bob's phone is imaged here
	w.send(w.alice, bob, "3")  // still the chain the thief knows...
	w.receive(w.bob, "3")
	w.send(w.bob, alice, "4") // ...until bob's next turn ratchets
	w.receive(w.alice, "4")
	w.send(w.alice, bob, "5")
	thief := New(w.conn(), w.bob.id)
	thief.Restore(stolen)
	w.receive(w.bob, "5")
	if msgs, _ := thief.Receive(); len(msgs) != 0 {
		t.Fatalf("a stale state read %q after the ratchet moved on", msgs[0].Body)
	}
	// bob holds alice's current key (she sent last); alice holds bob's key from his last send
	a, b := w.alice.sessions[bob.Fingerprint()], w.bob.sessions[alice.Fingerprint()]
	if bytes.Equal(a.PeerRatchet, bob.Prekey) || !bytes.Equal(b.PeerRatchet, mustPub(a.Ratchet)) {
		t.Fatal("ratchet keys did not advance past the prekey")
	}
}

// A device certified by its name's primary identity is found by fan-out; an
// uncertified bundle published at a device key is not.
func TestDeviceAuthorization(t *testing.T) {
	w := setup(t)
	phoneID, err := NewIdentity(w.alice.id.Name + "/phone")
	if err != nil {
		t.Fatal(err)
	}
	phone := New(w.conn(), phoneID)
	if err := phone.Publish(); err != nil {
		t.Fatal(err)
	}
	phoneBundle, err := w.alice.Lookup(phoneID.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.alice.AuthorizeDevice("phone", phoneBundle); err != nil {
		t.Fatal(err)
	}

	tabletID, _ := NewIdentity(w.alice.id.Name + "/tablet")
	tablet := New(w.conn(), tabletID)
	if err := tablet.Publish(); err != nil {
		t.Fatal(err)
	}

	all, err := w.bob.LookupAll(w.alice.id.Name)
	if err != nil || len(all) != 2 {
		t.Fatalf("LookupAll = %v, %v; want the primary and phone only", all, err)
	}

	if err := w.bob.SendAll(w.alice.id.Name, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	w.receive(w.alice, "hi")
	w.receive(phone, "hi")
	if msgs, _ := tablet.Receive(); len(msgs) != 0 {
		t.Fatal("an uncertified device received a fan-out send")
	}
}

// A group send reaches every member over their own session, tagged with the
// group's name; only the creator can change the roster.
func TestGroupSendAndAuth(t *testing.T) {
	w := setup(t)
	carol := w.user("carol")
	name := w.name("book-club")
	if _, err := w.alice.CreateGroup(name, []string{w.bob.id.Name, carol.id.Name}); err != nil {
		t.Fatal(err)
	}
	if err := w.alice.SendGroup(name, []byte("meeting friday")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Client{w.bob, carol} {
		msgs, err := c.Receive()
		if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "meeting friday" || msgs[0].Group != name {
			t.Fatalf("member received %v, %v; want one message tagged %q", msgs, err, name)
		}
	}
	if _, err := w.bob.AddMember(name, w.name("mallory")); !errors.Is(err, ErrNotCreator) {
		t.Fatalf("non-creator changed the roster: %v", err)
	}
}

func mustPub(priv []byte) []byte {
	k, err := ecdh.X25519().NewPrivateKey(priv)
	if err != nil {
		panic(err)
	}
	return k.PublicKey().Bytes()
}

// First contact reveals nothing about the sender to the node: the stored
// envelope holds an ephemeral key and ciphertext, never the bundle.
func TestSealedSender(t *testing.T) {
	w := setup(t)
	bob := w.lookup(w.alice, "bob")
	w.send(w.alice, bob, "hi")
	hello := helloMailbox(bob)
	raw, _ := w.raw.Do("GET", "msg:"+hello+":"+w.ids(hello)[0])
	stored := raw.([]byte)
	name := w.alice.id.Name
	if bytes.Contains(stored, []byte(name)) || bytes.Contains(stored, []byte(base64.StdEncoding.EncodeToString(w.alice.id.Bundle().Signing))) {
		t.Fatal("the stored hello names its sender")
	}
	var env envelope
	json.Unmarshal(stored, &env)
	if env.EK == nil || env.Hello == nil {
		t.Fatal("hello envelope lacks the ephemeral key or the sealed header")
	}
	// only bob's identity key opens it
	if _, err := unsealHello(w.alice.id, env.EK, env.Hello, hello); err == nil {
		t.Fatal("a hello opened with the wrong identity key")
	}
	w.receive(w.bob, "hi")
}

// The ratchet header is ciphertext to the node: two messages in one chain
// look alike, and only the session's header key opens them.
func TestHeadersAreOpaque(t *testing.T) {
	w := setup(t)
	bob := w.lookup(w.alice, "bob")
	w.send(w.alice, bob, "one")
	w.send(w.alice, bob, "two")
	hello := helloMailbox(bob)
	var envs []envelope
	for _, id := range w.ids(hello) {
		raw, _ := w.raw.Do("GET", "msg:"+hello+":"+id)
		var env envelope
		json.Unmarshal(raw.([]byte), &env)
		envs = append(envs, env)
	}
	if len(envs) != 2 || bytes.Equal(envs[0].EH, envs[1].EH) || len(envs[0].EH) != len(envs[1].EH) {
		t.Fatalf("headers %d/%d bytes, equal=%v", len(envs[0].EH), len(envs[1].EH), bytes.Equal(envs[0].EH, envs[1].EH))
	}
	s := w.alice.sessions[bob.Fingerprint()]
	h0, ok0 := hdecrypt(s.SendHeader, envs[0].EH)
	h1, ok1 := hdecrypt(s.SendHeader, envs[1].EH)
	if !ok0 || !ok1 || h0.N != 0 || h1.N != 1 {
		t.Fatalf("headers under the sending header key: %+v %v, %+v %v", h0, ok0, h1, ok1)
	}
	if _, ok := hdecrypt(s.NextSend, envs[0].EH); ok {
		t.Fatal("a header opened under the wrong header key")
	}
	w.receive(w.bob, "one", "two")
}

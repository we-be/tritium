package messenger

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

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
	ids, _ := w.raw.Do("ZRANGEBYSCORE", hello, "-inf", "+inf")
	first := string(ids.([]any)[0].([]byte))
	w.raw.Do("DEL", "msg:"+hello+":"+first)
	w.receive(w.bob, "two", "three")
	if _, ok := w.bob.sessions[w.alice.id.Fingerprint()].Skipped[0]; !ok {
		t.Fatal("key for the missing message was not kept")
	}

	w.send(w.alice, bob, "four")
	ids, _ = w.raw.Do("ZRANGEBYSCORE", hello, "-inf", "+inf")
	key := "msg:" + hello + ":" + string(ids.([]any)[0].([]byte))
	raw, _ := w.raw.Do("GET", key)
	var env envelope
	json.Unmarshal(raw.([]byte), &env)
	env.CT[0] ^= 1
	tampered, _ := json.Marshal(env)
	w.raw.Do("SET", key, string(tampered))
	w.receive(w.bob)

	w.send(w.alice, bob, "five")
	ids, _ = w.raw.Do("ZRANGEBYSCORE", hello, "-inf", "+inf", "WITHSCORES")
	id, score := string(ids.([]any)[0].([]byte)), string(ids.([]any)[1].([]byte))
	raw, _ = w.raw.Do("GET", "msg:"+hello+":"+id)
	w.receive(w.bob, "five")
	w.raw.Do("SET", "msg:"+hello+":"+id, string(raw.([]byte)))
	w.raw.Do("ZADD", hello, score, id)
	w.bob.hello = cursor{} // forget we read it, so only the ratchet can catch the replay
	w.receive(w.bob)
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

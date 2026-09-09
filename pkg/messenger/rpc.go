package messenger

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/we-be/tritium/pkg/tritium"
)

const askPoll = 200 * time.Millisecond

// What Ask ends with when no reply can be taken.
var (
	ErrFingerprint = errors.New("messenger: the peer's fingerprint is not the pinned one")
	ErrTimeout     = errors.New("messenger: no reply before the deadline")
)

// Ask sends body to the identity published under name and returns its first
// reply, or ErrTimeout. It speaks from a throwaway identity that exists only
// for this call, so processes never share ratchet state, and every call pays
// one key agreement. With pinned set, a bundle whose fingerprint differs is
// refused before anything is sent: names on a node are first come, so the
// fingerprint is what a caller trusts, never the name. Anything a caller must
// keep from strangers still has to travel inside body, where only the pinned
// peer can read it.
func Ask(t *tritium.Client, name, pinned string, body []byte, timeout time.Duration, ttl int) ([]byte, error) {
	var r [4]byte
	rand.Read(r[:])
	id, err := NewIdentity("ask-" + hex.EncodeToString(r[:]))
	if err != nil {
		return nil, err
	}
	c := New(t, id)
	c.TTL = ttl
	peer, err := c.Lookup(name)
	if err != nil {
		return nil, err
	}
	if pinned != "" && peer.Fingerprint() != pinned {
		return nil, ErrFingerprint
	}
	if err := c.Send(peer, body); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		msgs, err := c.Receive()
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			if m.From.Fingerprint() == peer.Fingerprint() {
				c.Receive() // the read after the read: deletes the reply from the server
				return m.Body, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, ErrTimeout
		}
		time.Sleep(askPoll)
	}
}

package server

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/replica"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

// A detach and a repair each leave a readable entry on a different, live
// node's local store — proof the event replicated like any key, not just
// that it was logged locally.
func TestEventsReplicateAcrossFleet(t *testing.T) {
	t.Run("detach", func(t *testing.T) {
		hurry(t)
		seed := startNode(t, config.Config{})
		peer := startNode(t, config.Config{JoinAddr: seed.Addr()})
		third := startNode(t, config.Config{JoinAddr: seed.Addr()})
		seedID, peerAddr := seed.cluster.local.ID, peer.Addr()
		waitFor(t, "all three attached", func() bool {
			return len(seed.store.Replicas()) == 2 && len(third.store.Replicas()) == 2
		})

		peer.Stop() // ages out on both seed's and third's own clocks, independently
		tc := dial(t, third)
		waitFor(t, "third to read seed's detach of peer", func() bool {
			return hasEvent(t, tc, seedID, "detach", peerAddr)
		})
	})

	t.Run("repair", func(t *testing.T) {
		hurry(t)
		realA, realB := reserve(t), reserve(t)
		linkA, linkB := newLink(t, realA), newLink(t, realB)
		a, err := New(config.Config{StoreAddr: resptest.Addr(t), ListenAddr: realA, AdvertiseAddr: linkA.Addr(), PoolSize: 2, PeerPassword: testPeerPW})
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Start(realA); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { a.Stop() })
		b, err := New(config.Config{StoreAddr: resptest.Addr(t), ListenAddr: realB, AdvertiseAddr: linkB.Addr(), PoolSize: 2, JoinAddr: linkA.Addr(), PeerPassword: testPeerPW})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Start(realB); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Stop() })
		if err := b.Join(linkA.Addr()); err != nil {
			t.Fatal(err)
		}
		aID, bAddr := a.cluster.local.ID, linkB.Addr()
		ca, cb := dial(t, a), dial(t, b)
		waitFor(t, "both attached", func() bool { return len(a.store.Replicas()) == 1 && len(b.store.Replicas()) == 1 })

		// a cut briefer than downAfter holds b rather than detaching it;
		// healing it lets the next health tick repair it — and, once healed,
		// the repair event written on a reaches b like any other key.
		linkA.Cut()
		linkB.Cut()
		ca.want("OK", "SET", "ev:hold", "x", "EX", "60")
		waitFor(t, "a to hold b", func() bool { return slices.Contains(a.store.Held(), bAddr) })
		linkA.Heal()
		linkB.Heal()
		waitFor(t, "b to read a's repair event", func() bool { return hasEvent(t, cb, aID, "repair", bAddr) })
	})
}

// hasEvent reports whether node's log, read through c, has an entry of kind
// naming peer.
func hasEvent(t *testing.T, c raw, node, kind, peer string) bool {
	t.Helper()
	v, err := c.do("ZRANGEBYSCORE", storage.EventsKeyPrefix+node, "-inf", "+inf")
	if err != nil {
		return false
	}
	members, _ := v.([]any)
	for _, m := range members {
		b, ok := m.([]byte)
		if !ok {
			continue
		}
		var ev storage.Event
		if json.Unmarshal(b, &ev) == nil && ev.Event == kind && ev.Peer == peer {
			return true
		}
	}
	return false
}

// A flapping peer cannot grow a node's log past eventsCap, and an entry
// older than the retention window is trimmed on the next write.
func TestEventsCapAndTrim(t *testing.T) {
	savedCap := eventsCap
	eventsCap = 10
	t.Cleanup(func() { eventsCap = savedCap })

	store, err := replica.NewStore(resptest.Addr(t), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	el := newEventLog("node-test", store)
	t.Cleanup(el.stop)
	key := storage.EventsKeyPrefix + "node-test"

	for i := range eventsCap + 5 {
		el.emit("attach", fmt.Sprintf("peer-%d", i), 0, 0)
	}
	waitFor(t, "the count to cap at eventsCap", func() bool {
		n, _ := store.Query("ZCARD", key)
		return n == int64(eventsCap)
	})

	stale := resp.NewCommand("ZADD", key, strconv.FormatInt(time.Now().Add(-25*time.Hour).UnixMilli(), 10), `{"event":"stale"}`)
	if _, err := store.Mutate(stale); err != nil {
		t.Fatal(err)
	}
	el.emit("attach", "trigger", 0, 0) // any write re-runs the age trim
	waitFor(t, "the stale entry to trim", func() bool {
		v, _ := store.Query("ZRANGEBYSCORE", key, "-inf", "+inf")
		members, _ := v.([]any)
		for _, m := range members {
			if b, ok := m.([]byte); ok && strings.Contains(string(b), "stale") {
				return false
			}
		}
		return true
	})
}

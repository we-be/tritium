package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/we-be/tritium/internal/config"
)

// Two nodes race SET NX on the same keys: exactly one wins each key, both
// nodes hold the winner, and a DEL spanning owners counts every key.
func TestOwnedNXIsExclusive(t *testing.T) {
	seed := startNode(t, config.Config{})
	peer := startNode(t, config.Config{JoinAddr: seed.Addr()})
	waitFor(t, "both attached", func() bool { return len(seed.store.Replicas()) == 1 && len(peer.store.Replicas()) == 1 })
	const n = 50
	wins := make([]atomic.Int32, n)
	var wg sync.WaitGroup
	for _, s := range []*Server{seed, peer} {
		c := dial(t, s)
		wg.Go(func() {
			for i := range n {
				v, err := c.do("SET", fmt.Sprintf("race:%d", i), s.Addr(), "NX")
				if err != nil {
					t.Error(err)
					return
				}
				if v == "OK" {
					wins[i].Add(1)
				}
			}
		})
	}
	wg.Wait()
	a, b := dial(t, seed), dial(t, peer)
	remote := 0
	for i := range n {
		key := fmt.Sprintf("race:%d", i)
		if w := wins[i].Load(); w != 1 {
			t.Fatalf("%s: %d winners", key, w)
		}
		va, _ := a.do("GET", key)
		vb, _ := b.do("GET", key)
		if va == nil || vb == nil || string(va.([]byte)) != string(vb.([]byte)) {
			t.Fatalf("%s: seed holds %s, peer holds %s", key, va, vb)
		}
		if seed.ownerOf(key) != "" {
			remote++
		}
	}
	if remote == 0 || remote == n {
		t.Fatalf("%d of %d keys owned by the peer: the hash should split them", remote, n)
	}
	if v, _ := a.do("DEL", "race:0", "race:1", "race:2", "race:3"); v != int64(4) {
		t.Fatalf("DEL across owners counted %v", v)
	}
}

// The owner cannot be reached: the write is done here instead — the forward
// fell back, or the held peer was already passed over — and the client
// never notices.
func TestForwardFallsBack(t *testing.T) {
	seed := startNode(t, config.Config{})
	peer := startNode(t, config.Config{JoinAddr: seed.Addr()})
	waitFor(t, "attached", func() bool { return len(seed.store.Replicas()) == 1 })
	var key string
	for i := 0; key == ""; i++ {
		if k := fmt.Sprintf("far:%d", i); seed.ownerOf(k) != "" {
			key = k
		}
	}
	peer.Stop()
	c := dial(t, seed)
	c.want("OK", "SET", key, "v", "EX", "60")
	c.want("v", "GET", key)
	if seed.fallbacks.Load() == 0 && seed.ownerOf(key) != "" {
		t.Fatal("the owner was unreachable, yet the write neither fell back nor was the owner passed over")
	}
}

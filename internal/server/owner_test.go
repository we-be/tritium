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
// Ownership follows weight: a node of weight 2 owns about twice the keys of
// one of weight 1, and one of weight 0 owns none.
func TestOwnershipFollowsWeight(t *testing.T) {
	weights := map[string]int{"a:1": 2, "b:1": 1, "c:1": 0}
	won := map[string]int{}
	for i := range 3000 {
		won[owner(fmt.Sprintf("k%d", i), weights)]++
	}
	if won["c:1"] != 0 || won["a:1"] < 1700 || won["a:1"] > 2300 {
		t.Fatalf("ownership split %v, want about 2000:1000:0", won)
	}
}

// A node of weight 0 orders nothing: every write it takes is forwarded to a
// peer with a weight, and it is not leader even though it seeded the
// cluster — the cloud hub carries replicas and decides nothing.
func TestWeightZeroNeverOwns(t *testing.T) {
	zero := 0
	hub := startNode(t, config.Config{Electronegativity: &zero})
	home := startNode(t, config.Config{JoinAddr: hub.Addr()})
	waitFor(t, "the nodes to attach", func() bool { return len(hub.store.Replicas()) == 1 && len(home.store.Replicas()) == 1 })
	if hub.Nodes()[hub.cluster.local.ID].IsLeader {
		t.Fatal("a node of weight 0 is marked leader")
	}
	c := dial(t, hub)
	for i := range 20 {
		c.want("OK", "SET", fmt.Sprintf("w:%d", i), "v", "EX", "60")
	}
	if f, fb := hub.forwarded.Load(), hub.fallbacks.Load(); f != 20 || fb != 0 {
		t.Fatalf("the hub forwarded %d writes and wrote %d itself, want 20 and 0", f, fb)
	}
}

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

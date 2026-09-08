package server

import (
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

// TestChaos kills and restarts nodes of a three-node lab at random while
// writing through whichever nodes are up, then lets the fleet settle and
// checks every node holds every key with its last value. Three seconds by
// default; TRITIUM_CHAOS_SECONDS lengthens it and TRITIUM_CHAOS_SEED replays
// a run. `make chaos` runs a long one. It runs under both replication modes.
func TestChaos(t *testing.T) {
	if os.Getenv("TRITIUM_RESP_ADDR") != "" {
		t.Skip("chaos needs a store per node: over one shared store every node reads the same last write, and the whole suite is writing to it")
	}
	t.Run("sync", func(t *testing.T) { chaos(t, false) })
	t.Run("async", func(t *testing.T) { chaos(t, true) })
}

func chaos(t *testing.T, async bool) {
	hurry(t)
	dur := 3 * time.Second
	if s := os.Getenv("TRITIUM_CHAOS_SECONDS"); s != "" {
		n, _ := strconv.Atoi(s)
		dur = time.Duration(n) * time.Second
	}
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("TRITIUM_CHAOS_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	t.Logf("chaos for %s, seed %d", dur, seed)

	// three nodes on reserved ports, every one listing the others as seeds
	addrs := make([]string, 3)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		ln.Close()
	}
	lab := make([]*Server, 3)
	start := func(i int) {
		var seeds []string
		for j, a := range addrs {
			if j != i {
				seeds = append(seeds, a)
			}
		}
		cfg := config.Config{StoreAddr: resptest.Addr(t), ListenAddr: addrs[i], PoolSize: 2, JoinAddr: seeds[0] + "," + seeds[1], Async: async, PeerPassword: testPeerPW}
		s, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start(addrs[i]); err != nil {
			t.Fatalf("start %s: %v", addrs[i], err)
		}
		lab[i] = s
	}
	for i := range lab {
		start(i)
	}
	t.Cleanup(func() {
		for _, s := range lab {
			if s != nil {
				s.Stop()
			}
		}
	})
	waitFor(t, "the lab to form", func() bool {
		for _, s := range lab {
			if len(s.Nodes()) != 3 {
				return false
			}
		}
		return true
	})

	written := map[string]string{}
	stops, restarts, writes := 0, 0, 0
	live := func() []int {
		var out []int
		for i, s := range lab {
			if s != nil {
				out = append(out, i)
			}
		}
		return out
	}
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		up := live()
		switch x := rng.IntN(10); {
		case x < 2 && len(up) > 1: // kill one, keeping a survivor
			i := up[rng.IntN(len(up))]
			lab[i].Stop()
			lab[i] = nil
			stops++
		case x < 4 && len(up) < 3: // bring one back, with an empty store
			for i := range lab {
				if lab[i] == nil {
					start(i)
					restarts++
					break
				}
			}
		default: // write through a random live node
			i := up[rng.IntN(len(up))]
			c := dial(t, lab[i])
			key, val := "chaos:"+strconv.Itoa(rng.IntN(40)), strconv.Itoa(writes)
			if v, err := c.do("SET", key, val, "EX", "300"); err == nil && v == "OK" {
				written[key] = val
				writes++
			}
			c.conn.Close()
		}
		time.Sleep(time.Duration(50+rng.IntN(100)) * time.Millisecond)
	}
	for i := range lab {
		if lab[i] == nil {
			start(i)
			restarts++
		}
	}
	t.Logf("%d writes, %d stops, %d restarts", writes, stops, restarts)

	settled := time.Now()
	waitFor(t, "every node to see the others healthy", func() bool {
		for _, s := range lab {
			view := s.Nodes()
			if len(view) != 3 {
				return false
			}
			for _, n := range view {
				if n.State != storage.NodeStateHealthy {
					return false
				}
			}
		}
		return true
	})
	// every node holds every key with its last value, within a generous window
	converged := time.Now().Add(10 * time.Second)
	for {
		missing := ""
		for i, s := range lab {
			c := dial(t, s)
			for key, want := range written {
				v, _ := c.do("GET", key)
				b, _ := v.([]byte)
				if string(b) != want {
					missing = fmt.Sprintf("node %d has %s=%q, want %q", i, key, b, want)
					break
				}
			}
			c.conn.Close()
			if missing != "" {
				break
			}
		}
		if missing == "" {
			break
		}
		if time.Now().After(converged) {
			t.Fatalf("did not converge (seed %d): %s", seed, missing)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("converged %s after the last restart (%d keys on 3 nodes)", time.Since(settled).Round(time.Millisecond), len(written))
}

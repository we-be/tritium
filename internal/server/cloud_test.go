package server

import (
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
)

// dead is an address nothing listens on: what a home node advertises to a
// cloud node that can never reach it.
func dead(t *testing.T) string {
	t.Helper()
	addr := reserve(t)
	c, err := net.Dial("tcp", addr)
	if err == nil {
		c.Close()
		t.Skip("the reserved port was taken by something else")
	}
	return addr
}

// linkHome starts a node the cloud node cannot dial, linked to it, and
// returns it with the address it advertises.
func linkHome(t *testing.T, cloud *Server, password string) (*Server, string) {
	t.Helper()
	cfg := config.Config{StoreAddr: resptest.Addr(t), ListenAddr: "127.0.0.1:0", Password: password,
		AdvertiseAddr: dead(t), LinkAddr: cloud.Addr(), PoolSize: 2, PeerPassword: testPeerPW}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(cfg.ListenAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	return s, cfg.AdvertiseAddr
}

// A parked connection its peer closed leaves the park at once, so no attach
// or fan-out after a home node goes away is spent finding that out.
func TestDeadLinksAreDropped(t *testing.T) {
	hurry(t)
	cloud := startNode(t, config.Config{})
	home, addr := linkHome(t, cloud, "")
	waitFor(t, "the home node to link", func() bool { return cloud.links.parked(addr) > 0 })
	home.Stop()
	waitFor(t, "the dead links to leave the park", func() bool { return cloud.links.parked(addr) == 0 })
}

// A parked connection is a peer session on the home node's side from the
// moment it is parked: the cloud node authenticates on it as it is handed
// over, so the home's auth deadline never closes a spare link the cloud has
// not taken yet.
func TestParkedLinksAreAuthenticated(t *testing.T) {
	cloud := startNode(t, config.Config{})
	_, addr := linkHome(t, cloud, "home-pw")
	waitFor(t, "the home node to link", func() bool { return cloud.links.parked(addr) > 0 })
	c, err := cloud.links.take(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := resp.NewCommand("TRITIUM.NODES").Do(c, resp.NewReader(c)); err != nil {
		t.Fatalf("a parked connection is not a peer session on the home node: %v", err)
	}
}

// A cloud node the others can dial, and two home nodes it cannot: they open
// the connections, and the cloud node's writes come back down them. Every
// node ends up holding what any of them wrote.
func TestCloudPeering(t *testing.T) {
	hurry(t)
	cloud := startNode(t, config.Config{})
	homes := []*Server{}
	for range 2 {
		h, _ := linkHome(t, cloud, "")
		homes = append(homes, h)
	}
	waitFor(t, "the cloud node to reach both homes", func() bool { return len(cloud.store.Replicas()) == 2 })
	// Both ends of a link feed the other from a queue — a write must not wait
	// out the internet — while the two homes, on one network, wait on each other.
	if q := cloud.store.Queued(); len(q) != 2 {
		t.Fatalf("the cloud node queues for %v, want both homes", q)
	}
	for _, h := range homes {
		if q := h.store.Queued(); !slices.Equal(q, []string{cloud.Addr()}) {
			t.Fatalf("a home node queues for %v, want the cloud node alone", q)
		}
	}
	// Nothing dials the cloud node but the homes, so a home's view of it is
	// only as fresh as the home's own gossip: twenty rounds must pass
	// without either home writing it off.
	time.Sleep(20 * gossipInterval)
	for _, h := range homes {
		if hasEvent(t, dial(t, h), h.cluster.local.ID, "detach", cloud.Addr()) {
			t.Fatal("a home node detached the cloud node while it was up")
		}
	}

	cc := dial(t, cloud)
	cc.want("OK", "SET", "cloud:k", "1", "EX", "60")
	for i, h := range homes {
		hc := dial(t, h)
		waitFor(t, "the cloud write to reach a home node", func() bool {
			v, _ := hc.do("GET", "cloud:k")
			b, _ := v.([]byte)
			return string(b) == "1"
		})
		hc.want("OK", "SET", "home:k", string(rune('a'+i)), "EX", "60")
		waitFor(t, "a home write to reach the cloud node", func() bool {
			v, _ := cc.do("GET", "home:k")
			b, _ := v.([]byte)
			return string(b) == string(rune('a'+i))
		})
	}
}

// A user may read and write only under the prefixes its entry names, and is
// never a peer.
func TestUserPrefixRights(t *testing.T) {
	t.Setenv("TRITIUM_USER_gateway", "pw:rw:node:gateway,sig:gateway;r:board:,fleet")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Password = "boss"
	s := startNode(t, cfg)

	c := dial(t, s)
	c.want("OK", "AUTH", "gateway", "pw")
	c.want("gateway", "ACL", "WHOAMI")
	c.want("OK", "SET", "node:gateway", "up", "EX", "60")
	c.want("up", "GET", "node:gateway")
	c.want(int64(0), "ZCARD", "board:dm")
	for _, args := range [][]string{
		{"SET", "node:bazzite", "spoofed"}, // another node's presence
		{"GET", "msg:secret"},              // a prefix it was never granted
		{"DEL", "node:gateway", "fleet"},   // one key it may write, one it may only read
	} {
		if _, err := c.do(args...); err == nil || !strings.Contains(err.Error(), "NOPERM") {
			t.Fatalf("%v was allowed: %v", args, err)
		}
	}
	if _, err := c.do("TRITIUM.GOSSIP", "{}"); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("a user reached a peer command: %v", err)
	}
}

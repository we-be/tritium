package server

import (
	"net"
	"strings"
	"testing"

	"github.com/we-be/tritium/internal/config"
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

// A cloud node the others can dial, and two home nodes it cannot: they open
// the connections, and the cloud node's writes come back down them. Every
// node ends up holding what any of them wrote.
func TestCloudPeering(t *testing.T) {
	hurry(t)
	cloud := startNode(t, config.Config{})
	homes := []*Server{}
	for range 2 {
		cfg := config.Config{StoreAddr: resptest.Addr(t), ListenAddr: "127.0.0.1:0",
			AdvertiseAddr: dead(t), LinkAddr: cloud.Addr(), PoolSize: 2}
		s, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start(cfg.ListenAddr); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Stop() })
		homes = append(homes, s)
	}
	waitFor(t, "the cloud node to reach both homes", func() bool { return len(cloud.store.Replicas()) == 2 })

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

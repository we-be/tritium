package server

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
)

// Five refused AUTHs close the connection, and an unknown user is refused
// the same way as a wrong password.
func TestAuthFailuresCloseTheConnection(t *testing.T) {
	s := startNode(t, config.Config{Password: "right"})
	c := dial(t, s)
	c.wantErr("WRONGPASS", "AUTH", "nobody", "x")
	for range maxAuthFailures - 2 {
		c.wantErr("WRONGPASS", "AUTH", "wrong")
	}
	if _, err := c.do("AUTH", "wrong"); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("the last refusal: %v", err)
	}
	if _, err := c.do("PING"); err == nil {
		t.Fatal("the connection stayed open after repeated AUTH failures")
	}
}

// A connection that never authenticates is closed after authTimeout.
func TestUnauthenticatedConnectionsTimeOut(t *testing.T) {
	saved := authTimeout
	authTimeout = 200 * time.Millisecond
	t.Cleanup(func() { authTimeout = saved })
	s := startNode(t, config.Config{Password: "right"})
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := resp.NewReader(conn).ReadValue(); err == nil {
		t.Fatal("expected the node to close an idle unauthenticated connection")
	}
}

// Past MAX_CLIENTS a connection is turned away with an error.
func TestMaxClients(t *testing.T) {
	s := startNode(t, config.Config{MaxClients: 1})
	first := dial(t, s)
	first.want("PONG", "PING")
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	v, err := resp.NewReader(conn).ReadValue()
	if err == nil || !strings.Contains(err.Error(), "max number of clients") {
		t.Fatalf("second client got %v, %v", v, err)
	}
}

// A replicated write stamped more than an hour ahead is refused, so a peer
// cannot freeze a key or drag this node's clock.
func TestStampTooFarAheadIsRefused(t *testing.T) {
	s := startNode(t, config.Config{})
	c := dial(t, s)
	c.want("OK", "SET", "far:k", "now", "EX", "60")
	ahead := (uint64(time.Now().UnixMilli()-stampEpoch) + 2*maxStampAhead) << 24
	c.wantErr("ERR stamp too far ahead", "TRITIUM.REPLICATE", "STAMPED", strconvU(ahead), "SETEX", "far:k", "60", "frozen")
	c.want("now", "GET", "far:k")
	c.want("OK", "SET", "far:k", "later", "EX", "60") // the clock did not move: a fresh write still lands
	c.want("later", "GET", "far:k")
}

// A user with prefix rights does not get the cluster view.
func TestUsersHaveNoClusterView(t *testing.T) {
	t.Setenv("TRITIUM_USER_gateway", "pw:rw:node:gateway")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Password = "boss"
	s := startNode(t, cfg)
	c := dial(t, s)
	c.want("OK", "AUTH", "gateway", "pw")
	c.wantErr("NOPERM", "TRITIUM.NODES")
}

func strconvU(n uint64) string { return strconv.FormatUint(n, 10) }

// A node with no password refuses to listen anywhere but loopback unless told to.
func TestNoPasswordOffLoopbackIsRefused(t *testing.T) {
	s, err := New(config.Config{PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start("0.0.0.0:0"); err == nil {
		t.Fatal("a passwordless node bound to every interface started")
	}
	s.Stop()
	s, err = New(config.Config{PoolSize: 1, AllowNoAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start("0.0.0.0:0"); err != nil {
		t.Fatal(err)
	}
	s.Stop()
}

// A user sees the node's health in INFO, not its address or its store.
func TestUsersInfoIsTrimmed(t *testing.T) {
	t.Setenv("TRITIUM_USER_gateway", "pw:rw:node:gateway")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Password = "boss"
	s := startNode(t, cfg)
	c := dial(t, s)
	c.want("OK", "AUTH", "gateway", "pw")
	info, err := c.do("INFO")
	if err != nil {
		t.Fatal(err)
	}
	if body := string(info.([]byte)); strings.Contains(body, "node_addr") || strings.Contains(body, "store_") || !strings.Contains(body, "server_name") {
		t.Fatalf("a user's INFO: %s", body)
	}
}

// An address that keeps guessing across connections is shut out.
func TestGuessingAddressIsShutOut(t *testing.T) {
	saved := guessLimit
	guessLimit = 3
	t.Cleanup(func() { guessLimit = saved })
	s := startNode(t, config.Config{Password: "right"})
	for range 3 {
		c := dial(t, s)
		c.wantErr("WRONGPASS", "AUTH", "wrong")
	}
	c := dial(t, s)
	c.wantErr("ERR too many failed attempts", "AUTH", "right") // even the right one, until the lockout ends
}

// A user's keys live at most the default TTL, so it cannot pin its data past everyone else's.
func TestUsersTTLIsCapped(t *testing.T) {
	t.Setenv("TRITIUM_USER_gateway", "pw:rw:node:gateway")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Password = "boss"
	s := startNode(t, cfg)
	c := dial(t, s)
	c.want("OK", "AUTH", "gateway", "pw")
	c.want("OK", "SET", "node:gateway", "up", "EX", "99999999")
	ttl, err := c.do("TTL", "node:gateway")
	if err != nil || ttl.(int64) > DefaultTTL {
		t.Fatalf("a user's key lives %v s", ttl)
	}
}

// With PEER_ALLOW set, a node gossip names but the list does not is ignored.
func TestPeerAllowIgnoresStrangers(t *testing.T) {
	s := startNode(t, config.Config{PeerAllow: []string{"127.0.0.1:1"}})
	c := dial(t, s)
	if _, err := c.do("TRITIUM.GOSSIP", `{"id":"node-evil","addr":"10.9.9.9:8080","state":"healthy","last_seen":"2099-01-01T00:00:00Z"}`); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Nodes()["node-evil"]; ok {
		t.Fatal("a node outside PEER_ALLOW joined the view")
	}
}

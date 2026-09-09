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

// A command fed in one byte at a time is dropped once commandTimeout runs
// out, but a connection sitting idle between commands, however long, is not.
func TestCommandTimeout(t *testing.T) {
	saved := commandTimeout
	commandTimeout = 200 * time.Millisecond
	t.Cleanup(func() { commandTimeout = saved })
	s := startNode(t, config.Config{})

	stalled, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	if _, err := stalled.Write([]byte("*2\r\n$4\r\nPING\r\n$1\r\n")); err != nil {
		t.Fatal(err)
	}
	stalled.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := resp.NewReader(stalled).ReadValue(); err == nil {
		t.Fatal("expected the node to close a connection stalled mid-command")
	}

	idle := dial(t, s)
	time.Sleep(2 * commandTimeout)
	idle.want("PONG", "PING")
}

// An older hub is sent plain writes: RELAY is asked only of v0.18.0 and up,
// and of a checkout build, whose version does not parse.
func TestRelayNeedsANewEnoughHub(t *testing.T) {
	for v, want := range map[string]bool{"v0.17.8": false, "v0.18.0": true, "v0.18.1-dev.abc": true, "v1.0.0": true, "dev": true, "": true} {
		if got := versionAtLeast(v, relayVersion); got != want {
			t.Fatalf("%q at least %s: %v", v, relayVersion, got)
		}
	}
}

// Every connection's keepalive is its own: idle and interval drawn from
// ranges wide enough that a fleet's connections never probe in step.
func TestKeepaliveIsSpread(t *testing.T) {
	lo, hi := keepaliveConfig(func(int) int { return 0 }), keepaliveConfig(func(n int) int { return n - 1 })
	if lo.Idle != 30*time.Second || hi.Idle < 59*time.Second || lo.Interval != 15*time.Second || hi.Interval < 29*time.Second || !lo.Enable {
		t.Fatalf("keepalive ranges: %+v .. %+v", lo, hi)
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
	c.want("OK", "AUTH", "peer", testPeerPW) // TRITIUM.REPLICATE is peer-only
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
	c.wantErr("NOPERM", "CLIENT", "LIST") // nor who else is connected
	c.wantErr("NOPERM", "SCAN", "0")      // nor what keys exist beyond its own
}

// A connection can name itself, and CLIENT LIST names every connection.
func TestClientList(t *testing.T) {
	s := startNode(t, config.Config{})
	worker, cli := dial(t, s), dial(t, s)
	worker.want("OK", "CLIENT", "SETNAME", "worker")
	worker.want("worker", "CLIENT", "GETNAME")
	worker.wantErr("ERR Client names", "CLIENT", "SETNAME", "two words")
	v, err := cli.do("CLIENT", "LIST")
	if err != nil {
		t.Fatal(err)
	}
	list := string(v.([]byte))
	if !strings.Contains(list, "name=worker") || !strings.Contains(list, "cmd=client") || strings.Count(list, "\n") < 2 {
		t.Fatalf("CLIENT LIST: %q", list)
	}
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

// A node that peers refuses to start on a peer password that is unset or
// equal to AUTH_PASSWORD, since either lets any client join the cluster;
// ALLOW_SHARED_PEER_PASSWORD keeps the old behaviour.
func TestPeeringNeedsADistinctPeerPassword(t *testing.T) {
	if _, err := New(config.Config{PoolSize: 1, JoinAddr: "127.0.0.1:1"}); err == nil {
		t.Fatal("a peering node with no PEER_PASSWORD started")
	}
	if _, err := New(config.Config{PoolSize: 1, JoinAddr: "127.0.0.1:1", Password: "pw", PeerPassword: "pw"}); err == nil {
		t.Fatal("a peering node with PEER_PASSWORD equal to AUTH_PASSWORD started")
	}
	s, err := New(config.Config{PoolSize: 1, JoinAddr: "127.0.0.1:1", AllowSharedPeerPassword: true})
	if err != nil {
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
	c.want("OK", "AUTH", "peer", testPeerPW) // TRITIUM.GOSSIP is peer-only
	if _, err := c.do("TRITIUM.GOSSIP", `{"id":"node-evil","addr":"10.9.9.9:8080","state":"healthy","last_seen":"2099-01-01T00:00:00Z"}`); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Nodes()["node-evil"]; ok {
		t.Fatal("a node outside PEER_ALLOW joined the view")
	}
}

// A stream that declares a bottomless or endlessly nested array before
// authenticating loses its connection, and the node keeps serving.
func TestParserAbuseKeepsServing(t *testing.T) {
	s := startNode(t, config.Config{Password: "right"})
	for _, in := range []string{"*9223372036854775807\r\n", strings.Repeat("*1\r\n", 40)} {
		conn, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatal(err)
		}
		conn.Write([]byte(in))
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := resp.NewReader(conn).ReadValue(); err == nil {
			t.Fatalf("%.8q was accepted", in)
		}
		conn.Close()
	}
	c := dial(t, s)
	c.want("OK", "AUTH", "right")
	c.want("PONG", "PING")
}

// What valkey-cli sends on connect is answered — COMMAND DOCS with an
// empty array, ECHO with its argument — so the stock CLI starts without
// complaint.
func TestCLIHandshake(t *testing.T) {
	s := startNode(t, config.Config{})
	c := dial(t, s)
	v, err := c.do("COMMAND", "DOCS")
	if arr, _ := v.([]any); err != nil || len(arr) != 0 {
		t.Fatalf("COMMAND DOCS: %v, %v", v, err)
	}
	c.want("hi", "ECHO", "hi")
}

// A peer held to rights replicates and forwards only the keys they name,
// relays nothing, and still sees the view it needs to join.
func TestScopedPeerWritesOnlyItsKeys(t *testing.T) {
	t.Setenv("TRITIUM_PEER_pub", "pw:rw:pub:")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Password = "boss"
	s := startNode(t, cfg)
	c := dial(t, s)
	c.want("OK", "AUTH", "pub", "pw")
	c.want("pub", "ACL", "WHOAMI")
	c.want("OK", "TRITIUM.REPLICATE", "SET", "pub:k", "v", "EX", "60")
	c.wantErr("NOPERM", "TRITIUM.REPLICATE", "SET", "fleet:k", "v", "EX", "60")
	c.wantErr("NOPERM", "TRITIUM.REPLICATE", "DEL", "pub:k", "fleet:k")
	c.wantErr("NOPERM", "TRITIUM.REPLICATE", "RELAY", "1", "127.0.0.1:1", "SET", "pub:k", "v", "EX", "60")
	c.wantErr("NOPERM", "TRITIUM.FORWARD", "SET", "fleet:k", "v")
	c.want("OK", "TRITIUM.FORWARD", "SET", "pub:k2", "v")
	if _, err := c.do("TRITIUM.NODES"); err != nil {
		t.Fatalf("a scoped peer cannot see the view: %v", err)
	}
	owner := dial(t, s)
	owner.want("OK", "AUTH", "boss")
	owner.want("v", "GET", "pub:k")
	owner.want("v", "GET", "pub:k2")
	if v, err := owner.do("GET", "fleet:k"); err != nil || v != nil {
		t.Fatalf("fleet:k was written through a scoped peer: %v, %v", v, err)
	}
}

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
	t.Setenv("USER_gateway", "pw:rw:node:gateway")
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

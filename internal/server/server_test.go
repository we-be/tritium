package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
	"github.com/we-be/tritium/pkg/tritium"
)

func startNode(t *testing.T, cfg config.Config) *Server {
	t.Helper()
	cfg.StoreAddr, cfg.ListenAddr, cfg.PoolSize = resptest.Addr(t), "127.0.0.1:0", 2
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(cfg.ListenAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	if cfg.JoinAddr != "" {
		if err := s.Join(cfg.JoinAddr); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// raw is a bare RESP connection for poking at the protocol directly.
type raw struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
}

func dial(t *testing.T, s *Server) raw {
	t.Helper()
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return raw{t, conn, resp.NewReader(conn)}
}

func (c raw) do(args ...string) (any, error) {
	return resp.NewCommand(args...).Do(c.conn, c.r)
}

func (c raw) want(want any, args ...string) {
	c.t.Helper()
	got, err := c.do(args...)
	if err != nil {
		c.t.Fatalf("%v: %v", args, err)
	}
	if gb, ok := got.([]byte); ok {
		got = string(gb)
	}
	if got != want {
		c.t.Fatalf("%v: got %#v, want %#v", args, got, want)
	}
}

func (c raw) wantErr(prefix string, args ...string) {
	c.t.Helper()
	_, err := c.do(args...)
	var se *resp.ServerError
	if !errors.As(err, &se) || !strings.HasPrefix(se.Msg, prefix) {
		c.t.Fatalf("%v: got %v, want error starting %q", args, err, prefix)
	}
}

func TestCommands(t *testing.T) {
	s := startNode(t, config.Config{})
	c := dial(t, s)

	c.want("PONG", "PING")
	c.want("hi", "PING", "hi")
	c.want("OK", "SET", "cmd:k", "v", "EX", "60")
	c.want("v", "GET", "cmd:k")
	if ttl, err := c.do("TTL", "cmd:k"); err != nil || ttl.(int64) <= 0 || ttl.(int64) > 60 {
		t.Fatalf("TTL: %v, %v", ttl, err)
	}
	c.want(int64(0), "EXPIRE", "cmd:k", "30", "GT")
	c.want(int64(1), "EXPIRE", "cmd:k", "90", "GT")
	if ttl, err := c.do("TTL", "cmd:k"); err != nil || ttl.(int64) <= 60 {
		t.Fatalf("TTL after EXPIRE GT: %v, %v", ttl, err)
	}
	c.wantErr("ERR invalid expire time", "EXPIRE", "cmd:k", "0")
	c.want("OK", "SET", "cmd:k2", "v2", "px", "1500")
	c.want("OK", "SETEX", "cmd:k3", "30", "v3")
	c.want(nil, "SET", "cmd:k", "other", "NX")
	c.want("OK", "SET", "cmd:nx", "first", "NX", "EX", "60")
	c.want(int64(3), "EXISTS", "cmd:k", "cmd:k2", "cmd:k3", "cmd:missing")
	if v, err := c.do("MGET", "cmd:k", "cmd:missing"); err != nil || !reflect.DeepEqual(v, []any{[]byte("v"), nil}) {
		t.Fatalf("MGET: %#v, %v", v, err)
	}
	c.want("first", "GETDEL", "cmd:nx")
	c.want(nil, "GETDEL", "cmd:nx")
	c.want(int64(3), "DEL", "cmd:k", "cmd:k2", "cmd:k3")
	c.want(nil, "GET", "cmd:k")

	c.want(int64(2), "ZADD", "cmd:z", "2", "b", "1", "a")
	c.want(int64(0), "ZADD", "cmd:z", "3", "b")
	c.wantErr("ERR", "ZADD", "cmd:z", "x", "a")
	if v, err := c.do("ZRANGEBYSCORE", "cmd:z", "(1", "+inf", "WITHSCORES", "LIMIT", "0", "10"); err != nil || !reflect.DeepEqual(v, []any{[]byte("b"), []byte("3")}) {
		t.Fatalf("ZRANGEBYSCORE: %#v, %v", v, err)
	}
	if ttl, err := c.do("TTL", "cmd:z"); err != nil || ttl.(int64) <= 0 {
		t.Fatalf("sorted set did not get a TTL: %v, %v", ttl, err)
	}
	c.want(int64(2), "ZCARD", "cmd:z")
	c.want(int64(1), "ZREM", "cmd:z", "a")
	c.want(int64(1), "ZREMRANGEBYSCORE", "cmd:z", "-inf", "+inf")
	c.want(int64(0), "ZCARD", "cmd:z")
	c.want("OK", "CLIENT", "SETNAME", "test")
	c.want("OK", "SELECT", "0")
	c.wantErr("ERR DB index", "SELECT", "1")
	c.wantErr("ERR unknown command", "FLUSHALL")
	c.wantErr("ERR wrong number of arguments for 'get'", "GET")
	c.wantErr("ERR syntax error", "SET", "cmd:k", "v", "XX")
	c.wantErr("ERR AUTH <password> called without", "AUTH", "x")
	if info, err := c.do("INFO", "tritium"); err != nil || !strings.Contains(string(info.([]byte)), "node_id:node-") {
		t.Fatalf("INFO tritium: %q, %v", info, err)
	}
	if st := s.Stats(); st.ActiveConnections != 1 || st.BytesTransferred != 16 { // v, v, v2, v3, first (nx), first (getdel)
		t.Fatalf("stats: %+v", st)
	}
}

func TestAuth(t *testing.T) {
	s := startNode(t, config.Config{Password: "s3cret", PeerPassword: "peer-s3cret"})
	c := dial(t, s)

	c.wantErr("NOAUTH", "GET", "auth:k")
	c.wantErr("WRONGPASS", "AUTH", "wrong")
	c.wantErr("WRONGPASS", "AUTH", "admin", "s3cret")
	c.wantErr("WRONGPASS", "AUTH", "peer", "s3cret")
	c.want("OK", "AUTH", "s3cret")
	c.want("PONG", "PING")

	// A client may read the view but not change membership; a peer may do both.
	self := s.cluster.localJSON()
	if _, err := c.do("TRITIUM.NODES"); err != nil {
		t.Fatalf("client TRITIUM.NODES: %v", err)
	}
	c.wantErr("NOPERM", "TRITIUM.GOSSIP", self)
	c.want("OK", "AUTH", "peer", "peer-s3cret")
	if _, err := c.do("TRITIUM.GOSSIP", self); err != nil {
		t.Fatalf("peer TRITIUM.GOSSIP: %v", err)
	}

	h := dial(t, s)
	h.wantErr("NOPROTO", "HELLO", "4")
	h.wantErr("NOAUTH", "HELLO", "2")
	if v, err := h.do("HELLO", "2", "AUTH", "default", "s3cret", "SETNAME", "x"); err != nil || len(v.([]any)) != 12 {
		t.Fatalf("HELLO with AUTH: %v, %v", v, err)
	}
	h.want("PONG", "PING")
}

// HELLO 3 switches the connection to RESP3: a map reply and "_" nulls.
func TestRESP3(t *testing.T) {
	s := startNode(t, config.Config{})
	c := dial(t, s)
	if _, err := resp.NewCommand("HELLO", "3").WriteTo(c.conn); err != nil {
		t.Fatal(err)
	}
	if _, err := resp.NewCommand("GET", "resp3:missing").WriteTo(c.conn); err != nil {
		t.Fatal(err)
	}
	// Both replies may arrive in one read or two.
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var got string
	for buf := make([]byte, 512); !strings.HasSuffix(got, "_\r\n"); {
		n, err := c.conn.Read(buf)
		if err != nil {
			t.Fatalf("after %q: %v", got, err)
		}
		got += string(buf[:n])
	}
	if !strings.HasPrefix(got, "%6\r\n$6\r\nserver\r\n$7\r\ntritium\r\n") {
		t.Fatalf("RESP3 replies: %q", got)
	}
}

// Two nodes with client and peer passwords: join, announce and gossip all
// run as the peer user, and a write through one lands in the other's store.
func TestJoinReplicates(t *testing.T) {
	seed := startNode(t, config.Config{Password: "pw", PeerPassword: "peer-pw"})
	peer := startNode(t, config.Config{Password: "pw", PeerPassword: "peer-pw", JoinAddr: seed.Addr()})
	if seed.cluster.local.StoreAddr != peer.cluster.local.StoreAddr { // a shared store (TRITIUM_RESP_ADDR) is never attached
		waitFor(t, "the seed to replicate to the peer's node, never its store", func() bool { // attach follows the announce it raced
			r := seed.store.Replicas()
			return len(r) == 1 && r[0] == peer.Addr()
		})
	}
	c0 := dial(t, seed)
	c0.want("OK", "AUTH", "pw")
	c0.wantErr("NOPERM", "TRITIUM.REPLICATE", "SETEX", "x", "1", "y") // clients cannot inject writes

	if n := len(seed.Nodes()); n != 2 {
		t.Fatalf("seed sees %d nodes, want 2", n)
	}
	if n := len(peer.Nodes()); n != 2 {
		t.Fatalf("peer sees %d nodes, want 2", n)
	}
	client, err := tritium.NewClient(&tritium.ClientOptions{Address: peer.Addr(), Password: "pw"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Set("join:k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	if v, err := seed.store.Get("join:k"); err != nil || string(v) != "v" {
		t.Fatalf("write did not reach the seed's store: %q, %v", v, err)
	}
	if _, err := client.Do("ZADD", "join:z", "1", "a"); err != nil {
		t.Fatal(err)
	}
	if n, err := seed.store.Query("ZCARD", "join:z"); err != nil || n != int64(1) {
		t.Fatalf("sorted-set write did not reach the seed's store: %v, %v", n, err)
	}
}

// Mutual TLS between nodes and from a client, with a plaintext client refused.
func TestTLS(t *testing.T) {
	certFile, keyFile, pool := selfSigned(t)
	cfg := config.Config{TLSCert: certFile, TLSKey: keyFile, TLSCA: certFile, TLSClientAuth: true}
	seed := startNode(t, cfg)
	cfg.JoinAddr = seed.Addr()
	peer := startNode(t, cfg)
	if n := len(peer.Nodes()); n != 2 {
		t.Fatalf("peer sees %d nodes over TLS, want 2", n)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	client, err := tritium.NewClient(&tritium.ClientOptions{
		Address: seed.Addr(),
		TLS:     &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(); err != nil {
		t.Fatal(err)
	}
	// TLS 1.3 clients only learn of a rejected certificate on their first read.
	noCert, err := tritium.NewClient(&tritium.ClientOptions{Address: seed.Addr(), TLS: &tls.Config{RootCAs: pool}})
	if err == nil {
		err = noCert.Ping()
	}
	if err == nil {
		t.Fatal("client without a certificate was accepted under TLS_CLIENT_AUTH")
	}
	plain := dial(t, seed)
	if _, err := plain.do("PING"); err == nil {
		t.Fatal("plaintext client was accepted on a TLS listener")
	}
}

// selfSigned writes a certificate that is its own CA, valid for 127.0.0.1,
// usable for both server and client auth.
func selfSigned(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tritium-test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return certFile, keyFile, pool
}

// A node whose seed is down keeps trying, and joins when the seed comes up.
func TestRejoinsSeed(t *testing.T) {
	defer func(d time.Duration) { rejoinInterval = d }(rejoinInterval)
	rejoinInterval = 100 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	seedAddr := ln.Addr().String()
	ln.Close() // reserved for the seed, which is not up yet

	late, err := New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 2, JoinAddr: seedAddr})
	if err != nil {
		t.Fatal(err)
	}
	if err := late.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { late.Stop() })
	time.Sleep(3 * rejoinInterval)
	if n := len(late.Nodes()); n != 1 {
		t.Fatalf("saw %d nodes before the seed existed", n)
	}

	seed, err := New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Start(seedAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { seed.Stop() })
	deadline := time.Now().Add(5 * time.Second)
	for len(late.Nodes()) != 2 || len(seed.Nodes()) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no rejoin: late sees %d, seed sees %d", len(late.Nodes()), len(seed.Nodes()))
		}
		time.Sleep(rejoinInterval)
	}
}

// hurry shrinks the cluster's clocks for one test.
func hurry(t *testing.T) {
	saved := []time.Duration{gossipInterval, healthInterval, degradedAfter, downAfter, evictAfter, rejoinInterval}
	t.Cleanup(func() {
		gossipInterval, healthInterval, degradedAfter, downAfter, evictAfter, rejoinInterval = saved[0], saved[1], saved[2], saved[3], saved[4], saved[5]
	})
	gossipInterval, healthInterval, degradedAfter, downAfter, evictAfter, rejoinInterval =
		100*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond, 300*time.Millisecond, 10*time.Second, 100*time.Millisecond
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A peer that was down comes back with an empty store and is brought up to
// date: what was written while it was away is copied over on attach.
func TestResyncAfterOutage(t *testing.T) {
	hurry(t)
	seed := startNode(t, config.Config{})
	peer := startNode(t, config.Config{JoinAddr: seed.Addr()})
	peerAddr, peerID := peer.Addr(), peer.cluster.local.ID
	c := dial(t, seed)
	c.want("OK", "SET", "sync:before", "v1", "EX", "60")
	c.want(int64(1), "ZADD", "sync:z", "1", "a")

	peer.Stop()
	waitFor(t, "the seed to see the peer down", func() bool {
		n, ok := seed.Nodes()[peerID]
		return ok && n.State == storage.NodeStateDown
	})
	c.want("OK", "SET", "sync:during", "v2", "EX", "60") // the peer misses this

	cfg := config.Config{StoreAddr: resptest.Addr(t), ListenAddr: peerAddr, PoolSize: 2, JoinAddr: seed.Addr()}
	back, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := back.Start(peerAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { back.Stop() })
	if err := back.Join(seed.Addr()); err != nil {
		t.Fatal(err)
	}
	bc := dial(t, back)
	waitFor(t, "the resync to land", func() bool { // keys arrive in scan order: wait for all of them
		n, _ := bc.do("EXISTS", "sync:before", "sync:during", "sync:z")
		return n == int64(3)
	})
	bc.want("v1", "GET", "sync:before")
	bc.want("v2", "GET", "sync:during")
	bc.want(int64(1), "ZCARD", "sync:z")
	if ttl, _ := bc.do("TTL", "sync:during"); ttl.(int64) <= 0 || ttl.(int64) > 60 {
		t.Fatalf("resynced key lost its TTL: %v", ttl)
	}
}

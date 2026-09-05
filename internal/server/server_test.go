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
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
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
	c.want("OK", "SET", "cmd:k2", "v2", "px", "1500")
	c.want("OK", "SETEX", "cmd:k3", "30", "v3")
	c.want(int64(3), "EXISTS", "cmd:k", "cmd:k2", "cmd:k3", "cmd:missing")
	c.want(int64(3), "DEL", "cmd:k", "cmd:k2", "cmd:k3")
	c.want(nil, "GET", "cmd:k")
	c.want("OK", "CLIENT", "SETNAME", "test")
	c.want("OK", "SELECT", "0")
	c.wantErr("ERR DB index", "SELECT", "1")
	c.wantErr("ERR unknown command", "FLUSHALL")
	c.wantErr("ERR wrong number of arguments for 'get'", "GET")
	c.wantErr("ERR syntax error", "SET", "cmd:k", "v", "NX")
	c.wantErr("ERR AUTH <password> called without", "AUTH", "x")
	if info, err := c.do("INFO", "tritium"); err != nil || !strings.Contains(string(info.([]byte)), "node_id:node-") {
		t.Fatalf("INFO tritium: %q, %v", info, err)
	}
	if st := s.Stats(); st.ActiveConnections != 1 || st.BytesTransferred != 6 { // v, v (read), v2, v3
		t.Fatalf("stats: %+v", st)
	}
}

func TestAuth(t *testing.T) {
	s := startNode(t, config.Config{Password: "s3cret"})
	c := dial(t, s)

	c.wantErr("NOAUTH", "GET", "auth:k")
	c.wantErr("WRONGPASS", "AUTH", "wrong")
	c.wantErr("WRONGPASS", "AUTH", "admin", "s3cret")
	c.want("OK", "AUTH", "s3cret")
	c.want("PONG", "PING")

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

// Two nodes with a shared password: join, announce and gossip all run over
// authenticated RESP, and a write through one lands in the other's store.
func TestJoinReplicates(t *testing.T) {
	seed := startNode(t, config.Config{Password: "pw"})
	peer := startNode(t, config.Config{Password: "pw", JoinAddr: seed.Addr()})

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

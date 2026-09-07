package tritium_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/internal/server"
	"github.com/we-be/tritium/pkg/tritium"
)

func TestClient(t *testing.T) {
	srv, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	c, err := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Set("client:k", []byte("v"), new(60)); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get("client:k"); err != nil || string(v) != "v" {
		t.Fatalf("get: %q, %v", v, err)
	}
	if _, err := c.Get("client:missing"); !errors.Is(err, tritium.ErrNotFound) {
		t.Fatalf("missing: got %v, want ErrNotFound", err)
	}
	if ok, err := c.Delete("client:k"); err != nil || !ok {
		t.Fatalf("delete: %v, %v", ok, err)
	}
	if nodes, err := c.Nodes(); err != nil || len(nodes) != 1 {
		t.Fatalf("nodes: %v, %v", nodes, err)
	}
}

// A page walk with MATCH visits every live key exactly once, regardless of
// how COUNT chops it up, and skips what MATCH excludes.
func TestScan(t *testing.T) {
	srv, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	c, err := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	want := map[string]bool{}
	for i := range 25 {
		k := fmt.Sprintf("scan:%02d", i)
		want[k] = true
		if err := c.Set(k, []byte("v"), new(60)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Set("other:key", []byte("v"), new(60)); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	var cursor uint64
	for {
		keys, next, err := c.Scan(cursor, "scan:*", 5)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			if seen[k] {
				t.Fatalf("key %q seen twice", k)
			}
			seen[k] = true
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("scan saw %v, want %v", seen, want)
	}
	if n, err := c.DBSize(); err != nil || n != 26 {
		t.Fatalf("DBSize: %v, %v", n, err)
	}
}

// TYPE on a key that doesn't exist is "none", not an error.
func TestType(t *testing.T) {
	srv, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	c, err := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Set("type:k", []byte("v"), new(60)); err != nil {
		t.Fatal(err)
	}
	if typ, err := c.Type("type:k"); err != nil || typ != "string" {
		t.Fatalf("Type: %q, %v", typ, err)
	}
	if typ, err := c.Type("type:missing"); err != nil || typ != "none" {
		t.Fatalf("Type of missing key: %q, %v", typ, err)
	}
}

// A keyed client round-trips values, the node only ever sees ciphertext, and
// the wrong key, a moved value, or an unencrypted value are all refused.
func TestEncryption(t *testing.T) {
	srv, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	key := tritium.KeyFromPassphrase("correct horse", "salt")
	sealed, err := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr(), Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Close()
	plain, err := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()

	secret := []byte("attack at dawn")
	if err := sealed.Set("enc:k", secret, nil); err != nil {
		t.Fatal(err)
	}
	if v, err := sealed.Get("enc:k"); err != nil || !bytes.Equal(v, secret) {
		t.Fatalf("round trip: %q, %v", v, err)
	}
	raw, err := plain.Get("enc:k")
	if err != nil || bytes.Contains(raw, secret) || !bytes.HasPrefix(raw, []byte("TE1")) {
		t.Fatalf("stored value is not sealed: %q, %v", raw, err)
	}

	wrong, _ := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr(), Key: tritium.KeyFromPassphrase("wrong", "salt")})
	defer wrong.Close()
	if _, err := wrong.Get("enc:k"); !errors.Is(err, tritium.ErrDecrypt) {
		t.Fatalf("wrong key: got %v, want ErrDecrypt", err)
	}
	if err := plain.Set("enc:moved", raw, nil); err != nil { // same ciphertext under another name
		t.Fatal(err)
	}
	if _, err := sealed.Get("enc:moved"); !errors.Is(err, tritium.ErrDecrypt) {
		t.Fatalf("moved value: got %v, want ErrDecrypt", err)
	}
	if err := plain.Set("enc:plain", []byte("clear"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sealed.Get("enc:plain"); !errors.Is(err, tritium.ErrNotEncrypted) {
		t.Fatalf("unencrypted value: got %v, want ErrNotEncrypted", err)
	}
	if _, err := tritium.ParseKey("too-short"); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestOptionsFromEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.env")
	os.WriteFile(path, []byte("LISTEN_ADDRESS=:9090\nAUTH_PASSWORD=pw\n"), 0o600)
	opts, err := tritium.OptionsFromEnv(path)
	if err != nil || opts.Address != "127.0.0.1:9090" || opts.Password != "pw" || opts.TLS != nil {
		t.Fatalf("OptionsFromEnv = %+v, %v", opts, err)
	}
}

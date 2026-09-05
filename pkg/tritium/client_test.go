package tritium_test

import (
	"errors"
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

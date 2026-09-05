package server

import (
	"net/rpc"
	"testing"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

func startNode(t *testing.T, join string) *Server {
	t.Helper()
	cfg := config.Config{StoreAddr: resptest.Addr(t), RPCAddr: "127.0.0.1:0", PoolSize: 2, JoinAddr: join}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(cfg.RPCAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	if join != "" {
		if err := s.Join(join); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestRPCSetGetDelete(t *testing.T) {
	s := startNode(t, "")
	c, err := rpc.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var set storage.SetReply
	if err := c.Call("Store.Set", &storage.SetArgs{Key: "server:k", Value: []byte("v"), TTL: new(60)}, &set); err != nil || set.Error != "" {
		t.Fatalf("set: %v %q", err, set.Error)
	}
	var get storage.GetReply
	if err := c.Call("Store.Get", &storage.GetArgs{Key: "server:k"}, &get); err != nil || string(get.Value) != "v" {
		t.Fatalf("get: %v %+v", err, get)
	}
	var missing storage.GetReply
	if err := c.Call("Store.Get", &storage.GetArgs{Key: "server:missing"}, &missing); err != nil || missing.Error != storage.ErrNotFound.Error() {
		t.Fatalf("missing: %v %+v", err, missing)
	}
	var del storage.DeleteReply
	if err := c.Call("Store.Delete", &storage.DeleteArgs{Key: "server:k"}, &del); err != nil || !del.Deleted {
		t.Fatalf("delete: %v %+v", err, del)
	}
	if st := s.Stats(); st.ActiveConnections != 1 || st.BytesTransferred != 2 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestJoinReplicates(t *testing.T) {
	seed := startNode(t, "")
	peer := startNode(t, seed.Addr())

	if n := len(seed.Nodes()); n != 2 {
		t.Fatalf("seed sees %d nodes, want 2", n)
	}
	if n := len(peer.Nodes()); n != 2 {
		t.Fatalf("peer sees %d nodes, want 2", n)
	}

	c, err := rpc.Dial("tcp", peer.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var set storage.SetReply
	if err := c.Call("Store.Set", &storage.SetArgs{Key: "server:rep", Value: []byte("v")}, &set); err != nil || set.Error != "" {
		t.Fatalf("set via peer: %v %q", err, set.Error)
	}
	if v, err := seed.store.Get("server:rep"); err != nil || string(v) != "v" {
		t.Fatalf("write did not reach the seed's store: %q, %v", v, err)
	}
}

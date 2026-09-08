package load

import (
	"context"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/internal/server"
	"github.com/we-be/tritium/pkg/tritium"
)

// A short run against a two-node cluster reports every command kind and a
// replication lag, with nothing missed.
func TestRun(t *testing.T) {
	seed := node(t, "")
	peer := node(t, seed.Addr())
	rep, err := Run(context.Background(), tritium.ClientOptions{Address: seed.Addr()}, &tritium.ClientOptions{Address: peer.Addr()},
		Options{Rate: 400, Duration: 700 * time.Millisecond, Conns: 2, Keys: 50, Size: 32, TTL: 30, LagEvery: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Ops < 100 || rep.Errors != 0 || rep.Set.N == 0 || rep.Get.N == 0 || rep.ZAdd.N == 0 {
		t.Fatalf("thin or failing run: %+v", rep)
	}
	if rep.Lag.N < 3 || rep.LagMissed != 0 || rep.Lag.Max > time.Second {
		t.Fatalf("replication lag: %+v missed=%d", rep.Lag, rep.LagMissed)
	}
	t.Log(rep)
}

// testPeerPW lets the seed and its joiner share a peer password, whether or
// not either happens to be the one whose config names the other.
const testPeerPW = "peer-test-pw"

func node(t *testing.T, join string) *server.Server {
	t.Helper()
	s, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 2, JoinAddr: join, PeerPassword: testPeerPW})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start("127.0.0.1:0"); err != nil {
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

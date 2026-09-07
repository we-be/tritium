package monitor_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/monitor"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/internal/server"
	"github.com/we-be/tritium/pkg/tritium"
)

// A snapshot reads each node's store through the node, and renders.
func TestSnapshotReadsStoreThroughNode(t *testing.T) {
	s, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	snap := monitor.New([]string{s.Addr()}, tritium.ClientOptions{}).Snapshot()
	if snap.Err != nil || len(snap.Nodes) != 1 {
		t.Fatalf("snapshot: %+v", snap)
	}
	if st := snap.Stores[snap.Nodes[0].ID]; !st.Healthy() {
		t.Fatalf("store through the node: %+v", st)
	}
	var out bytes.Buffer
	monitor.Render(&out, snap, false, time.Now(), time.Second)
	if !strings.Contains(out.String(), "Store ("+snap.Nodes[0].StoreAddr+")") {
		t.Fatalf("rendered:\n%s", out.String())
	}
}

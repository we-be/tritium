package server

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/pkg/tritium"
)

// A client's SET/GET mix through one node onto its embedded store, as many
// clients as GOMAXPROCS: where a node spends its time under load.
func BenchmarkNodeMixed(b *testing.B) {
	srv, err := New(config.Config{ListenAddr: "127.0.0.1:0", PoolSize: 4, Ownership: true})
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}
	defer srv.Stop()
	value := make([]byte, 64)
	var seq atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, err := tritium.NewClient(&tritium.ClientOptions{Address: srv.Addr()})
		if err != nil {
			b.Error(err)
			return
		}
		defer c.Close()
		ttl := 60
		for pb.Next() {
			n := seq.Add(1)
			key := "bench:" + strconv.FormatInt(n%10000, 10)
			if n%2 == 0 {
				if err := c.Set(key, value, &ttl); err != nil {
					b.Error(err)
					return
				}
			} else if _, err := c.Get(key); err != nil && err != tritium.ErrNotFound {
				b.Error(err)
				return
			}
		}
	})
}

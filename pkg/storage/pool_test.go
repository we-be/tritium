package storage

import (
	"testing"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
)

// A pooled connection that died while idle (the peer restarted) costs one
// redial, not the write: the batch goes again on a fresh connection.
func TestDeadPooledConnectionIsRetried(t *testing.T) {
	addr := resptest.Start(t).Addr()
	p, err := newPool(addr, 1, direct(""))
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	c := <-p.slots
	c.Conn.Close() // dead, but still in the pool
	p.slots <- c
	if v, err := p.do(resp.NewCommand("SET", "k", "v", "EX", "10")); err != nil || v != "OK" {
		t.Fatalf("SET over a dead connection = %v, %v", v, err)
	}
}

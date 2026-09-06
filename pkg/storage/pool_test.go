package storage

import (
	"testing"
	"time"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
)

// A pooled connection that died while idle (the peer restarted) costs one
// redial, not the write: the batch goes again on a fresh connection.
func TestDeadPooledConnectionIsRetried(t *testing.T) {
	addr := resptest.Start(t).Addr()
	p, err := newPool(addr, 2, direct(""))
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	for range 2 { // every slot dead at once, as after a peer restart
		c := <-p.slots
		c.Conn.Close()
		p.slots <- c
	}
	if v, err := p.do(resp.NewCommand("SET", "k", "v", "EX", "10")); err != nil || v != "OK" {
		t.Fatalf("SET over a dead connection = %v, %v", v, err)
	}
}

// A pool closed under an operation in flight fails fast instead of parking
// the caller on an empty channel forever (a detach racing a fan-out).
func TestClosedPoolFailsFast(t *testing.T) {
	addr := resptest.Start(t).Addr()
	p, err := newPool(addr, 1, direct(""))
	if err != nil {
		t.Fatal(err)
	}
	held := <-p.slots // the one slot is out, as during a fan-out
	p.close()
	done := make(chan error, 1)
	go func() { _, err := p.do(resp.NewCommand("PING")); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a closed pool answered")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("do on a closed pool blocked")
	}
	p.put(held, nil) // the in-flight caller returns its connection: must not block either
}

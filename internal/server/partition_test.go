package server

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

// link is a TCP proxy in front of a node that a test can cut: a partition
// with both sides alive and taking writes, which no stop or freeze
// reproduces. Peers dial the link; clients dial the node.
type link struct {
	ln    net.Listener
	to    string
	mu    sync.Mutex
	cut   bool
	conns []net.Conn
}

func newLink(t *testing.T, to string) *link {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &link{ln: ln, to: to}
	t.Cleanup(func() { ln.Close(); l.Cut() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			l.mu.Lock()
			cut := l.cut
			if !cut {
				l.conns = append(l.conns, c)
			}
			l.mu.Unlock()
			if cut {
				c.Close()
				continue
			}
			go l.pipe(c)
		}
	}()
	return l
}

func (l *link) pipe(c net.Conn) {
	up, err := net.Dial("tcp", l.to)
	if err != nil {
		c.Close()
		return
	}
	l.mu.Lock()
	l.conns = append(l.conns, up)
	l.mu.Unlock()
	go func() { io.Copy(up, c); up.Close() }()
	io.Copy(c, up)
	c.Close()
}

func (l *link) Addr() string { return l.ln.Addr().String() }

// Cut drops every connection and refuses new ones until Heal.
func (l *link) Cut() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cut = true
	for _, c := range l.conns {
		c.Close()
	}
	l.conns = nil
}

func (l *link) Heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cut = false
}

// Two live nodes cut off from each other keep taking writes; once the link
// is back, what each wrote reaches the other — the writes a held peer missed
// survive it being detached and re-attached — and neither reads as a new
// incarnation.
func TestPartitionHeals(t *testing.T) {
	hurry(t)
	if resptest.Addr(t) == resptest.Addr(t) {
		t.Skip("a shared store cannot be partitioned")
	}
	realA, realB := reserve(t), reserve(t)
	linkA, linkB := newLink(t, realA), newLink(t, realB)
	a, err := New(config.Config{StoreAddr: resptest.Addr(t), ListenAddr: realA, AdvertiseAddr: linkA.Addr(), PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(realA); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Stop() })
	b, err := New(config.Config{StoreAddr: resptest.Addr(t), ListenAddr: realB, AdvertiseAddr: linkB.Addr(), PoolSize: 2, JoinAddr: linkA.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(realB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Stop() })
	if err := b.Join(linkA.Addr()); err != nil {
		t.Fatal(err)
	}
	ca, cb := dial(t, a), dial(t, b)
	aID, bID := a.cluster.local.ID, b.cluster.local.ID
	waitFor(t, "both attached", func() bool { return len(a.store.Replicas()) == 1 && len(b.store.Replicas()) == 1 })
	ca.want("OK", "SET", "part:a", "1", "EX", "60")
	waitFor(t, "the first write to replicate", func() bool { v, _ := cb.do("GET", "part:a"); return string(v.([]byte)) == "1" })
	startedB := a.Nodes()[bID].Started

	linkA.Cut()
	linkB.Cut()
	ca.want("OK", "SET", "part:a", "2", "EX", "60") // b misses this
	cb.want("OK", "SET", "part:b", "1", "EX", "60") // a misses this
	waitFor(t, "both to read each other as down", func() bool {
		return a.Nodes()[bID].State == storage.NodeStateDown && b.Nodes()[aID].State == storage.NodeStateDown
	})
	cb.want("1", "GET", "part:a")

	linkA.Heal()
	linkB.Heal()
	waitFor(t, "a's update to reach b", func() bool { v, _ := cb.do("GET", "part:a"); return string(v.([]byte)) == "2" })
	waitFor(t, "b's write to reach a", func() bool { v, _ := ca.do("GET", "part:b"); return string(v.([]byte)) == "1" })
	if !a.Nodes()[bID].Started.Equal(startedB) {
		t.Fatal("a partition survivor came back as a new incarnation")
	}
}

// reserve picks a free loopback address a node can listen on later.
func reserve(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// Both sides of a partition write the same key; once it heals every node
// holds the later write, and a stale write or delete that turns up
// afterwards changes nothing.
func TestPartitionSettlesConflicts(t *testing.T) {
	hurry(t)
	realA, realB := reserve(t), reserve(t)
	linkA, linkB := newLink(t, realA), newLink(t, realB)
	a, err := New(config.Config{ListenAddr: realA, AdvertiseAddr: linkA.Addr(), PoolSize: 2}) // embedded stores keep stamps
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(realA); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Stop() })
	b, err := New(config.Config{ListenAddr: realB, AdvertiseAddr: linkB.Addr(), PoolSize: 2, JoinAddr: linkA.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(realB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Stop() })
	if err := b.Join(linkA.Addr()); err != nil {
		t.Fatal(err)
	}
	ca, cb := dial(t, a), dial(t, b)
	aID, bID := a.cluster.local.ID, b.cluster.local.ID
	waitFor(t, "both attached", func() bool { return len(a.store.Replicas()) == 1 && len(b.store.Replicas()) == 1 })
	ca.want("OK", "SET", "part:c", "0", "EX", "60")
	ca.want("OK", "SET", "part:d", "0", "EX", "60")
	waitFor(t, "the first writes to replicate", func() bool { v, _ := cb.do("GET", "part:d"); return string(v.([]byte)) == "0" })

	linkA.Cut()
	linkB.Cut()
	ca.want("OK", "SET", "part:c", "a-side", "EX", "60")
	ca.want(int64(1), "DEL", "part:d")
	time.Sleep(3 * time.Millisecond) // b writes later: the clocks are one machine's
	cb.want("OK", "SET", "part:c", "b-side", "EX", "60")
	cb.want("OK", "SET", "part:d", "b-side", "EX", "60")
	waitFor(t, "both to read each other as down", func() bool {
		return a.Nodes()[bID].State == storage.NodeStateDown && b.Nodes()[aID].State == storage.NodeStateDown
	})
	linkA.Heal()
	linkB.Heal()
	for _, key := range []string{"part:c", "part:d"} {
		waitFor(t, key+" to settle on both sides", func() bool {
			va, _ := ca.do("GET", key)
			vb, _ := cb.do("GET", key)
			return va != nil && vb != nil && string(va.([]byte)) == "b-side" && string(vb.([]byte)) == "b-side"
		})
	}

	pa := dial(t, a)
	pa.want("OK", "TRITIUM.REPLICATE", "STAMPED", "1", "SETEX", "part:c", "60", "stale")
	pa.want(int64(0), "TRITIUM.REPLICATE", "STAMPED", "1", "DEL", "part:c")
	ca.want("b-side", "GET", "part:c")
}

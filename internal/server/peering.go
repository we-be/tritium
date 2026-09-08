package server

import (
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// Peering for a node that can dial out but cannot be dialed back — the
// machines behind a home NAT, with mDNS names, next to a node on a public
// address. Such a node names the reachable peer in LINK_ADDRESS and opens the
// connections itself: TRITIUM.PEERLINK hands one over, and from there the two
// ends swap roles on that socket. The reachable node sends its fan-out down a
// connection it never opened, and we answer it exactly as if it had dialed us.
//
// Nothing downstream knows the difference. A parked connection is an ordinary
// peer connection, so a linked peer that goes quiet is held, its backlog
// parked by address, and replayed when it links again — the same path a
// partition already takes.

const (
	// A linked peer must keep enough connections parked to fill the far
	// node's replication pool (MAX_SERVER_CONNECTIONS, 4 by default) and the
	// single-connection pools a resync and a repair open on top of it.
	linkDepth = 8
	linkRetry = time.Second     // how often a peer that refused a link is tried again
	linkWait  = 2 * time.Second // how long a fan-out waits for a link before giving up on it
)

// links parks the connections inbound peers opened for us, by the address
// each advertises, so a dial for that address takes one instead of opening a
// socket to somewhere we cannot reach. Every parked connection is watched:
// the peer never speaks on it unasked, so anything read — an EOF once it
// closed the socket, a reset — means it is gone, and it leaves the park at
// once instead of failing the next attach or fan-out that takes it.
type links struct {
	mu   sync.Mutex
	by   map[string][]*parkedConn
	wake map[string]chan struct{} // closed, and forgotten, when addr gains a connection
}

// parkedConn is one connection in the park and the goroutine watching it.
type parkedConn struct {
	net.Conn
	done chan struct{} // closed once the watcher has stopped reading
}

func newLinks() *links {
	return &links{by: map[string][]*parkedConn{}, wake: map[string]chan struct{}{}}
}

// park hands a connection to whoever next dials addr.
func (l *links) park(addr string, c net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.by[addr]) >= 4*linkDepth {
		c.Close() // the peer is opening more than we could ever use
		return
	}
	p := &parkedConn{Conn: c, done: make(chan struct{})}
	l.by[addr] = append(l.by[addr], p)
	go l.watch(addr, p)
	if w, ok := l.wake[addr]; ok {
		close(w)
		delete(l.wake, addr)
	}
}

// watch reads a parked connection until the read returns: the peer closed
// it, the socket was reset, or take expired its deadline to reclaim it. Only
// a connection still in the park is dropped.
func (l *links) watch(addr string, p *parkedConn) {
	defer close(p.done)
	var b [1]byte
	p.Read(b[:])
	l.mu.Lock()
	defer l.mu.Unlock()
	if i := slices.Index(l.by[addr], p); i >= 0 {
		l.by[addr] = slices.Delete(l.by[addr], i, i+1)
		p.Close()
	}
}

// has reports whether addr is a peer served over connections it opened. We
// must never dial such a peer, and it gossips to us rather than the reverse.
func (l *links) has(addr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.by[addr]
	return ok
}

// parked is how many of addr's connections are waiting to be taken.
func (l *links) parked(addr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.by[addr])
}

// take waits briefly for one of addr's parked connections. Failing is the
// same as a failed dial: the replica is held until the peer links again.
func (l *links) take(addr string) (net.Conn, error) {
	deadline := time.Now().Add(linkWait)
	for {
		l.mu.Lock()
		if q := l.by[addr]; len(q) > 0 {
			p := q[len(q)-1]
			l.by[addr] = q[:len(q)-1]
			l.mu.Unlock()
			p.SetReadDeadline(time.Now()) // reclaim it from the watcher, which finds it gone from the park
			<-p.done
			p.SetReadDeadline(time.Time{})
			return p.Conn, nil
		}
		w, ok := l.wake[addr]
		if !ok {
			w = make(chan struct{})
			l.wake[addr] = w
		}
		l.mu.Unlock()
		t := time.NewTimer(time.Until(deadline))
		select {
		case <-w:
			t.Stop()
		case <-t.C:
			return nil, fmt.Errorf("no connection parked by %s", addr)
		}
	}
}

func (l *links) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for addr, q := range l.by {
		for _, p := range q {
			p.Close()
		}
		l.by[addr] = nil
	}
}

// handOff parks a connection a peer opened for us and takes its node into the
// view. We stop reading that socket here; from now on we send on it. The peer
// is learned after the connection is parked, since attaching it as a replica
// is what takes connections back out of the queue.
func (s *Server) handOff(n storage.NodeInfo, c net.Conn) {
	s.links.park(n.Addr, c)
	s.cluster.learn(n)
}

// linker keeps linkDepth connections open to one peer this node cannot be
// dialed by, replacing each as it ends.
type linker struct {
	srv  *Server
	addr string
	done <-chan struct{}
	wake chan struct{}

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	failed bool // so a peer that stays down is logged once, not every second
}

// startLinks opens the outbound peer links this node is configured for. It
// runs after the cluster exists, since every link introduces this node.
func (s *Server) startLinks() {
	for _, addr := range s.cfg.Links() {
		l := &linker{srv: s, addr: addr, done: s.linkDone, wake: make(chan struct{}, 1), conns: map[net.Conn]struct{}{}}
		s.linkWG.Go(func() { l.run() })
	}
}

func (l *linker) run() {
	t := time.NewTicker(linkRetry)
	defer t.Stop()
	for {
		l.fill()
		select {
		case <-l.done:
			l.closeAll()
			return
		case <-l.wake:
		case <-t.C:
		}
	}
}

func (l *linker) fill() {
	for l.count() < linkDepth {
		select { // run is still counted in linkWG here, so serving a new link can join it
		case <-l.done:
			return
		default:
		}
		if err := l.open(); err != nil {
			if !l.failed {
				slog.Warn("cluster: peer link refused, retrying", "peer", l.addr, "every", linkRetry, "err", err)
				l.failed = true
			}
			return
		}
		if l.failed {
			slog.Info("cluster: peer link open", "peer", l.addr)
			l.failed = false
		}
	}
}

// open dials the peer, introduces this node, and then serves the connection:
// past the +OK the peer is the one sending commands on it.
func (l *linker) open() error {
	conn, err := l.srv.dialPeer(l.addr)
	if err != nil {
		return err
	}
	r := resp.NewReader(conn)
	conn.SetDeadline(time.Now().Add(peerTimeout))
	if pw := l.srv.peerPassword(); pw != "" {
		if _, err := resp.NewCommand("AUTH", "peer", pw).Do(conn, r); err != nil {
			conn.Close()
			return fmt.Errorf("auth: %w", err)
		}
	}
	if _, err := resp.NewCommand("TRITIUM.PEERLINK", l.srv.cluster.localJSON()).Do(conn, r); err != nil {
		conn.Close()
		return err
	}
	conn.SetDeadline(time.Time{})
	l.add(conn)
	l.srv.linkWG.Go(func() {
		defer func() {
			l.remove(conn)
			select { // a link that ended is replaced now, not at the next tick
			case l.wake <- struct{}{}:
			default:
			}
		}()
		l.srv.serveConnWith(conn, r)
	})
	return nil
}

func (l *linker) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.conns)
}

func (l *linker) add(c net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conns[c] = struct{}{}
}

func (l *linker) remove(c net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.conns, c)
}

func (l *linker) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for c := range l.conns {
		c.Close()
	}
}

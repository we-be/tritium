package server

import (
	"fmt"
	"log/slog"
	"net"
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
// socket to somewhere we cannot reach.
type links struct {
	mu sync.Mutex
	by map[string]chan net.Conn
}

func newLinks() *links { return &links{by: map[string]chan net.Conn{}} }

func (l *links) queue(addr string) chan net.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	q, ok := l.by[addr]
	if !ok {
		q = make(chan net.Conn, 4*linkDepth)
		l.by[addr] = q
	}
	return q
}

// park hands a connection to whoever next dials addr.
func (l *links) park(addr string, c net.Conn) {
	select {
	case l.queue(addr) <- c:
	default:
		c.Close() // the peer is opening more than we could ever use
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

// take waits briefly for one of addr's parked connections. Failing is the
// same as a failed dial: the replica is held until the peer links again.
func (l *links) take(addr string) (net.Conn, error) {
	t := time.NewTimer(linkWait)
	defer t.Stop()
	select {
	case c := <-l.queue(addr):
		return c, nil
	case <-t.C:
		return nil, fmt.Errorf("no connection parked by %s", addr)
	}
}

func (l *links) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, q := range l.by {
		for {
			select {
			case c := <-q:
				c.Close()
			default:
				goto next
			}
		}
	next:
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
		s.linkers = append(s.linkers, l)
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

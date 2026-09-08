// Package server is a tritium node: a RESP front end over a replicated RESP
// store, plus the gossip that keeps nodes aware of each other.
package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/memstore"
	"github.com/we-be/tritium/internal/replica"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// DefaultTTL, in seconds, applies to writes that don't carry one. Every key
// in tritium expires.
const DefaultTTL = 17600

// asyncDepth is how many fan-outs an asynchronously fed peer may have
// queued before it is held and repaired instead.
const asyncDepth = 4096

// authTimeout is how long a connection may sit without authenticating: an
// open port on the internet collects connections that never say anything,
// and each holds a file descriptor and a goroutine until it goes.
var authTimeout = 10 * time.Second

// commandTimeout bounds an authenticated command once its first byte has
// arrived: idle time between commands stays unbounded (client pools and
// watchers sit quiet for hours), but a command fed in one byte at a time
// must finish within this or the connection is dropped.
var commandTimeout = 60 * time.Second

// Version is reported by INFO and HELLO. Release builds stamp it with -X;
// a `go install ...@vX.Y.Z` build takes it from the module version instead.
var Version = "dev"

func init() {
	if Version != "dev" {
		return
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		Version = shortVersion(bi.Main.Version)
	}
}

// shortVersion turns a module pseudo-version, what a build from a checkout
// reports (v0.9.1-0.20260907003558-86af7bd77692), into v0.9.1-dev.86af7bd;
// anything else is returned as is.
func shortVersion(v string) string {
	base, rest, ok := strings.Cut(v, "-0.")
	if !ok {
		return v
	}
	stamp, hash, ok := strings.Cut(rest, "-")
	if !ok || len(stamp) != 14 || len(hash) < 7 {
		return v
	}
	return base + "-dev." + hash[:7]
}

type Server struct {
	cfg       config.Config
	store     *replica.Store
	listener  net.Listener
	cluster   *cluster
	links     *links // connections peers that cannot be dialed opened for us
	linkDone  chan struct{}
	linkWG    sync.WaitGroup     // the link loops and what they serve; Stop waits for them
	tlsServer *tls.Config        // nil: plaintext listener
	tlsPeer   *tls.Config        // nil: plaintext peer dials
	embedded  *memstore.Listener // set when the node runs its own store
	memstore  *memstore.Store
	pconns    peerConns // authenticated connections to peers, for forwards and gossip
	clock     *clock    // stamps this node's writes
	guesses   guesses   // refused AUTHs by client address
	connMu    sync.Mutex
	conns     map[net.Conn]struct{} // accepted connections still being served: Stop closes them and waits, so no handler outlives the node
	sessMu    sync.Mutex
	sessions  map[int64]*session // every connection being served, links included, for CLIENT LIST
	connWG    sync.WaitGroup
	forwarded atomic.Int64 // writes carried to their owner, and writes done here because the owner was out of reach
	fallbacks atomic.Int64
	active    atomic.Int64
	bytes     atomic.Int64
	clientSeq atomic.Int64
	stopOnce  sync.Once
}

// New loads TLS material and connects to the node's RESP store. Nothing
// listens until Start.
func New(cfg config.Config) (*Server, error) {
	if cfg.Peering() && (cfg.PeerPassword == "" || cfg.PeerPassword == cfg.Password) {
		const msg = "PEER_PASSWORD is unset or equal to AUTH_PASSWORD, and this node peers (JOIN_ADDRESS, ADVERTISE_ADDRESS or LINK_ADDRESS is set)"
		if !cfg.AllowSharedPeerPassword {
			return nil, errors.New(msg + ": any client, or whoever holds AUTH_PASSWORD, could join the cluster; set a distinct PEER_PASSWORD, or ALLOW_SHARED_PEER_PASSWORD=true to mean it")
		}
		slog.Warn(msg + ", so any client that knows it can join the cluster")
	}
	tlsServer, tlsPeer, err := tlsConfigs(cfg)
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, tlsServer: tlsServer, tlsPeer: tlsPeer, links: newLinks(), linkDone: make(chan struct{})}
	if cfg.StoreAddr == "" {
		// The node's own store: reached over RESP like any other, through
		// connections that never leave the process.
		s.memstore = memstore.New(memstore.Options{MaxMemory: cfg.StoreMaxMemory, Version: Version})
		s.embedded = memstore.Listen()
		go s.memstore.Serve(s.embedded)
		via := replica.Transport{Dial: func(string) (net.Conn, error) { return s.embedded.Dial() }}
		s.store, err = replica.NewStoreVia(via, config.EmbeddedStore, cfg.PoolSize)
	} else if cfg.StoreTLS {
		var storeTLS *tls.Config
		if storeTLS, err = storeTLSConfig(cfg); err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
		s.store, err = replica.NewStoreTLS(cfg.StoreAddr, cfg.PoolSize, cfg.StorePassword, storeTLS)
	} else {
		s.store, err = replica.NewStore(cfg.StoreAddr, cfg.PoolSize, cfg.StorePassword)
	}
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	return s, nil
}

// peerPassword is what nodes present to each other as AUTH peer <password>.
func (s *Server) peerPassword() string {
	if s.cfg.PeerPassword != "" {
		return s.cfg.PeerPassword
	}
	return s.cfg.Password
}

// Start listens on addr (":0" picks a free port), with TLS when configured,
// and begins serving.
func (s *Server) Start(addr string) error {
	if s.cfg.Password == "" && !s.cfg.AllowNoAuth && !loopbackListener(addr) {
		return errors.New("AUTH_PASSWORD is unset and the listener is not loopback: every connection would be a client and a peer; set a password, or ALLOW_NO_AUTH=true to mean it")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if s.tlsServer != nil {
		ln = tls.NewListener(ln, s.tlsServer)
	}
	return s.Serve(ln)
}

// Serve seeds the cluster view with this node, advertised as
// cfg.AdvertiseAddr or the listener's address, and accepts connections in
// the background.
func (s *Server) Serve(ln net.Listener) error {
	s.listener = ln
	advertise := s.cfg.AdvertiseAddr
	if advertise == "" {
		advertise = ln.Addr().String()
	}
	s.store.SetReplicaTransport(s.peerTransport())
	s.clock = newClock(advertise)
	s.store.SetStamper(s.clock.next, s.cfg.StoreAddr == "")
	s.store.SetAsyncFor(asyncDepth, s.far)
	if s.cfg.Async {
		s.store.SetAsync(asyncDepth)
	}
	s.cluster = newCluster(s, advertise, s.cfg.StoreLabel(), s.cfg.Seeds())
	s.startLinks()
	s.connWG.Go(s.acceptLoop) // counted with the handlers it starts, so Stop never waits on an empty group one is about to join
	return nil
}

func (s *Server) acceptLoop() {
	for {
		c, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("accept failed", "err", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if s.cfg.MaxClients > 0 && s.active.Load() >= int64(s.cfg.MaxClients) {
			c.Write(resp.AppendError(nil, "ERR max number of clients reached"))
			c.Close()
			continue
		}
		s.connMu.Lock()
		if s.conns == nil {
			s.conns = map[net.Conn]struct{}{}
		}
		s.conns[c] = struct{}{}
		s.connMu.Unlock()
		keepalive(c)
		s.connWG.Go(func() {
			defer func() {
				s.connMu.Lock()
				delete(s.conns, c)
				s.connMu.Unlock()
			}()
			s.serveConn(c)
		})
	}
}

// loopbackListener reports whether addr binds this machine alone.
func loopbackListener(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// keepalive spreads a connection's TCP keepalive out: Go's default probes
// every socket 15 s after it goes idle and every 15 s after that, so the
// dozens of connections a fleet opens together — links, pools — probe
// together, and on the cloud node that lands as a burst of a hundred
// packets in ten milliseconds, past a nano instance's packet-rate
// allowance (seen with tcpdump: 184 packets in one 10 ms window, 143 of
// them empty ACKs, from 28 ports). Each connection now gets its own idle
// and interval, drawn at random from a range wide enough that no two
// stay in step, and long enough to cost little; a dead peer is still
// noticed within a few minutes, and gossip notices it in fifteen seconds
// regardless.
func keepalive(c net.Conn) {
	if tc, ok := c.(*tls.Conn); ok {
		c = tc.NetConn()
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAliveConfig(keepaliveConfig(rand.IntN))
	}
}

// keepaliveConfig draws the idle and interval for one connection: 30–60 s
// idle, 15–30 s between probes, five probes before the peer is given up.
func keepaliveConfig(intn func(int) int) net.KeepAliveConfig {
	return net.KeepAliveConfig{
		Enable:   true,
		Idle:     30*time.Second + time.Duration(intn(30000))*time.Millisecond,
		Interval: 15*time.Second + time.Duration(intn(15000))*time.Millisecond,
		Count:    5,
	}
}

// far reports whether addr is the peer on the other end of a link — one we
// opened (LINK_ADDRESS) or one it opened to us — and so on another network:
// it is fed from a queue, so no write here waits out the internet.
func (s *Server) far(addr string) bool {
	return slices.Contains(s.cfg.Links(), addr) || s.links.has(addr)
}

// dialPeer opens a connection to another node, over TLS when this node
// serves TLS.
func (s *Server) dialPeer(addr string) (net.Conn, error) {
	if s.links.has(addr) {
		return s.links.take(addr) // it cannot be dialed; it left us connections instead
	}
	d := net.Dialer{Timeout: peerTimeout}
	if s.tlsPeer == nil {
		c, err := d.Dial("tcp", addr)
		if err == nil {
			keepalive(c)
		}
		return c, err
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	cfg := s.tlsPeer.Clone()
	cfg.ServerName = host
	// Connect and handshake within one peerTimeout, but separately, so a
	// dial that times out says which of the two stalled and for how long.
	start := time.Now()
	raw, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	connected := time.Since(start)
	raw.SetDeadline(start.Add(peerTimeout))
	conn := tls.Client(raw, cfg)
	if err := conn.Handshake(); err != nil {
		raw.Close()
		return nil, fmt.Errorf("tls handshake with %s: %w (connect took %s, handshake %s)", addr, err, connected.Round(time.Millisecond), (time.Since(start) - connected).Round(time.Millisecond))
	}
	raw.SetDeadline(time.Time{})
	keepalive(raw)
	return conn, nil
}

// peerTransport reaches a peer's node the way gossip does — TLS when
// configured, AUTH as the peer user — and wraps every write in
// TRITIUM.REPLICATE, which the peer applies to its own store only.
func (s *Server) peerTransport() replica.Transport {
	t := replica.Transport{Dial: s.dialPeer, Wrap: "TRITIUM.REPLICATE", Timeout: 2 * time.Second}
	if pw := s.peerPassword(); pw != "" {
		t.Auth = resp.NewCommand("AUTH", "peer", pw)
	}
	return t
}

// Join adopts the cluster view of the node at addr and announces this node
// to every peer in it: what a started node does with its seeds, for tests
// that pair nodes after the fact.
func (s *Server) Join(addr string) error {
	if s.cluster == nil {
		return errors.New("join before Start")
	}
	return s.cluster.join(addr)
}

// Nodes is a snapshot of this node's cluster view.
func (s *Server) Nodes() map[string]storage.NodeInfo {
	if s.cluster == nil {
		return nil
	}
	return s.cluster.snapshot()
}

func (s *Server) Stats() storage.NodeStats {
	st := storage.NodeStats{
		ActiveConnections: s.active.Load(),
		BytesTransferred:  s.bytes.Load(),
		Replicas:          len(s.store.Replicas()),
		Held:              len(s.store.Held()),
		Queued:            len(s.store.Queued()),
		Writes:            s.store.Writes(),
	}
	if f, err := s.storeFields(); err == nil {
		st.Keys = storeKeys(f)
		st.Memory, _ = strconv.ParseInt(f["used_memory"], 10, 64)
	}
	return st
}

// Addr is the bound listener address, or "" before Start.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop closes the listener, halts gossip and drops store connections. It is
// safe to call more than once.
func (s *Server) Stop() error {
	var err error
	s.stopOnce.Do(func() {
		close(s.linkDone)
		s.linkWG.Wait()
		s.links.close()
		s.pconns.close()
		if s.cluster != nil {
			s.cluster.stop()
		}
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.connMu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.connMu.Unlock()
		s.connWG.Wait()
		s.store.Close()
		if s.embedded != nil {
			s.embedded.Close()
			s.memstore.Close()
		}
	})
	return err
}

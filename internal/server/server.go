// Package server is a tritium node: a RESP front end over a replicated RESP
// store, plus the gossip that keeps nodes aware of each other.
package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/pkg/storage"
)

// DefaultTTL, in seconds, applies to writes that don't carry one. Every key
// in tritium expires.
const DefaultTTL = 17600

// Version is reported by INFO and HELLO. Release builds stamp it with -X;
// a `go install ...@vX.Y.Z` build takes it from the module version instead.
var Version = "dev"

func init() {
	if Version != "dev" {
		return
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		Version = bi.Main.Version
	}
}

type Server struct {
	cfg       config.Config
	store     *storage.Store
	listener  net.Listener
	cluster   *cluster
	tlsServer *tls.Config // nil: plaintext listener
	tlsPeer   *tls.Config // nil: plaintext peer dials
	active    atomic.Int64
	bytes     atomic.Int64
	clientSeq atomic.Int64
	stopOnce  sync.Once
}

// New loads TLS material and connects to the node's RESP store. Nothing
// listens until Start.
func New(cfg config.Config) (*Server, error) {
	tlsServer, tlsPeer, err := tlsConfigs(cfg)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewStore(cfg.StoreAddr, cfg.PoolSize, cfg.StorePassword)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if cfg.Password != "" && cfg.PeerPassword == "" {
		slog.Warn("PEER_PASSWORD is unset, so any client that knows AUTH_PASSWORD can join the cluster")
	}
	return &Server{cfg: cfg, store: store, tlsServer: tlsServer, tlsPeer: tlsPeer}, nil
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
	s.cluster = newCluster(s, advertise, s.cfg.StoreAddr, s.cfg.Seeds())
	go s.acceptLoop()
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
		go s.serveConn(c)
	}
}

// dialPeer opens a connection to another node, over TLS when this node
// serves TLS.
func (s *Server) dialPeer(addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: peerTimeout}
	if s.tlsPeer == nil {
		return d.Dial("tcp", addr)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	cfg := s.tlsPeer.Clone()
	cfg.ServerName = host
	return tls.DialWithDialer(&d, "tcp", addr, cfg)
}

// Join adopts the cluster view of the node at addr and announces this node
// to every peer in it.
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
	return storage.NodeStats{
		ActiveConnections: s.active.Load(),
		BytesTransferred:  s.bytes.Load(),
	}
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
		if s.cluster != nil {
			s.cluster.stop()
		}
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.store.Close()
	})
	return err
}

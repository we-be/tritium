// Package server is a tritium node: an RPC front end over a replicated RESP
// store, plus the gossip that keeps nodes aware of each other.
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/pkg/storage"
)

// DefaultTTL, in seconds, applies to Set calls that don't carry one.
const DefaultTTL = 17600

type Server struct {
	cfg      config.Config
	store    *storage.Store
	rpc      *rpc.Server
	listener net.Listener
	cluster  *cluster
	active   atomic.Int64
	bytes    atomic.Int64
	stopOnce sync.Once
	done     chan struct{}
}

// New connects to the node's RESP store and registers the RPC service.
// Nothing listens until Start.
func New(cfg config.Config) (*Server, error) {
	store, err := storage.NewStore(cfg.StoreAddr, cfg.PoolSize)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	s := &Server{cfg: cfg, store: store, rpc: rpc.NewServer(), done: make(chan struct{})}
	if err := s.rpc.RegisterName("Store", s); err != nil {
		store.Close()
		return nil, fmt.Errorf("register rpc: %w", err)
	}
	return s, nil
}

// Start listens on addr (":0" picks a free port) and begins serving.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve seeds the cluster view with this node, advertised as
// cfg.AdvertiseAddr or the listener's address, and accepts RPC connections
// in the background.
func (s *Server) Serve(ln net.Listener) error {
	s.listener = ln
	advertise := s.cfg.AdvertiseAddr
	if advertise == "" {
		advertise = ln.Addr().String()
	}
	s.cluster = newCluster(s, advertise, s.cfg.StoreAddr, s.cfg.JoinAddr == "")
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

func (s *Server) serveConn(c net.Conn) {
	s.active.Add(1)
	defer s.active.Add(-1)
	s.rpc.ServeConn(c)
}

// Set is the Store.Set RPC.
func (s *Server) Set(args *storage.SetArgs, reply *storage.SetReply) error {
	ttl := DefaultTTL
	if args.TTL != nil {
		ttl = *args.TTL
	}
	if err := s.store.Set(args.Key, args.Value, ttl); err != nil {
		reply.Error = err.Error()
		return nil
	}
	s.bytes.Add(int64(len(args.Value)))
	return nil
}

// Get is the Store.Get RPC.
func (s *Server) Get(args *storage.GetArgs, reply *storage.GetReply) error {
	v, err := s.store.Get(args.Key)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}
	reply.Value = v
	s.bytes.Add(int64(len(v)))
	return nil
}

// Delete is the Store.Delete RPC.
func (s *Server) Delete(args *storage.DeleteArgs, reply *storage.DeleteReply) error {
	ok, err := s.store.Delete(args.Key)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}
	reply.Deleted = ok
	return nil
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
		close(s.done)
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

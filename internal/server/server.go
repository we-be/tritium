package server

import (
	"fmt"
	"net"
	"net/rpc"
	"sync/atomic"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/pkg/storage"
)

const DefaultTTL int = 17600

type Server struct {
	store    *storage.RespServer
	listener net.Listener
	rpc      *rpc.Server
	stats    ServerStats
	stopCh   chan struct{}
	cluster  *ClusterInfo
}

type ServerStats struct {
	ActiveConnections int64
	BytesTransferred  int64
}

func NewServerBare() (*Server, error) {
	s := &Server{
		rpc:    rpc.NewServer(),
		stopCh: make(chan struct{}),
	}

	// Register our Store RPC interface
	if err := s.rpc.RegisterName("Store", s); err != nil {
		return nil, fmt.Errorf("failed to register RPC: %w", err)
	}

	return s, nil
}

// NewServer creates a new Tritium server
func NewServer(config config.Config) (*Server, error) {
	// Initialize with empty replica list
	store, err := storage.NewRespServer(
		config.MemStoreAddr,
		config.MaxConnections,
		[]string{}, // Start with no replicas
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create RESP server: %w", err)
	}

	srv := &Server{
		store:  store,
		rpc:    rpc.NewServer(),
		stopCh: make(chan struct{}),
	}

	// Register RPC methods
	if err := srv.rpc.RegisterName("Store", srv); err != nil {
		return nil, fmt.Errorf("failed to register RPC methods: %w", err)
	}

	// Initialize cluster capabilities
	if err := srv.initCluster(config.RPCAddr, config.MemStoreAddr); err != nil {
		return nil, fmt.Errorf("failed to initialize cluster: %w", err)
	}

	// If we have a join address, join the cluster
	if config.JoinAddr != "" {
		if err := srv.JoinCluster(config.JoinAddr); err != nil {
			return nil, fmt.Errorf("failed to join cluster: %w", err)
		}
	}

	return srv, nil
}

// Start starts the RPC server
func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	s.listener = listener

	// Update our cluster's local node RPC address with the actual address
	if s.cluster != nil {
		s.cluster.mu.Lock()
		s.cluster.localNode.RPCAddr = s.listener.Addr().String()
		s.cluster.mu.Unlock()
		fmt.Printf("[Start] Updated local node RPCAddr: %s\n", s.cluster.localNode.RPCAddr)
	}

	// Accept connections
	go s.acceptLoop()
	return nil
}

func (s *Server) InitCluster(rpcAddr, memStoreAddr string) error {
	// Create a new RespServer with no replicas initially
	store, err := storage.NewRespServer(memStoreAddr, 4, []string{})
	if err != nil {
		return fmt.Errorf("failed to create RESP server: %w", err)
	}
	s.store = store

	// Actually initialize the cluster info
	return s.initCluster(rpcAddr, memStoreAddr)
}

func (s *Server) StartWithListener(listener net.Listener) error {
	s.listener = listener
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				// handle error
				return
			}
			go s.rpc.ServeConn(conn)
		}
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.stats.ActiveConnections, -1)
	}()
	s.rpc.ServeConn(conn)
}

// Set handles the Set RPC call
func (s *Server) Set(args *storage.SetArgs, reply *storage.SetReply) error {
	if args == nil {
		reply.Error = "invalid arguments"
		return nil
	}

	// Use default TTL if none provided
	ttl := DefaultTTL
	if args.TTL != nil {
		ttl = *args.TTL
	}

	value := string(args.Value)
	_, err := s.store.SetEx(args.Key, ttl, value)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}

	atomic.AddInt64(&s.stats.BytesTransferred, int64(len(args.Value)))
	return nil
}

// Get handles the Get RPC call
func (s *Server) Get(args *storage.GetArgs, reply *storage.GetReply) error {
	if args == nil {
		reply.Error = "invalid arguments"
		return nil
	}

	value, err := s.store.Get(args.Key)
	if err != nil {
		reply.Error = err.Error()
		return nil
	}

	switch v := value.(type) {
	case []byte:
		if len(v) == 0 {
			reply.Error = "key not found"
			return nil
		}
		reply.Value = v
		atomic.AddInt64(&s.stats.BytesTransferred, int64(len(v)))
	case string:
		if v == "" {
			reply.Error = "key not found"
			return nil
		}
		reply.Value = []byte(v)
		atomic.AddInt64(&s.stats.BytesTransferred, int64(len(v)))
	case nil:
		reply.Error = "key not found"
	default:
		reply.Error = "unexpected value type"
	}

	return nil
}

// Stats returns current server statistics
func (s *Server) Stats() ServerStats {
	return ServerStats{
		ActiveConnections: atomic.LoadInt64(&s.stats.ActiveConnections),
		BytesTransferred:  atomic.LoadInt64(&s.stats.BytesTransferred),
	}
}

// Stop gracefully shuts down the server
func (s *Server) Stop() error {
	// Signal acceptLoop to stop
	close(s.stopCh)

	// Stop cluster operations
	if s.cluster != nil {
		s.cluster.stopCluster()
	}

	// Close listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	// Close RESP store
	if err := s.store.Close(); err != nil {
		return fmt.Errorf("failed to close store: %w", err)
	}

	return nil
}

func (s *Server) GetAddress() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// When a node joins the cluster, add its RESP server as a replica
func (s *Server) addNodeAsReplica(node *NodeInfo) error {
	maxConn := s.store.GetMaxConnections()
	return s.store.AddReplica(node.RespAddr, maxConn)
}

// When a node leaves the cluster, remove its RESP server replica
func (s *Server) removeNodeReplica(node *NodeInfo) error {
	return s.store.RemoveReplica(node.RespAddr)
}

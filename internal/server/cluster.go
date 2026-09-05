package server

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/we-be/tritium/pkg/storage"
)

const (
	gossipInterval = 5 * time.Second
	healthInterval = 5 * time.Second
	degradedAfter  = 10 * time.Second // no word from a peer for this long: degraded
	downAfter      = 15 * time.Second // ...for this long: down, stop replicating to it
	evictAfter     = 60 * time.Second // ...for this long: forget it entirely
	rpcDialTimeout = 3 * time.Second
)

// cluster is this node's view of its peers, kept fresh by announce-on-join,
// random-peer gossip every gossipInterval, and LastSeen-based health checks.
// Every live peer's RESP store is attached to the local Store as a replica.
type cluster struct {
	server *Server
	mu     sync.RWMutex
	nodes  map[string]*storage.NodeInfo
	local  *storage.NodeInfo
	done   chan struct{}
}

func newCluster(s *Server, rpcAddr, respAddr string, seed bool) *cluster {
	local := &storage.NodeInfo{
		ID:       "node-" + rpcAddr,
		RPCAddr:  rpcAddr,
		RespAddr: respAddr,
		State:    storage.NodeStateHealthy,
		LastSeen: time.Now(),
		IsLeader: seed,
	}
	c := &cluster{
		server: s,
		nodes:  map[string]*storage.NodeInfo{local.ID: local},
		local:  local,
		done:   make(chan struct{}),
	}
	go c.loop()
	slog.Info("cluster: node registered", "id", local.ID, "store", respAddr, "seed", seed)
	return c
}

func (c *cluster) loop() {
	gossip := time.NewTicker(gossipInterval)
	defer gossip.Stop()
	health := time.NewTicker(healthInterval)
	defer health.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-gossip.C:
			c.gossip()
		case <-health.C:
			c.checkHealth()
			c.touchLocal()
		}
	}
}

func (c *cluster) stop() { close(c.done) }

func dialRPC(addr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, rpcDialTimeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

// join fetches the cluster view from a known node, adopts it, and announces
// this node to every peer in it.
func (c *cluster) join(addr string) error {
	client, err := dialRPC(addr)
	if err != nil {
		return fmt.Errorf("join %s: %w", addr, err)
	}
	defer client.Close()
	var nodes map[string]storage.NodeInfo
	if err := client.Call("Store.GetClusterNodes", struct{}{}, &nodes); err != nil {
		return fmt.Errorf("join %s: %w", addr, err)
	}
	c.merge(nodes)
	c.announce()
	return nil
}

// announce introduces this node to every known peer and merges each reply,
// so nodes joining at the same moment learn of each other right away.
func (c *cluster) announce() {
	local := c.localCopy()
	for _, p := range c.peers() {
		client, err := dialRPC(p.RPCAddr)
		if err != nil {
			slog.Warn("cluster: announce dial failed", "peer", p.ID, "err", err)
			continue
		}
		var view map[string]storage.NodeInfo
		err = client.Call("Store.GossipExchange", &local, &view)
		client.Close()
		if err != nil {
			slog.Warn("cluster: announce failed", "peer", p.ID, "err", err)
			continue
		}
		c.merge(view)
	}
}

// gossip exchanges views with one random peer, down ones included so a
// restarted node gets rediscovered.
func (c *cluster) gossip() {
	peers := c.peers()
	if len(peers) == 0 {
		return
	}
	p := peers[rand.IntN(len(peers))]
	client, err := dialRPC(p.RPCAddr)
	if err != nil {
		slog.Debug("cluster: gossip dial failed", "peer", p.ID, "err", err)
		return
	}
	defer client.Close()
	local := c.localCopy()
	var remote map[string]storage.NodeInfo
	if err := client.Call("Store.GossipExchange", &local, &remote); err != nil {
		slog.Debug("cluster: gossip failed", "peer", p.ID, "err", err)
		return
	}
	c.merge(remote)
}

// merge adopts every remote entry that is newer than ours. A peer we are
// hearing about for the first time, or one back from the dead, gets its
// store attached as a replica.
func (c *cluster) merge(remote map[string]storage.NodeInfo) {
	var attach []storage.NodeInfo
	c.mu.Lock()
	for id, n := range remote {
		if id == c.local.ID {
			continue
		}
		cur, known := c.nodes[id]
		if known && !n.LastSeen.After(cur.LastSeen) {
			continue
		}
		if time.Since(n.LastSeen) < downAfter && (!known || cur.State == storage.NodeStateDown) {
			attach = append(attach, n)
		}
		c.nodes[id] = &n
	}
	c.mu.Unlock()
	for _, n := range attach {
		c.attach(n)
	}
}

func (c *cluster) attach(n storage.NodeInfo) {
	if n.RespAddr == c.local.RespAddr {
		return // sharing our store; replicating to it would be a self-write
	}
	if err := c.server.store.AddReplica(n.RespAddr); err != nil {
		slog.Warn("cluster: attach replica failed", "peer", n.ID, "store", n.RespAddr, "err", err)
		return
	}
	slog.Info("cluster: peer attached", "peer", n.ID, "store", n.RespAddr)
}

func (c *cluster) detach(n storage.NodeInfo) {
	if c.server.store.RemoveReplica(n.RespAddr) {
		slog.Info("cluster: peer detached", "peer", n.ID, "store", n.RespAddr)
	}
}

func (c *cluster) checkHealth() {
	var detach []storage.NodeInfo
	c.mu.Lock()
	now := time.Now()
	for id, n := range c.nodes {
		if id == c.local.ID {
			continue
		}
		age := now.Sub(n.LastSeen)
		was := n.State
		switch {
		case age > downAfter:
			n.State = storage.NodeStateDown
		case age > degradedAfter:
			n.State = storage.NodeStateDegraded
		default:
			n.State = storage.NodeStateHealthy
		}
		if n.State == storage.NodeStateDown && was != storage.NodeStateDown {
			detach = append(detach, *n)
		}
		if age > evictAfter {
			delete(c.nodes, id)
			slog.Info("cluster: peer evicted", "peer", id, "silent_for", age.Round(time.Second))
		}
	}
	c.mu.Unlock()
	for _, n := range detach {
		c.detach(n)
	}
}

func (c *cluster) touchLocal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.local.Stats = c.server.Stats()
	c.local.LastSeen = time.Now()
}

func (c *cluster) localCopy() storage.NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.local
}

func (c *cluster) peers() []storage.NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]storage.NodeInfo, 0, len(c.nodes)-1)
	for id, n := range c.nodes {
		if id != c.local.ID {
			out = append(out, *n)
		}
	}
	return out
}

func (c *cluster) snapshot() map[string]storage.NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]storage.NodeInfo, len(c.nodes))
	for id, n := range c.nodes {
		out[id] = *n
	}
	return out
}

// RegisterNode is the Store.RegisterNode RPC: a peer announcing itself.
// Kept for older nodes; new ones announce through GossipExchange.
func (s *Server) RegisterNode(n *storage.NodeInfo, _ *struct{}) error {
	n.LastSeen = time.Now()
	s.cluster.merge(map[string]storage.NodeInfo{n.ID: *n})
	return nil
}

// GetClusterNodes is the Store.GetClusterNodes RPC.
func (s *Server) GetClusterNodes(_ struct{}, reply *map[string]storage.NodeInfo) error {
	*reply = s.cluster.snapshot()
	return nil
}

// GossipExchange is the Store.GossipExchange RPC: learn the caller, return our view.
func (s *Server) GossipExchange(peer *storage.NodeInfo, reply *map[string]storage.NodeInfo) error {
	peer.LastSeen = time.Now()
	s.cluster.merge(map[string]storage.NodeInfo{peer.ID: *peer})
	*reply = s.cluster.snapshot()
	return nil
}

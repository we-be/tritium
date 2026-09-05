package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

const (
	gossipInterval = 5 * time.Second
	healthInterval = 5 * time.Second
	degradedAfter  = 10 * time.Second // no word from a peer for this long: degraded
	downAfter      = 15 * time.Second // ...for this long: down, stop replicating to it
	evictAfter     = 60 * time.Second // ...for this long: forget it entirely
	peerTimeout    = 3 * time.Second  // dial plus one round trip to a peer
)

// cluster is this node's view of its peers, kept fresh by announce-on-join,
// random-peer gossip every gossipInterval, and LastSeen-based health checks.
// Every live peer's RESP store is attached to the local Store as a replica.
// Nodes talk to each other with TRITIUM.NODES and TRITIUM.GOSSIP, which
// carry the view as JSON.
type cluster struct {
	server *Server
	mu     sync.RWMutex
	nodes  map[string]*storage.NodeInfo
	local  *storage.NodeInfo
	done   chan struct{}
}

func newCluster(s *Server, addr, storeAddr string, seed bool) *cluster {
	local := &storage.NodeInfo{
		ID:        "node-" + addr,
		Addr:      addr,
		StoreAddr: storeAddr,
		State:     storage.NodeStateHealthy,
		LastSeen:  time.Now(),
		IsLeader:  seed,
	}
	c := &cluster{
		server: s,
		nodes:  map[string]*storage.NodeInfo{local.ID: local},
		local:  local,
		done:   make(chan struct{}),
	}
	go c.loop()
	slog.Info("cluster: node registered", "id", local.ID, "store", storeAddr, "seed", seed)
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

// join fetches the cluster view from a known node, adopts it, and announces
// this node to every peer in it.
func (c *cluster) join(addr string) error {
	view, err := c.exchange(addr, resp.NewCommand("TRITIUM.NODES"))
	if err != nil {
		return fmt.Errorf("join %s: %w", addr, err)
	}
	c.merge(view)
	c.announce()
	return nil
}

// announce introduces this node to every known peer and merges each reply,
// so nodes joining at the same moment learn of each other right away.
func (c *cluster) announce() {
	local := c.localJSON()
	for _, p := range c.peers() {
		view, err := c.exchange(p.Addr, resp.NewCommand("TRITIUM.GOSSIP", local))
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
	view, err := c.exchange(p.Addr, resp.NewCommand("TRITIUM.GOSSIP", c.localJSON()))
	if err != nil {
		slog.Debug("cluster: gossip failed", "peer", p.ID, "err", err)
		return
	}
	c.merge(view)
}

// exchange sends one command to a peer, authenticating first when the
// cluster has a password, and decodes the JSON view it replies with.
func (c *cluster) exchange(addr string, cmd resp.Command) (map[string]storage.NodeInfo, error) {
	conn, err := c.server.dialPeer(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(peerTimeout))
	r := resp.NewReader(conn)
	if pw := c.server.cfg.Password; pw != "" {
		if _, err := resp.NewCommand("AUTH", pw).Do(conn, r); err != nil {
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	v, err := cmd.Do(conn, r)
	if err != nil {
		return nil, err
	}
	raw, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected reply %T", v)
	}
	var view map[string]storage.NodeInfo
	if err := json.Unmarshal(raw, &view); err != nil {
		return nil, fmt.Errorf("decode view: %w", err)
	}
	return view, nil
}

// learn records a peer that just spoke to us.
func (c *cluster) learn(n storage.NodeInfo) {
	n.LastSeen = time.Now()
	c.merge(map[string]storage.NodeInfo{n.ID: n})
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
	if n.StoreAddr == c.local.StoreAddr {
		return // sharing our store; replicating to it would be a self-write
	}
	if err := c.server.store.AddReplica(n.StoreAddr); err != nil {
		slog.Warn("cluster: attach replica failed", "peer", n.ID, "store", n.StoreAddr, "err", err)
		return
	}
	slog.Info("cluster: peer attached", "peer", n.ID, "store", n.StoreAddr)
}

func (c *cluster) detach(n storage.NodeInfo) {
	if c.server.store.RemoveReplica(n.StoreAddr) {
		slog.Info("cluster: peer detached", "peer", n.ID, "store", n.StoreAddr)
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

func (c *cluster) localJSON() string {
	b, _ := json.Marshal(c.localCopy())
	return string(b)
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

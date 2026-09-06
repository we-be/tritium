package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

const peerTimeout = 3 * time.Second // dial plus one round trip to a peer

// The cluster's clocks. Variables so tests can hurry them.
var (
	gossipInterval = 5 * time.Second
	healthInterval = 5 * time.Second
	degradedAfter  = 10 * time.Second // no word from a peer for this long: degraded
	downAfter      = 15 * time.Second // ...for this long: down, stop replicating to it
	evictAfter     = 60 * time.Second // ...for this long: forget it entirely
	rejoinInterval = 5 * time.Second  // how often a configured seed that is not a live peer is dialed again
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
	seeds  []string        // configured peers, dialed until they answer and again whenever they drop out
	failed map[string]bool // seeds whose last attempt failed, so a retry is logged once, not every tick
	done   chan struct{}
	wg     sync.WaitGroup // the loops; stop waits for them so nothing gossips after Stop returns
}

func newCluster(s *Server, addr, storeAddr string, seeds []string) *cluster {
	local := &storage.NodeInfo{
		ID:        "node-" + addr,
		Addr:      addr,
		StoreAddr: storeAddr,
		State:     storage.NodeStateHealthy,
		LastSeen:  time.Now(),
		IsLeader:  len(seeds) == 0,
		Started:   time.Now(),
	}
	c := &cluster{
		server: s,
		nodes:  map[string]*storage.NodeInfo{local.ID: local},
		local:  local,
		seeds:  seeds,
		failed: map[string]bool{},
		done:   make(chan struct{}),
	}
	c.wg.Go(c.loop)
	if len(seeds) > 0 {
		c.wg.Go(c.seedLoop)
	}
	slog.Info("cluster: node registered", "id", local.ID, "store", storeAddr, "seeds", seeds)
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

func (c *cluster) stop() {
	close(c.done)
	c.wg.Wait()
}

// seedLoop keeps this node attached to its configured seeds: a seed that is
// down at boot is joined when it comes up, and one evicted after an outage is
// joined again when it returns, whichever side restarted.
func (c *cluster) seedLoop() {
	t := time.NewTicker(rejoinInterval)
	defer t.Stop()
	for {
		c.rejoin()
		select {
		case <-c.done:
			return
		case <-t.C:
		}
	}
}

// rejoin dials every seed that is not currently a live peer.
func (c *cluster) rejoin() {
	for _, addr := range c.seeds {
		if addr == c.local.Addr || c.livePeer(addr) {
			continue
		}
		if err := c.join(addr); err != nil {
			if !c.failed[addr] {
				slog.Warn("cluster: seed unreachable, retrying", "seed", addr, "every", rejoinInterval, "err", err)
			}
			c.failed[addr] = true
			continue
		}
		c.failed[addr] = false
		slog.Info("cluster: joined", "via", addr)
	}
}

func (c *cluster) livePeer(addr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.nodes {
		if n.Addr == addr && n.State != storage.NodeStateDown {
			return true
		}
	}
	return false
}

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
	if pw := c.server.peerPassword(); pw != "" {
		if _, err := resp.NewCommand("AUTH", "peer", pw).Do(conn, r); err != nil {
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
// hearing about for the first time, one back from the dead, or one that
// restarted since we last saw it — a new Started, even inside the window
// where it never read as down — gets its store attached as a replica and
// brought up to date. A restart is a fresh incarnation: its old connections
// are dropped and, as with a return from down, our copies win there.
func (c *cluster) merge(remote map[string]storage.NodeInfo) {
	type attaching struct {
		node      storage.NodeInfo
		wasDown   bool
		restarted bool
	}
	var attach []attaching
	c.mu.Lock()
	for id, n := range remote {
		if id == c.local.ID {
			continue
		}
		cur, known := c.nodes[id]
		if known && !n.LastSeen.After(cur.LastSeen) {
			continue
		}
		restarted := known && n.Started.After(cur.Started)
		if time.Since(n.LastSeen) < downAfter && (!known || cur.State == storage.NodeStateDown || restarted) {
			attach = append(attach, attaching{n, known, restarted})
		}
		c.nodes[id] = &n
	}
	c.mu.Unlock()
	for _, a := range attach {
		if a.restarted {
			c.server.store.RemoveReplica(a.node.Addr)
		}
		c.attach(a.node, a.wasDown)
	}
}

// attach starts replicating to a peer — through its node, which applies our
// writes to its own store — and, in the background, copies what it has
// missed. A peer we watched go down and return is stale, so our copy of
// every key wins there; one we are meeting for the first time keeps what it
// holds and only has its gaps filled — it may be the survivor and we the one
// that just started.
func (c *cluster) attach(n storage.NodeInfo, wasDown bool) {
	if n.StoreAddr == c.local.StoreAddr && !loopback(c.local.StoreAddr) {
		return // sharing our store; replicating to it would be a self-write (a loopback store is never shared)
	}
	if err := c.server.store.AddReplica(n.Addr); err != nil {
		slog.Warn("cluster: attach replica failed", "peer", n.ID, "err", err)
		return
	}
	slog.Info("cluster: peer attached", "peer", n.ID, "store", n.StoreAddr)
	go func() {
		copied, err := c.server.store.Sync(n.Addr, wasDown)
		if err != nil {
			slog.Warn("cluster: resync incomplete", "peer", n.ID, "keys", copied, "err", err)
			return
		}
		slog.Info("cluster: resynced", "peer", n.ID, "keys", copied, "overwrite", wasDown)
	}()
}

// loopback reports whether addr names this machine only, so equal loopback
// store addresses on two nodes are two stores, not one.
func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *cluster) detach(n storage.NodeInfo) {
	if c.server.store.RemoveReplica(n.Addr) {
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

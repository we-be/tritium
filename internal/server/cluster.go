package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/we-be/tritium/internal/config"
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
// random-peer gossip every gossipInterval (plus every peer a round stale),
// and LastSeen-based health checks.
// Every live peer's RESP store is attached to the local Store as a replica.
// Nodes talk to each other with TRITIUM.NODES and TRITIUM.GOSSIP, which
// carry the view as JSON.
type cluster struct {
	server    *Server
	mu        sync.RWMutex
	nodes     map[string]*storage.NodeInfo
	local     *storage.NodeInfo
	seeds     []string             // configured peers, dialed until they answer and again whenever they drop out
	failed    map[string]bool      // seeds whose last attempt failed, so a retry is logged once, not every tick
	gone      map[string]time.Time // Started of every peer forgotten after an outage, so its restart still reads as one
	tick      time.Time            // when the health check last ran; a long gap means this node was the one away
	done      chan struct{}
	wg        sync.WaitGroup // the loops; stop waits for them so nothing gossips after Stop returns
	repairing atomic.Bool
	attaching sync.Mutex // one attach at a time, so two merges cannot both find a peer missing
	events    *eventLog
}

func newCluster(s *Server, addr, storeAddr string, seeds []string) *cluster {
	local := &storage.NodeInfo{
		ID:        "node-" + addr,
		Addr:      addr,
		StoreAddr: storeAddr,
		State:     storage.NodeStateHealthy,
		LastSeen:  time.Now(),
		IsLeader:  len(seeds) == 0 && s.cfg.Weight() > 0,
		Started:   time.Now(),
		Version:   Version,
		Seeds:     seeds,

		Electronegativity: s.cfg.Electronegativity,
	}
	events := newEventLog(local.ID, s.store)
	s.store.SetHoldHook(func(addr string) { events.emit("hold", addr, 0, 0) })
	s.store.SetRepairHook(func(addr string, keys int) { events.emit("repair", addr, keys, 0) })
	c := &cluster{
		server: s,
		nodes:  map[string]*storage.NodeInfo{local.ID: local},
		local:  local,
		seeds:  seeds,
		gone:   map[string]time.Time{},
		tick:   time.Now(),
		failed: map[string]bool{},
		done:   make(chan struct{}),
		events: events,
	}
	events.emit("start", "", 0, 0)
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
			if c.repairing.CompareAndSwap(false, true) { // a held replica's replay may take a while; never two at once
				c.wg.Go(func() {
					defer c.repairing.Store(false)
					c.server.store.Repair()
				})
			}
		}
	}
}

func (c *cluster) stop() {
	close(c.done)
	c.wg.Wait()
	c.events.stop()
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
	for _, p := range c.dialable(c.peers()) {
		view, err := c.exchange(p.Addr, resp.NewCommand("TRITIUM.GOSSIP", local))
		if err != nil {
			slog.Warn("cluster: announce failed", "peer", p.ID, "err", err)
			continue
		}
		c.merge(view)
	}
}

// gossip exchanges views with one random peer, down ones included so a
// restarted node gets rediscovered, and with every peer not heard from
// since the last round: a peer that never dials us — the one at the far
// end of a link cannot — would otherwise be refreshed only when the random
// pick lands on it, and three misses in a row read as down. (Seen on the
// fleet: the two machines wrote the cloud node off 38 times in six hours,
// often in the same second, while it never lost either of them.)
func (c *cluster) gossip() {
	var wg sync.WaitGroup
	for _, p := range gossipTargets(c.dialable(c.peers())) {
		wg.Go(func() {
			view, err := c.exchange(p.Addr, resp.NewCommand("TRITIUM.GOSSIP", c.localJSON()))
			if err != nil {
				slog.Debug("cluster: gossip failed", "peer", p.ID, "err", err)
				return
			}
			c.merge(view)
		})
	}
	wg.Wait()
}

// gossipTargets is one of peers at random plus every one a round old or
// more, each once.
func gossipTargets(peers []storage.NodeInfo) []storage.NodeInfo {
	if len(peers) == 0 {
		return nil
	}
	pick := rand.IntN(len(peers))
	out := []storage.NodeInfo{peers[pick]}
	for i, p := range peers {
		if i != pick && time.Since(p.LastSeen) >= gossipInterval {
			out = append(out, p)
		}
	}
	return out
}

// exchange sends one command to a peer over a pooled peer connection —
// dialed and authenticated once, kept between rounds, so a gossip round
// costs one round trip rather than a dial, a handshake and an AUTH — and
// decodes the JSON view it replies with. A connection that fails is
// dropped; the next round dials afresh.
func (c *cluster) exchange(addr string, cmd resp.Command) (map[string]storage.NodeInfo, error) {
	conn, err := c.server.peerConn(addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(peerTimeout))
	v, err := cmd.Do(conn, conn.r)
	if err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Time{})
	c.server.pconns.put(addr, conn, c.server.cfg.PoolSize)
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

// merge adopts every remote entry that is newer than ours and makes sure
// every live peer is attached as a replica. A peer that restarted since we
// last saw it — a newer Started, whether it read as down meanwhile, was
// forgotten, or never left the window — is a fresh incarnation: its old
// connections are dropped and our copy of every key wins there. Any other
// peer we are not replicating to yet, met for the first time or back from a
// partition both of us lived through, keeps what it holds and only has its
// gaps filled.
func (c *cluster) merge(remote map[string]storage.NodeInfo) {
	type attaching struct {
		node      storage.NodeInfo
		restarted bool
	}
	var attach []attaching
	replicating := c.server.store.Replicas()
	allow := c.server.cfg.PeerAllow
	c.mu.Lock()
	for id, n := range remote {
		if id == c.local.ID {
			continue
		}
		if len(allow) > 0 && !slices.Contains(allow, n.Addr) {
			if !c.failed["allow:"+n.Addr] { // a peer's word is not enough to add a member: PEER_ALLOW says who may be one
				c.failed["allow:"+n.Addr] = true
				slog.Warn("cluster: ignoring a node not in PEER_ALLOW", "node", id, "addr", n.Addr)
			}
			continue
		}
		cur, known := c.nodes[id]
		if known && !n.LastSeen.After(cur.LastSeen) {
			continue
		}
		prev := c.gone[id]
		if known {
			prev = cur.Started
		}
		restarted := !prev.IsZero() && n.Started.After(prev)
		if time.Since(n.LastSeen) < downAfter && (restarted || !slices.Contains(replicating, n.Addr)) {
			attach = append(attach, attaching{n, restarted})
		}
		delete(c.gone, id)
		c.nodes[id] = &n
	}
	c.mu.Unlock()
	for _, a := range attach {
		if a.restarted {
			c.server.store.RemoveReplica(a.node.Addr)
		}
		c.attach(a.node, a.restarted)
	}
}

// attach starts replicating to a peer — through its node, which applies our
// writes to its own store — and, in the background, copies what it has
// missed. With overwrite our copy of every key wins there: the peer is a
// fresh incarnation that missed whatever we wrote while it was away. Without
// it the peer keeps what it holds and only has its gaps filled — it may be
// the survivor and we the one that just started.
func (c *cluster) attach(n storage.NodeInfo, overwrite bool) {
	if n.StoreAddr == c.local.StoreAddr && !private(c.local.StoreAddr) {
		return // sharing our store; replicating to it would be a self-write
	}
	c.attaching.Lock()
	defer c.attaching.Unlock()
	if !overwrite && slices.Contains(c.server.store.Replicas(), n.Addr) {
		return // a gossip and a join learned of it at once: one attach, one resync
	}
	if err := c.server.store.AddReplica(n.Addr); err != nil {
		slog.Warn("cluster: attach replica failed", "peer", n.ID, "err", err)
		return
	}
	slog.Info("cluster: peer attached", "peer", n.ID, "store", n.StoreAddr)
	c.events.emit("attach", n.Addr, 0, 0)
	go func() {
		start := time.Now()
		copied, err := c.server.store.Sync(n.Addr, overwrite)
		if err != nil {
			slog.Warn("cluster: resync incomplete", "peer", n.ID, "keys", copied, "err", err)
			return
		}
		took := time.Since(start)
		slog.Info("cluster: resynced", "peer", n.ID, "keys", copied, "overwrite", overwrite, "took", took.Round(time.Millisecond))
		c.events.emit("resync", n.Addr, copied, took)
	}()
}

// private reports whether a store address can belong to this node alone —
// embedded, or bound to loopback — so equal addresses on two nodes are two
// stores, not one shared.
func private(addr string) bool {
	if addr == config.EmbeddedStore {
		return true
	}
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
		c.events.emit("detach", n.Addr, 0, 0)
	}
}

// checkHealth ages every peer by when it last spoke. It also notices when
// this node was the one away — stopped, asleep, or starved past the point
// where peers write a node off: whatever they wrote meanwhile is newer than
// our copy, so we come back as a fresh incarnation (peers re-attach and
// overwrite us) and meet every peer anew, filling only its gaps.
func (c *cluster) checkHealth() {
	var detach []storage.NodeInfo
	c.mu.Lock()
	now := time.Now()
	if gap := now.Sub(c.tick); gap > downAfter {
		slog.Warn("cluster: stalled, rejoining as a new incarnation", "for", gap.Round(time.Second))
		c.events.emit("stall", "", 0, gap)
		c.local.Started = now
		for id, n := range c.nodes {
			if id != c.local.ID {
				detach = append(detach, *n)
				delete(c.nodes, id)
			}
		}
	}
	c.tick = now
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
		if n.State == storage.NodeStateDegraded && was == storage.NodeStateHealthy {
			slog.Info("cluster: peer degraded", "peer", id, "silent_for", age.Round(time.Second)) // the first word of trouble; a detach follows only if it stays quiet
		}
		if n.State == storage.NodeStateDown && was != storage.NodeStateDown {
			detach = append(detach, *n)
		}
		if age > evictAfter {
			c.gone[id] = n.Started
			delete(c.nodes, id)
			slog.Info("cluster: peer evicted", "peer", id, "silent_for", age.Round(time.Second))
			c.events.emit("evict", n.Addr, 0, age)
		}
	}
	c.mu.Unlock()
	for _, n := range detach {
		c.detach(n)
	}
}

func (c *cluster) touchLocal() {
	stats := c.server.Stats() // asks the store: never under the view lock
	c.mu.Lock()
	defer c.mu.Unlock()
	c.local.Stats = stats
	c.local.LastSeen = time.Now()
}

// localCopy is this node as peers should see it: a node answering right now
// was last seen right now, whatever its clocks have been through.
func (c *cluster) localCopy() storage.NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := *c.local
	n.LastSeen = time.Now()
	return n
}

// addr is this node's advertised address; set once, so no lock.
func (c *cluster) addr() string { return c.local.Addr }

// weightOf is the electronegativity a peer gossiped, 1 for one that never said.
func (c *cluster) weightOf(addr string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.nodes {
		if n.Addr == addr {
			return n.Weight()
		}
	}
	return 1
}

func (c *cluster) localJSON() string {
	b, _ := json.Marshal(c.localCopy())
	return string(b)
}

// dialable drops the peers served over connections they opened: they gossip
// to us on their own schedule, and their parked connections are for the
// fan-out, not for a view exchange that would close one per round.
func (c *cluster) dialable(peers []storage.NodeInfo) []storage.NodeInfo {
	out := peers[:0]
	for _, p := range peers {
		if !c.server.links.has(p.Addr) {
			out = append(out, p)
		}
	}
	return out
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

func (s *session) nodes(args []string) []byte {
	return viewJSON(s.srv.cluster.snapshot())
}

func (s *session) gossip(args []string) []byte {
	var n storage.NodeInfo
	if err := json.Unmarshal([]byte(args[0]), &n); err != nil || n.ID == "" {
		return resp.AppendError(nil, "ERR invalid node info")
	}
	if r := s.certNames(n.Addr); r != nil {
		return r
	}
	s.srv.cluster.learn(n)
	return viewJSON(s.srv.cluster.snapshot())
}

func viewJSON(view map[string]storage.NodeInfo) []byte {
	b, err := json.Marshal(view)
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendBulk(nil, b)
}

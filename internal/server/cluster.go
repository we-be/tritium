package server

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type NodeState string

const (
	NodeStateHealthy  NodeState = "healthy"
	NodeStateDegraded NodeState = "degraded"
	NodeStateDown     NodeState = "down"
)

type NodeInfo struct {
	ID       string      `json:"id"`
	RPCAddr  string      `json:"rpc_addr"`
	RespAddr string      `json:"resp_addr"`
	State    NodeState   `json:"state"`
	LastSeen time.Time   `json:"last_seen"`
	IsLeader bool        `json:"is_leader"`
	Stats    ServerStats `json:"stats"`
}

type ClusterInfo struct {
	mu           sync.RWMutex
	nodes        map[string]*NodeInfo
	localNode    *NodeInfo
	server       *Server
	healthTicker *time.Ticker
	stopCh       chan struct{}
}

// initCluster sets up the local node and starts the health-check and gossip routines.
func (s *Server) initCluster(rpcAddr, respAddr string) error {
	nodeID := fmt.Sprintf("node-%s", rpcAddr)

	s.cluster = &ClusterInfo{
		nodes: make(map[string]*NodeInfo),
		localNode: &NodeInfo{
			ID:       nodeID,
			RPCAddr:  rpcAddr, // will be updated in Start()
			RespAddr: respAddr,
			State:    NodeStateHealthy,
			LastSeen: time.Now(),
			Stats:    ServerStats{},
		},
		server:       s,
		healthTicker: time.NewTicker(5 * time.Second),
		stopCh:       make(chan struct{}),
	}

	// Register the local node.
	s.cluster.mu.Lock()
	s.cluster.nodes[nodeID] = s.cluster.localNode
	s.cluster.mu.Unlock()
	fmt.Printf("[init] Local node registered: %+v\n", s.cluster.localNode)

	// Start health-check and gossip routines.
	go s.cluster.healthCheckLoop()
	s.cluster.StartGossipLoop()
	fmt.Println("[init] Gossip loop started.")
	return nil
}

// JoinCluster connects to a known node, merges its cluster view, and announces the local node.
func (s *Server) JoinCluster(knownAddr string) error {
	fmt.Printf("[join] Dialing known node at %s...\n", knownAddr)
	client, err := rpc.Dial("tcp", knownAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to cluster at %s: %w", knownAddr, err)
	}
	defer client.Close()

	var nodes map[string]*NodeInfo
	if err := client.Call("Store.GetClusterNodes", struct{}{}, &nodes); err != nil {
		return fmt.Errorf("failed to get cluster nodes: %w", err)
	}
	fmt.Printf("[join] Got cluster nodes from known node: %+v\n", nodes)

	// Merge the remote nodes into our cluster.
	s.cluster.mu.Lock()
	for id, node := range nodes {
		if id != s.cluster.localNode.ID {
			s.cluster.nodes[id] = node
			fmt.Printf("[join] Added peer: %+v\n", node)
			// Optionally add node's RESP replica here.
			if err := s.addNodeAsReplica(node); err != nil {
				fmt.Printf("[warning] failed to add replica for node %s: %v\n", node.RPCAddr, err)
			}
		}
	}
	s.cluster.mu.Unlock()

	// Announce our presence to peers and refresh our cluster view.
	if err := s.cluster.announceToCluster(); err != nil {
		fmt.Printf("[join] announceToCluster error: %v\n", err)
	}
	return nil
}

// announceToCluster sends our local node info to all known peers.
func (ci *ClusterInfo) announceToCluster() error {
	ci.mu.RLock()
	nodesCopy := make(map[string]*NodeInfo)
	for k, v := range ci.nodes {
		nodesCopy[k] = v
	}
	ci.mu.RUnlock()

	fmt.Printf("[announce] Announcing self %+v to peers...\n", ci.localNode)
	for _, node := range nodesCopy {
		if node.ID == ci.localNode.ID {
			continue
		}

		client, err := rpc.Dial("tcp", node.RPCAddr)
		if err != nil {
			fmt.Printf("[announce] Failed to dial node %s: %v\n", node.RPCAddr, err)
			continue
		}

		var reply struct{}
		if err := client.Call("Store.RegisterNode", ci.localNode, &reply); err != nil {
			fmt.Printf("[announce] Failed to register with node %s: %v\n", node.RPCAddr, err)
			client.Close()
			continue
		}
		client.Close()
		fmt.Printf("[announce] Successfully announced to node %s\n", node.RPCAddr)
	}

	// Refresh cluster view from a peer.
	if err := ci.RefreshCluster(); err != nil {
		fmt.Printf("[announce] RefreshCluster failed: %v\n", err)
	}
	return nil
}

// RefreshCluster dials a peer and updates our cluster view.
func (ci *ClusterInfo) RefreshCluster() error {
	ci.mu.RLock()
	var peer *NodeInfo
	for _, node := range ci.nodes {
		if node.ID != ci.localNode.ID {
			peer = node
			break
		}
	}
	ci.mu.RUnlock()

	if peer == nil {
		return fmt.Errorf("no peer available to refresh")
	}

	client, err := rpc.Dial("tcp", peer.RPCAddr)
	if err != nil {
		return fmt.Errorf("failed to dial peer %s: %v", peer.RPCAddr, err)
	}
	defer client.Close()

	var remoteNodes map[string]*NodeInfo
	if err := client.Call("Store.GetClusterNodes", struct{}{}, &remoteNodes); err != nil {
		return fmt.Errorf("failed to get cluster nodes from peer: %v", err)
	}
	fmt.Printf("[refresh] Got remote cluster view from %s: %+v\n", peer.RPCAddr, remoteNodes)

	ci.mu.Lock()
	for id, remoteNode := range remoteNodes {
		if id != ci.localNode.ID {
			ci.nodes[id] = remoteNode
		}
	}
	ci.mu.Unlock()
	return nil
}

// RegisterNode is called via RPC when a new node joins.
func (s *Server) RegisterNode(node *NodeInfo, reply *struct{}) error {
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()

	node.LastSeen = time.Now()
	s.cluster.nodes[node.ID] = node
	fmt.Printf("[RegisterNode] Node registered: %+v\n", node)

	// Optionally add node's RESP replica.
	if err := s.addNodeAsReplica(node); err != nil {
		return fmt.Errorf("failed to add replica: %w", err)
	}
	return nil
}

// GetClusterNodes returns the current cluster view.
func (s *Server) GetClusterNodes(args struct{}, reply *map[string]*NodeInfo) error {
	s.cluster.mu.RLock()
	defer s.cluster.mu.RUnlock()

	*reply = make(map[string]*NodeInfo)
	for k, v := range s.cluster.nodes {
		(*reply)[k] = v
	}
	fmt.Printf("[GetClusterNodes] Returning cluster view: %+v\n", *reply)
	return nil
}

// GossipExchange is the RPC method used during gossip.
// It returns a copy of our cluster view.
func (s *Server) GossipExchange(peerNode *NodeInfo, reply *map[string]*NodeInfo) error {
	s.cluster.mu.RLock()
	defer s.cluster.mu.RUnlock()

	gossipData := make(map[string]*NodeInfo)
	for id, node := range s.cluster.nodes {
		gossipData[id] = node
	}
	*reply = gossipData
	fmt.Printf("[GossipExchange] Returning cluster view: %+v\n", gossipData)
	return nil
}

// StartGossipLoop starts a ticker that periodically performs gossip.
func (ci *ClusterInfo) StartGossipLoop() {
	ticker := time.NewTicker(5 * time.Second) // adjust interval as needed
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ci.stopCh:
				return
			case <-ticker.C:
				ci.gossip()
			}
		}
	}()
}

// gossip picks a random peer and exchanges cluster state.
func (ci *ClusterInfo) gossip() {
	ci.mu.RLock()
	var peers []*NodeInfo
	for _, node := range ci.nodes {
		if node.ID != ci.localNode.ID {
			peers = append(peers, node)
		}
	}
	ci.mu.RUnlock()

	if len(peers) == 0 {
		fmt.Println("[gossip] No peers available.")
		return
	}

	// Pick a random peer.
	rand.Seed(time.Now().UnixNano())
	peer := peers[rand.Intn(len(peers))]
	fmt.Printf("[gossip] Selected peer %s for gossip.\n", peer.RPCAddr)

	client, err := rpc.Dial("tcp", peer.RPCAddr)
	if err != nil {
		fmt.Printf("[gossip] Failed to dial peer %s: %v\n", peer.RPCAddr, err)
		return
	}
	defer client.Close()

	var remoteNodes map[string]*NodeInfo
	if err := client.Call("Store.GossipExchange", ci.localNode, &remoteNodes); err != nil {
		fmt.Printf("[gossip] Exchange with %s failed: %v\n", peer.RPCAddr, err)
		return
	}
	fmt.Printf("[gossip] Received remote nodes: %+v\n", remoteNodes)

	// Merge remote view into our local view.
	ci.mu.Lock()
	for id, remoteNode := range remoteNodes {
		if localNode, exists := ci.nodes[id]; !exists || remoteNode.LastSeen.After(localNode.LastSeen) {
			ci.nodes[id] = remoteNode
			fmt.Printf("[gossip] Updated node %s: %+v\n", id, remoteNode)
		}
	}
	ci.mu.Unlock()
}

// updateNodeStats updates our local node's stats and timestamp.
func (ci *ClusterInfo) updateNodeStats(stats ServerStats) {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	ci.localNode.Stats = stats
	ci.localNode.LastSeen = time.Now()
}

// healthCheckLoop periodically checks node health and updates our local node's stats.
func (ci *ClusterInfo) healthCheckLoop() {
	for {
		select {
		case <-ci.stopCh:
			return
		case <-ci.healthTicker.C:
			ci.checkNodesHealth()
			ci.updateNodeStats(ci.server.Stats())
		}
	}
}

// checkNodesHealth marks nodes as degraded or down based on their LastSeen.
func (ci *ClusterInfo) checkNodesHealth() {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	now := time.Now()
	for id, node := range ci.nodes {
		// Skip the local node
		if id == ci.localNode.ID {
			continue
		}

		oldState := node.State

		// Mark down if not seen in 15s
		if now.Sub(node.LastSeen) > 15*time.Second {
			node.State = NodeStateDown
		} else if now.Sub(node.LastSeen) > 10*time.Second {
			node.State = NodeStateDegraded
		} else {
			node.State = NodeStateHealthy
		}

		// If newly marked down, remove replica from local store
		if oldState != NodeStateDown && node.State == NodeStateDown {
			if err := ci.server.removeNodeReplica(node); err != nil {
				fmt.Printf("[health] Failed to remove replica for node %s: %v\n", node.RPCAddr, err)
			}
		}

		// After being down for e.g. 60s, remove from cluster membership
		if node.State == NodeStateDown && now.Sub(node.LastSeen) > 60*time.Second {
			fmt.Printf("[health] Node %s removed from cluster after extended downtime\n", node.RPCAddr)
			delete(ci.nodes, id)
		}
	}
}

// stopCluster stops both the health-check and gossip routines.
func (ci *ClusterInfo) stopCluster() {
	close(ci.stopCh)
	ci.healthTicker.Stop()
}

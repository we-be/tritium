// Package storage holds the cluster-view types shared by nodes and clients,
// and the RESP-backed replicated Store a node writes through.
package storage

import "time"

type NodeState string

const (
	NodeStateHealthy  NodeState = "healthy"
	NodeStateDegraded NodeState = "degraded"
	NodeStateDown     NodeState = "down"
)

type NodeStats struct {
	ActiveConnections int64 `json:"active_connections"`
	BytesTransferred  int64 `json:"bytes_transferred"`
	Replicas          int   `json:"replicas"`      // peers this node fans writes out to
	Held              int   `json:"held_replicas"` // of those, ones that stopped answering and await a repair
}

// NodeInfo is one node's entry in the cluster view, exchanged as JSON over
// the TRITIUM.NODES and TRITIUM.GOSSIP commands. IsLeader only marks the
// node that seeded the cluster; there is no election.
type NodeInfo struct {
	ID        string    `json:"id"`
	Addr      string    `json:"addr"`       // where clients and peers reach this node
	StoreAddr string    `json:"store_addr"` // the RESP store it writes through
	State     NodeState `json:"state"`
	LastSeen  time.Time `json:"last_seen"`
	IsLeader  bool      `json:"is_leader"`
	Started   time.Time `json:"started,omitzero"`  // this incarnation's start: a peer that restarted has a new one
	Version   string    `json:"version,omitempty"` // the tritium build it runs, so a fleet upgrade can be watched
	Seeds     []string  `json:"seeds,omitempty"`   // the peers it dials to join and rejoin
	Stats     NodeStats `json:"stats"`
}

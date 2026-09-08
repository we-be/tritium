// Package storage holds the cluster-view types shared by nodes and
// clients: a node's record in the view, its state and stats, the fleet event
// log's entry and key prefix, and the error a missing key reads as.
package storage

import (
	"errors"
	"time"
)

// ErrNotFound is what a read of a key that is missing or expired reports.
var ErrNotFound = errors.New("key not found")

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
// node that seeded the cluster and has a weight; there is no election.
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
	// Electronegativity is the node's pull on key ownership, as its
	// ELECTRONEGATIVITY says: absent is 1; 0 never owns a key.
	Electronegativity *int `json:"electronegativity,omitempty"`
}

// Weight is the node's electronegativity: its share of key ownership
// relative to its peers. A node that never said has weight 1.
func (n NodeInfo) Weight() int {
	if n.Electronegativity == nil {
		return 1
	}
	return *n.Electronegativity
}

// EventsKeyPrefix plus a node's ID names the sorted set holding that node's
// own cluster event log, replicated like any other key.
const EventsKeyPrefix = "tritium:events:"

// Event is one line of a node's cluster event log — attach, detach, hold,
// repair, stall, evict, resync or start — stored as a member of the sorted
// set at EventsKeyPrefix+Node, scored by At in unix milliseconds. Fields not
// meaningful for a given Event are left zero and omitted from the JSON.
type Event struct {
	At    int64  `json:"at"`
	Node  string `json:"node"`           // the node whose log this is
	Event string `json:"event"`          // attach, detach, hold, repair, stall, evict, resync, start
	Peer  string `json:"peer,omitempty"` // the peer address involved, when there is one
	Keys  int    `json:"keys,omitempty"` // keys copied or replayed, for resync and repair
	Took  int64  `json:"took,omitempty"` // milliseconds: how long the operation took, or the outage it closed
}

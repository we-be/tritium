// Package storage holds the RPC wire types shared by tritium servers and
// clients, and the RESP-backed replicated Store a server writes through.
//
// The server registers itself under the RPC service name "Store", so clients
// call "Store.Set", "Store.Get", "Store.Delete" and "Store.GetClusterNodes".
package storage

import "time"

type SetArgs struct {
	Key   string
	Value []byte
	TTL   *int // seconds; nil means the server default
}

type SetReply struct {
	Error string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value []byte
	Error string
}

type DeleteArgs struct {
	Key string
}

type DeleteReply struct {
	Deleted bool
	Error   string
}

type NodeState string

const (
	NodeStateHealthy  NodeState = "healthy"
	NodeStateDegraded NodeState = "degraded"
	NodeStateDown     NodeState = "down"
)

type NodeStats struct {
	ActiveConnections int64 `json:"active_connections"`
	BytesTransferred  int64 `json:"bytes_transferred"`
}

// NodeInfo is one node's entry in the cluster view. IsLeader only marks the
// node that seeded the cluster; there is no election.
type NodeInfo struct {
	ID       string    `json:"id"`
	RPCAddr  string    `json:"rpc_addr"`
	RespAddr string    `json:"resp_addr"`
	State    NodeState `json:"state"`
	LastSeen time.Time `json:"last_seen"`
	IsLeader bool      `json:"is_leader"`
	Stats    NodeStats `json:"stats"`
}

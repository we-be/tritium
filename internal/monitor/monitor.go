// Package monitor gathers a live picture of a tritium cluster: the node view
// from any reachable node, plus replication state from every node's RESP
// store and the replicas that store reports.
package monitor

import (
	"errors"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
	"github.com/we-be/tritium/pkg/tritium"
)

const dialTimeout = time.Second

type Monitor struct {
	addrs []string
	opts  tritium.ClientOptions // Password and TLS are used; Address and Timeout are set per node
}

// New polls the given node addresses in order until one answers.
func New(addrs []string, opts tritium.ClientOptions) *Monitor {
	return &Monitor{addrs: addrs, opts: opts}
}

// Snapshot is one refresh of the cluster.
type Snapshot struct {
	Nodes  []storage.NodeInfo // sorted by address
	Stores map[string]Store   // keyed by the node's RESP address
	Err    error              // set when no node answered
}

// Store is a RESP server's "INFO replication" view and, for a primary, the
// replicas it reports.
type Store struct {
	Addr     string
	Info     map[string]string // nil when unreachable
	Replicas []Store
}

func (s Store) Role() string { return s.Info["role"] }

// Healthy is true for a reachable primary, or a replica whose link is up.
func (s Store) Healthy() bool {
	if s.Info == nil {
		return false
	}
	return s.Role() != "slave" || s.Info["master_link_status"] == "up"
}

func (m *Monitor) Snapshot() Snapshot {
	nodes, err := m.nodes()
	snap := Snapshot{Err: err, Stores: map[string]Store{}}
	for _, n := range nodes {
		snap.Nodes = append(snap.Nodes, n)
		if _, seen := snap.Stores[n.StoreAddr]; !seen {
			snap.Stores[n.StoreAddr] = inspectStore(n.StoreAddr)
		}
	}
	slices.SortFunc(snap.Nodes, func(a, b storage.NodeInfo) int { return strings.Compare(a.Addr, b.Addr) })
	return snap
}

func (m *Monitor) nodes() (map[string]storage.NodeInfo, error) {
	for _, addr := range m.addrs {
		opts := m.opts
		opts.Address, opts.Timeout = addr, dialTimeout
		c, err := tritium.NewClient(&opts)
		if err != nil {
			continue
		}
		nodes, err := c.Nodes()
		c.Close()
		if err == nil {
			return nodes, nil
		}
	}
	return nil, errors.New("no node answered")
}

func inspectStore(addr string) Store {
	s := Store{Addr: addr, Info: replicationInfo(addr)}
	for _, r := range replicaAddrs(s.Info) {
		s.Replicas = append(s.Replicas, Store{Addr: r, Info: replicationInfo(r)})
	}
	return s
}

// replicationInfo parses "INFO replication" into its key:value fields.
func replicationInfo(addr string) map[string]string {
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(dialTimeout))
	v, err := resp.NewCommand("INFO", "replication").Do(conn, resp.NewReader(conn))
	if err != nil {
		return nil
	}
	raw, _ := v.([]byte)
	info := map[string]string{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if k, val, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			info[k] = val
		}
	}
	return info
}

// replicaAddrs reads a primary's "slaveN:ip=...,port=...,state=..." fields.
func replicaAddrs(info map[string]string) []string {
	var addrs []string
	for k, v := range info {
		if !strings.HasPrefix(k, "slave") {
			continue
		}
		var ip, port string
		for field := range strings.SplitSeq(v, ",") {
			if name, val, ok := strings.Cut(field, "="); ok {
				switch name {
				case "ip":
					ip = val
				case "port":
					port = val
				}
			}
		}
		if ip != "" && port != "" {
			addrs = append(addrs, net.JoinHostPort(ip, port))
		}
	}
	slices.Sort(addrs)
	return addrs
}

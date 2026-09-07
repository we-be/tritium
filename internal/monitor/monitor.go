// Package monitor gathers a live picture of a tritium cluster: the node view
// from any reachable node, plus each node's own store as that node reports
// it — stores bind to loopback, so nothing else can reach them.
package monitor

import (
	"errors"
	"slices"
	"strings"
	"time"

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

// eventsWindow is how far back the panel looks.
const eventsWindow = time.Hour

// Snapshot is one refresh of the cluster.
type Snapshot struct {
	Nodes  []storage.NodeInfo // sorted by address
	Stores map[string]Store   // keyed by node ID
	Events []storage.Event    // the fleet's merged recent log, oldest first
	Err    error              // set when no node answered
}

// Store is one node's store as its INFO reports it: the store_* fields, or
// nil when the node itself did not answer.
type Store struct {
	Addr string
	Info map[string]string
}

// Healthy is true when the node answered and its store did too.
func (s Store) Healthy() bool { return s.Info != nil && s.Info["store_status"] == "ok" }

func (m *Monitor) Snapshot() Snapshot {
	nodes, err := m.nodes()
	snap := Snapshot{Err: err, Stores: map[string]Store{}}
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		snap.Nodes = append(snap.Nodes, n)
		snap.Stores[n.ID] = Store{Addr: n.StoreAddr, Info: m.info(n.Addr)}
		ids = append(ids, n.ID)
	}
	slices.SortFunc(snap.Nodes, func(a, b storage.NodeInfo) int { return strings.Compare(a.Addr, b.Addr) })
	if err == nil {
		snap.Events = m.events(ids)
	}
	return snap
}

// events reads the fleet's merged log from whichever node answers first:
// every node's writes replicate everywhere, so one read covers them all.
func (m *Monitor) events(ids []string) []storage.Event {
	for _, addr := range m.addrs {
		c, err := m.dial(addr)
		if err != nil {
			continue
		}
		events, err := c.Events(ids, eventsWindow)
		c.Close()
		if err == nil {
			return events
		}
	}
	return nil
}

func (m *Monitor) nodes() (map[string]storage.NodeInfo, error) {
	for _, addr := range m.addrs {
		c, err := m.dial(addr)
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

func (m *Monitor) dial(addr string) (*tritium.Client, error) {
	opts := m.opts
	opts.Address, opts.Timeout = addr, dialTimeout
	return tritium.NewClient(&opts)
}

// info asks one node for its store section and parses the key:value lines.
func (m *Monitor) info(addr string) map[string]string {
	c, err := m.dial(addr)
	if err != nil {
		return nil
	}
	defer c.Close()
	v, err := c.Do("INFO", "store")
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

package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// A Prometheus scrape endpoint, so a Grafana stack can read what INFO says
// without a RESP client. The text format is the whole protocol, so it costs
// no dependency; it carries no authentication either, which is what every
// scraper expects, so METRICS_ADDRESS belongs on loopback or a private
// interface and never on the address clients reach.

// metricsContentType names the text exposition format version scrapers parse.
const metricsContentType = "text/plain; version=0.0.4"

// startMetrics serves METRICS_ADDRESS, if it is set. Serve calls it last:
// a scrape reads the cluster view, so the view has to exist first.
func (s *Server) startMetrics() error {
	if s.cfg.MetricsAddr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", s.cfg.MetricsAddr)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	s.metricsLn = ln
	s.metrics = &http.Server{Handler: http.HandlerFunc(s.handleMetrics), ReadHeaderTimeout: 5 * time.Second}
	s.connWG.Go(func() { // counted like the accept loop, so Stop outlives no goroutine of ours
		if err := s.metrics.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics: serve failed", "err", err)
		}
	})
	slog.Info("metrics listening", "addr", ln.Addr())
	return nil
}

// MetricsAddr is the bound scrape address, or "" when the node serves none.
func (s *Server) MetricsAddr() string {
	if s.metricsLn == nil {
		return ""
	}
	return s.metricsLn.Addr().String()
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", metricsContentType)
	w.Write([]byte(s.metricsText()))
}

// metricsText renders one scrape from the sources INFO reads, so a
// dashboard and a `tritium-cli info` can never disagree.
func (s *Server) metricsText() string {
	local := s.cluster.localCopy()
	stats := s.Stats()
	m := &scrape{}

	m.head("tritium_node_info", "gauge", "The node's identity and build, always 1.")
	fmt.Fprintf(m, "tritium_node_info{version=%s,node=%s,addr=%s} 1\n", quote(Version), quote(local.ID), quote(local.Addr))
	m.gauge("tritium_electronegativity", "This node's pull on key ownership; 0 never owns a key.", int64(s.cfg.Weight()))
	m.gauge("tritium_cluster_nodes", "Nodes in this node's cluster view, itself included.", int64(len(s.Nodes())))
	m.gauge("tritium_replicas", "Peers this node fans writes out to.", int64(stats.Replicas))
	m.gauge("tritium_replicas_held", "Replicas that stopped answering and await a repair.", int64(stats.Held))
	m.gauge("tritium_replicas_queued", "Replicas fed from a queue instead of waited on.", int64(stats.Queued))
	m.counter("tritium_writes_total", "Writes this node has carried out as owner since it started.", stats.Writes)
	m.counter("tritium_forwarded_total", "Writes carried to the node that owns the key.", s.forwarded.Load())
	m.counter("tritium_forward_fallbacks_total", "Writes done here because the owner was out of reach.", s.fallbacks.Load())
	m.gauge("tritium_connections_active", "Connections being served, clients and peers alike.", stats.ActiveConnections)
	m.counter("tritium_bytes_transferred_total", "Bytes this node has read from and written to connections.", stats.BytesTransferred)
	m.gauge("tritium_events_kept", "Entries in this node's own cluster event log.", s.eventsKept(local.ID))

	if fields, err := s.storeFields(); err == nil {
		m.gauge("tritium_store_keys", "Keys the node's store holds.", storeKeys(fields))
		m.gauge("tritium_store_used_memory_bytes", "Bytes the node's store uses.", number(fields["used_memory"]))
		m.gauge("tritium_store_maxmemory_bytes", "Bytes the store keeps before it evicts; 0 is no limit.", number(fields["maxmemory"]))
		if up, ok := fields["uptime_in_seconds"]; ok {
			m.gauge("tritium_store_uptime_seconds", "Seconds since the node's store started.", number(up))
		}
	}

	m.head("tritium_peer_state", "gauge", "Each peer's state in this node's view, always 1.")
	for _, p := range s.cluster.peers() {
		fmt.Fprintf(m, "tritium_peer_state{peer=%s,state=%s} 1\n", quote(p.Addr), quote(string(p.State)))
	}
	return m.String()
}

// scrape accumulates one response in the text format: every metric is
// preceded by its HELP and TYPE, which is what makes a scrape self-describing.
type scrape struct{ strings.Builder }

func (m *scrape) head(name, kind, help string) {
	fmt.Fprintf(m, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

func (m *scrape) gauge(name, help string, v int64) {
	m.head(name, "gauge", help)
	fmt.Fprintf(m, "%s %d\n", name, v)
}

func (m *scrape) counter(name, help string, v int64) {
	m.head(name, "counter", help)
	fmt.Fprintf(m, "%s %d\n", name, v)
}

// quote renders a label value; addresses and versions come from config and
// gossip, so they are escaped rather than trusted to be plain.
func quote(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return `"` + r.Replace(v) + `"`
}

// number reads a figure out of the store's INFO; a field it does not report
// is 0.
func number(raw string) int64 {
	n, _ := strconv.ParseInt(raw, 10, 64)
	return n
}

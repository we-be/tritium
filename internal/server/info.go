package server

import (
	"cmp"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/we-be/tritium/internal/resp"
)

// INFO: what a node says about itself — server, clients, replication and
// tritium sections, with the embedded store's own figures proxied in.

// info renders INFO [section ...] in the usual "# Section" layout.
func (s *session) info(args []string) []byte {
	local := s.srv.cluster.localCopy()
	stats := s.srv.Stats()
	_, port, _ := net.SplitHostPort(s.srv.Addr())
	sections := []struct{ name, body string }{
		{"server", fmt.Sprintf("server_name:tritium\r\ntritium_version:%s\r\ntcp_port:%s\r\n", Version, port)},
		{"clients", fmt.Sprintf("connected_clients:%d\r\n", stats.ActiveConnections)},
		{"stats", fmt.Sprintf("bytes_transferred:%d\r\n", stats.BytesTransferred)},
		{"replication", "role:master\r\n"},
		{"tritium", fmt.Sprintf("node_id:%s\r\nnode_addr:%s\r\nversion:%s\r\nseeds:%s\r\nstore:%s\r\nstore_tls:%s\r\ncluster_nodes:%d\r\nreplicas:%d\r\nheld_replicas:%d\r\nqueued_replicas:%d\r\nreplication:%s\r\nkey_ownership:%s\r\nelectronegativity:%d\r\nforwarded:%d\r\nforward_fallbacks:%d\r\nwrites:%d\r\nstamps:%s\r\nevents:%d\r\n",
			local.ID, local.Addr, Version, strings.Join(local.Seeds, ","), local.StoreAddr, onOff(s.srv.cfg.StoreTLS), len(s.srv.Nodes()), stats.Replicas, stats.Held, len(s.srv.store.Queued()), replicationMode(s.srv.store.Async()), onOff(s.srv.cfg.Ownership), s.srv.cfg.Weight(), s.srv.forwarded.Load(), s.srv.fallbacks.Load(), stats.Writes, stampsMode(s.srv.store.Stamps()), s.srv.eventsKept(local.ID))},
		{"store", s.srv.storeInfo()},
	}

	all := len(args) == 0
	for _, a := range args {
		switch strings.ToLower(a) {
		case "all", "default", "everything":
			all = true
		}
	}
	var sb strings.Builder
	for _, sec := range sections {
		if !all && !containsFold(args, sec.name) {
			continue
		}
		if s.user != nil && (sec.name == "tritium" || sec.name == "store") {
			continue // a user gets the node's health, not its address, its seeds, or its store
		}
		sb.WriteString("# " + strings.ToUpper(sec.name[:1]) + sec.name[1:] + "\r\n" + sec.body + "\r\n")
	}
	return resp.AppendBulkString(nil, sb.String())
}

// storeInfo is the primary store as seen through this node — the only way
// to see it once stores bind to loopback — as store_* fields: whether it
// answers, and what its own INFO says about version, uptime, memory and keys.
func (s *Server) storeInfo() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "store_addr:%s\r\n", s.cfg.StoreLabel())
	fields, err := s.storeFields()
	if err != nil {
		fmt.Fprintf(&sb, "store_status:unreachable\r\nstore_error:%s\r\n", strings.ReplaceAll(err.Error(), "\n", " "))
		return sb.String()
	}
	sb.WriteString("store_status:ok\r\n")
	if ver := cmp.Or(fields["tritium_version"], fields["valkey_version"], fields["redis_version"]); ver != "" {
		fmt.Fprintf(&sb, "store_version:%s\r\n", ver)
	}
	for _, f := range []string{"uptime_in_seconds", "used_memory", "maxmemory", "maxmemory_policy"} {
		if val, ok := fields[f]; ok {
			fmt.Fprintf(&sb, "store_%s:%s\r\n", f, val)
		}
	}
	if _, ok := fields["db0"]; ok {
		fmt.Fprintf(&sb, "store_keys:%d\r\n", storeKeys(fields))
	}
	return sb.String()
}

// storeFields is the store's INFO — server, memory and keyspace — as a map.
func (s *Server) storeFields() (map[string]string, error) {
	v, err := s.store.Query("INFO", "server", "memory", "keyspace")
	if err != nil {
		return nil, err
	}
	raw, _ := v.([]byte)
	fields := map[string]string{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if k, val, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			fields[k] = val
		}
	}
	return fields, nil
}

// storeKeys reads the key count out of INFO's db0 line: keys=N,expires=N,avg_ttl=N.
func storeKeys(fields map[string]string) int64 {
	_, n, ok := strings.Cut(fields["db0"], "keys=")
	if !ok {
		return 0
	}
	n, _, _ = strings.Cut(n, ",")
	keys, _ := strconv.ParseInt(n, 10, 64)
	return keys
}

// stampsMode is how INFO reports write stamps: kept by this node's own
// store, or minted for peers only because an external store keeps none.
func stampsMode(on, primary bool) string {
	switch {
	case on && primary:
		return "kept"
	case on:
		return "pass-through"
	}
	return "off"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func replicationMode(async bool) string {
	if async {
		return "async"
	}
	return "sync"
}

func containsFold(list []string, s string) bool {
	for _, l := range list {
		if strings.EqualFold(l, s) {
			return true
		}
	}
	return false
}

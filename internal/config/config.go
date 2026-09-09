// Package config loads a node's settings from a dotenv file and the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
)

const (
	DefaultListenAddr = "localhost:8080"
	DefaultPoolSize   = 4
	DefaultMaxClients = 10000
)

// EmbeddedStore is what a node reports as its store address when it runs
// its own: there is nothing to dial.
const EmbeddedStore = "embedded"

type Config struct {
	ListenAddr              string            // LISTEN_ADDRESS: where this node accepts clients and peers
	AdvertiseAddr           string            // ADVERTISE_ADDRESS: address peers dial us on; defaults to the bound address
	JoinAddr                string            // JOIN_ADDRESS: nodes to join, comma-separated, retried for as long as they are unreachable; empty seeds a new cluster
	LinkAddr                string            // LINK_ADDRESS: peers this node cannot be dialed by, comma-separated; it opens the connections and is served over them
	MetricsAddr             string            // METRICS_ADDRESS: where GET /metrics answers scrapes; unauthenticated, so loopback or a private interface. Empty serves none
	Password                string            // AUTH_PASSWORD: required from clients when set
	AuthUser                string            // AUTH_USER: the user a client next to this node authenticates as; empty is the default user
	PeerPassword            string            // PEER_PASSWORD: what nodes AUTH to each other with; required, distinct from AUTH_PASSWORD, once this node peers
	StoreAddr               string            // SECURE_STORE_ADDRESS: RESP server this node writes through; empty runs the node's own store in-process
	StorePassword           string            // SECURE_STORE_PASSWORD: AUTH for the store and every replica
	StoreTLS                bool              // SECURE_STORE_TLS: verify the store's certificate instead of dialing it in the clear
	StoreCA                 string            // SECURE_STORE_CA: PEM bundle the store's certificate must chain to; empty verifies against the system roots
	StoreServerName         string            // SECURE_STORE_SERVER_NAME: name to verify and send as SNI; defaults to the host part of SECURE_STORE_ADDRESS
	StoreMaxMemory          int64             // STORE_MAX_MEMORY: bytes the embedded store keeps before evicting the soonest-expiring keys; 0 is no limit
	PoolSize                int               // MAX_SERVER_CONNECTIONS: connections pooled per RESP server
	MaxClients              int               // MAX_CLIENTS: connections a node accepts at once; more are refused. 0: no limit
	PeerAllow               []string          // PEER_ALLOW: the only addresses this node will take as peers, comma-separated; empty takes what gossip says
	AllowNoAuth             bool              // ALLOW_NO_AUTH=true: run without AUTH_PASSWORD on a listener that is not loopback
	AllowSharedPeerPassword bool              // ALLOW_SHARED_PEER_PASSWORD=true: let a peering node start with PEER_PASSWORD unset or equal to AUTH_PASSWORD
	Async                   bool              // REPLICATION=async: answer once the primary has a write, feed peers from a queue; sync (default) waits for every peer
	Ownership               bool              // KEY_OWNERSHIP=on (default): each key's writes go through one owner node, so NX and order hold cluster-wide; off writes locally first
	Electronegativity       *int              // ELECTRONEGATIVITY: this node's pull on key ownership — rendezvous points per key; nil is 1, 0 never owns a key
	TLSCert                 string            // TLS_CERT: PEM certificate; with TLS_KEY, serves TLS and dials peers with it
	TLSKey                  string            // TLS_KEY: PEM private key
	TLSCA                   string            // TLS_CA: PEM bundle that peers, and clients under TLS_CLIENT_AUTH, must chain to
	TLSClientAuth           bool              // TLS_CLIENT_AUTH: require client certificates (mutual TLS)
	UsersFile               string            // USERS_FILE: a file of USER_<name> entries, one per line, with the bare name on the left
	Users                   map[string]User   // USER_<name>=<password>:<rights>: clients with only the key prefixes they name
	Surfaces                map[string]Rights // SURFACE_<name>=<rights>: a named rights set a credential holds as @<name>
}

// Local is how to reach the node a dotenv file configures from the same
// machine: its port on loopback and what it demands of a client. Tools take
// it through -config so the one file serves the node and its clients.
type Local struct {
	Addr, User, Password, StorePassword, CA string
	TLS                                     bool // the node serves TLS, whether or not TLS_CA names a private CA
}

func LoadLocal(path string) (Local, error) {
	cfg, err := Load(path)
	if err != nil {
		return Local{}, err
	}
	_, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return Local{}, fmt.Errorf("LISTEN_ADDRESS %q: %w", cfg.ListenAddr, err)
	}
	password := cfg.Password
	if u, ok := cfg.Users[cfg.AuthUser]; ok {
		password = u.Password
	}
	return Local{Addr: "127.0.0.1:" + port, User: cfg.AuthUser, Password: password, StorePassword: cfg.StorePassword, CA: cfg.TLSCA, TLS: cfg.TLSCert != ""}, nil
}

// warnIfShared says so when a file holding passwords can be read by other
// users of the machine; it goes on loading it, since the node is what keeps
// the plane up, but the line is in the log.
func warnIfShared(path string) {
	if fi, err := os.Stat(path); err == nil && fi.Mode().Perm()&0o077 != 0 {
		slog.Warn("config file is readable by other users; chmod 600 it", "path", path, "mode", fi.Mode().Perm())
	}
}

// StoreLabel names the store for logs and INFO: its address, or "embedded".
func (c Config) StoreLabel() string {
	if c.StoreAddr == "" {
		return EmbeddedStore
	}
	return c.StoreAddr
}

// ParseBytes reads a size such as 268435456, 256M, 1G, 512K or 64MB.
func ParseBytes(raw string) (int64, error) {
	s := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(raw)), "B")
	mult := int64(1)
	if n := len(s); n > 0 {
		switch s[n-1] {
		case 'K':
			mult, s = 1<<10, s[:n-1]
		case 'M':
			mult, s = 1<<20, s[:n-1]
		case 'G':
			mult, s = 1<<30, s[:n-1]
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a size", raw)
	}
	return n * mult, nil
}

// Seeds is every peer this node dials to join: JOIN_ADDRESS and LINK_ADDRESS
// alike. Each is dialed until it answers and again whenever it drops out of
// the view, so nodes can boot in any order.
func (c Config) Seeds() []string {
	out := list(c.JoinAddr)
	for _, l := range c.Links() {
		if !slices.Contains(out, l) {
			out = append(out, l)
		}
	}
	return out
}

// Weight is this node's electronegativity: its share of key ownership
// relative to its peers, 1 unless ELECTRONEGATIVITY says otherwise.
func (c Config) Weight() int {
	if c.Electronegativity == nil {
		return 1
	}
	return *c.Electronegativity
}

// Links is LINK_ADDRESS as a list: the peers that cannot dial us back, so we
// open the connections they serve us over (see TRITIUM.PEERLINK).
func (c Config) Links() []string { return list(c.LinkAddr) }

// Peering reports whether this node is configured to talk to other nodes at
// all: it dials some (Seeds) or tells them where to dial it (AdvertiseAddr).
// A node with none of these set is a lone node, whatever else is configured.
func (c Config) Peering() bool { return len(c.Seeds()) > 0 || c.AdvertiseAddr != "" }

func list(raw string) []string {
	var out []string
	for s := range strings.SplitSeq(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// env is what Load reads a setting through: the process environment, then
// the dotenv file, then a default.
type env map[string]string

func (e env) get(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	if v, ok := e[key]; ok {
		return v
	}
	return def
}

func (e env) boolean(key string) (bool, error) {
	raw := e.get(key, "false")
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s: %q is not a boolean", key, raw)
	}
	return v, nil
}

// integer reads a count no smaller than floor.
func (e env) integer(key string, def, floor int) (int, error) {
	raw := e.get(key, strconv.Itoa(def))
	n, err := strconv.Atoi(raw)
	if err != nil || n < floor {
		what := "a non-negative integer"
		if floor > 0 {
			what = "a positive integer"
		}
		return 0, fmt.Errorf("%s: %q is not %s", key, raw, what)
	}
	return n, nil
}

// Load reads path as a dotenv file (empty path: none), then lets process
// environment variables override it.
func Load(path string) (Config, error) {
	vals := map[string]string{}
	if path != "" {
		var err error
		if vals, err = ReadDotenv(path); err != nil {
			return Config{}, err
		}
		warnIfShared(path)
	}
	get := env(vals).get

	cfg := Config{
		ListenAddr:      get("LISTEN_ADDRESS", get("RPC_ADDRESS", DefaultListenAddr)), // RPC_ADDRESS: pre-RESP name
		AdvertiseAddr:   get("ADVERTISE_ADDRESS", ""),
		JoinAddr:        get("JOIN_ADDRESS", ""),
		LinkAddr:        get("LINK_ADDRESS", ""),
		MetricsAddr:     get("METRICS_ADDRESS", ""),
		PeerAllow:       list(get("PEER_ALLOW", "")),
		Password:        get("AUTH_PASSWORD", ""),
		AuthUser:        get("AUTH_USER", ""),
		PeerPassword:    get("PEER_PASSWORD", ""),
		StoreAddr:       get("SECURE_STORE_ADDRESS", ""),
		StorePassword:   get("SECURE_STORE_PASSWORD", ""),
		StoreCA:         get("SECURE_STORE_CA", ""),
		StoreServerName: get("SECURE_STORE_SERVER_NAME", ""),
		TLSCert:         get("TLS_CERT", ""),
		TLSKey:          get("TLS_KEY", ""),
		TLSCA:           get("TLS_CA", ""),
		UsersFile:       get(UsersFileVar, ""),
	}

	var err error
	if raw := get("STORE_MAX_MEMORY", ""); raw != "" {
		if cfg.StoreMaxMemory, err = ParseBytes(raw); err != nil {
			return Config{}, fmt.Errorf("STORE_MAX_MEMORY: %w", err)
		}
	}
	e := env(vals)
	if cfg.MaxClients, err = e.integer("MAX_CLIENTS", DefaultMaxClients, 0); err != nil {
		return Config{}, err
	}
	weight, err := e.integer("ELECTRONEGATIVITY", 1, 0)
	if err != nil {
		return Config{}, err
	}
	cfg.Electronegativity = &weight
	if cfg.PoolSize, err = e.integer("MAX_SERVER_CONNECTIONS", DefaultPoolSize, 1); err != nil {
		return Config{}, err
	}
	for _, b := range []struct {
		key string
		dst *bool
	}{
		{"TLS_CLIENT_AUTH", &cfg.TLSClientAuth},
		{"ALLOW_NO_AUTH", &cfg.AllowNoAuth},
		{"SECURE_STORE_TLS", &cfg.StoreTLS},
		{"ALLOW_SHARED_PEER_PASSWORD", &cfg.AllowSharedPeerPassword},
	} {
		if *b.dst, err = e.boolean(b.key); err != nil {
			return Config{}, err
		}
	}

	switch raw := get("KEY_OWNERSHIP", "on"); strings.ToLower(raw) {
	case "on", "true", "1":
		cfg.Ownership = true
	case "off", "false", "0":
	default:
		return Config{}, fmt.Errorf("KEY_OWNERSHIP: %q is not on or off", raw)
	}
	switch raw := get("REPLICATION", "sync"); strings.ToLower(raw) {
	case "sync":
	case "async":
		cfg.Async = true
	default:
		return Config{}, fmt.Errorf("REPLICATION: %q is not sync or async", raw)
	}

	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		return Config{}, errors.New("TLS_CERT and TLS_KEY must be set together")
	}
	if cfg.TLSClientAuth && cfg.TLSCA == "" {
		return Config{}, errors.New("TLS_CLIENT_AUTH needs TLS_CA to verify clients against")
	}
	if cfg.StoreTLS && cfg.StoreAddr == "" {
		return Config{}, errors.New("SECURE_STORE_TLS needs SECURE_STORE_ADDRESS: the embedded store has no network to secure")
	}
	if cfg.Surfaces, err = surfaces(vals); err != nil { // before the users that name them
		return Config{}, err
	}
	if cfg.Users, err = users(vals, cfg.UsersFile, cfg.Surfaces); err != nil {
		return Config{}, err
	}
	if cfg.AuthUser != "" {
		if _, ok := cfg.Users[cfg.AuthUser]; !ok {
			return Config{}, fmt.Errorf("AUTH_USER: no %s%s is configured", UserPrefix, cfg.AuthUser)
		}
	}
	return cfg, nil
}

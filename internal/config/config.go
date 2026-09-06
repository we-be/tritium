// Package config loads a node's settings from a dotenv file and the environment.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	DefaultStoreAddr  = "localhost:6379"
	DefaultListenAddr = "localhost:8080"
	DefaultPoolSize   = 4
)

type Config struct {
	ListenAddr    string // LISTEN_ADDRESS: where this node accepts clients and peers
	AdvertiseAddr string // ADVERTISE_ADDRESS: address peers dial us on; defaults to the bound address
	JoinAddr      string // JOIN_ADDRESS: nodes to join, comma-separated, retried for as long as they are unreachable; empty seeds a new cluster
	Password      string // AUTH_PASSWORD: required from clients when set
	PeerPassword  string // PEER_PASSWORD: what nodes AUTH to each other with; defaults to AUTH_PASSWORD
	StoreAddr     string // SECURE_STORE_ADDRESS: RESP server this node writes through
	StorePassword string // SECURE_STORE_PASSWORD: AUTH for the store and every replica
	PoolSize      int    // MAX_SERVER_CONNECTIONS: connections pooled per RESP server
	TLSCert       string // TLS_CERT: PEM certificate; with TLS_KEY, serves TLS and dials peers with it
	TLSKey        string // TLS_KEY: PEM private key
	TLSCA         string // TLS_CA: PEM bundle that peers, and clients under TLS_CLIENT_AUTH, must chain to
	TLSClientAuth bool   // TLS_CLIENT_AUTH: require client certificates (mutual TLS)
}

// Load reads path as a dotenv file (empty path: none), then lets process
// environment variables override it.
// Local is how to reach the node a dotenv file configures from the same
// machine: its port on loopback and what it demands of a client. Tools take
// it through -config so the one file serves the node and its clients.
type Local struct {
	Addr, Password, StorePassword, CA string
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
	return Local{Addr: "127.0.0.1:" + port, Password: cfg.Password, StorePassword: cfg.StorePassword, CA: cfg.TLSCA}, nil
}

// Seeds is JOIN_ADDRESS as a list. Each entry is dialed until it answers and
// again whenever it drops out of the view, so nodes can boot in any order.
func (c Config) Seeds() []string {
	var out []string
	for s := range strings.SplitSeq(c.JoinAddr, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func Load(path string) (Config, error) {
	vals := map[string]string{}
	if path != "" {
		var err error
		if vals, err = ReadDotenv(path); err != nil {
			return Config{}, err
		}
	}
	get := func(key, def string) string {
		if v, ok := os.LookupEnv(key); ok {
			return v
		}
		if v, ok := vals[key]; ok {
			return v
		}
		return def
	}

	cfg := Config{
		ListenAddr:    get("LISTEN_ADDRESS", get("RPC_ADDRESS", DefaultListenAddr)), // RPC_ADDRESS: pre-RESP name
		AdvertiseAddr: get("ADVERTISE_ADDRESS", ""),
		JoinAddr:      get("JOIN_ADDRESS", ""),
		Password:      get("AUTH_PASSWORD", ""),
		PeerPassword:  get("PEER_PASSWORD", ""),
		StoreAddr:     get("SECURE_STORE_ADDRESS", DefaultStoreAddr),
		StorePassword: get("SECURE_STORE_PASSWORD", ""),
		TLSCert:       get("TLS_CERT", ""),
		TLSKey:        get("TLS_KEY", ""),
		TLSCA:         get("TLS_CA", ""),
	}

	raw := get("MAX_SERVER_CONNECTIONS", strconv.Itoa(DefaultPoolSize))
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return Config{}, fmt.Errorf("MAX_SERVER_CONNECTIONS: %q is not a positive integer", raw)
	}
	cfg.PoolSize = n

	raw = get("TLS_CLIENT_AUTH", "false")
	if cfg.TLSClientAuth, err = strconv.ParseBool(raw); err != nil {
		return Config{}, fmt.Errorf("TLS_CLIENT_AUTH: %q is not a boolean", raw)
	}
	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		return Config{}, errors.New("TLS_CERT and TLS_KEY must be set together")
	}
	if cfg.TLSClientAuth && cfg.TLSCA == "" {
		return Config{}, errors.New("TLS_CLIENT_AUTH needs TLS_CA to verify clients against")
	}
	return cfg, nil
}

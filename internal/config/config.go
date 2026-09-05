// Package config loads a node's settings from a dotenv file and the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
)

const (
	DefaultStoreAddr = "localhost:6379"
	DefaultRPCAddr   = "localhost:8080"
	DefaultPoolSize  = 4
)

type Config struct {
	StoreAddr     string // SECURE_STORE_ADDRESS: RESP server this node writes through
	RPCAddr       string // RPC_ADDRESS: listen address for this node's RPC server
	AdvertiseAddr string // ADVERTISE_ADDRESS: address peers dial us on; defaults to the bound RPC address
	JoinAddr      string // JOIN_ADDRESS: an existing node to join; empty seeds a new cluster
	PoolSize      int    // MAX_SERVER_CONNECTIONS: connections pooled per RESP server
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
		StoreAddr:     get("SECURE_STORE_ADDRESS", DefaultStoreAddr),
		RPCAddr:       get("RPC_ADDRESS", DefaultRPCAddr),
		AdvertiseAddr: get("ADVERTISE_ADDRESS", ""),
		JoinAddr:      get("JOIN_ADDRESS", ""),
	}
	raw := get("MAX_SERVER_CONNECTIONS", strconv.Itoa(DefaultPoolSize))
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return Config{}, fmt.Errorf("MAX_SERVER_CONNECTIONS: %q is not a positive integer", raw)
	}
	cfg.PoolSize = n
	return cfg, nil
}

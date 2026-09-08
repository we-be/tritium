package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/we-be/tritium/internal/config"
)

// tlsConfigs builds the listener config and the config used to dial peers.
// Both are nil when TLS_CERT is unset. Peers present the node's own
// certificate, so TLS_CLIENT_AUTH turns node-to-node traffic into mutual TLS
// as long as every node's certificate chains to TLS_CA.
func tlsConfigs(cfg config.Config) (server, peer *tls.Config, err error) {
	if cfg.TLSCert == "" {
		return nil, nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, nil, fmt.Errorf("tls: %w", err)
	}
	var pool *x509.CertPool
	if cfg.TLSCA != "" {
		pem, err := os.ReadFile(cfg.TLSCA)
		if err != nil {
			return nil, nil, fmt.Errorf("tls: %w", err)
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, nil, fmt.Errorf("tls: no certificates found in %s", cfg.TLSCA)
		}
	}
	server = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if cfg.TLSClientAuth {
		server.ClientAuth = tls.RequireAndVerifyClientCert
		server.ClientCAs = pool
	}
	peer = &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12}
	return server, peer, nil
}

// storeTLSConfig builds what SECURE_STORE_TLS dials the store with:
// SECURE_STORE_CA pins the CA (unset trusts the system roots), and
// SECURE_STORE_SERVER_NAME overrides the address's host for verification
// and SNI, for a store reached by IP or behind a load balancer.
func storeTLSConfig(cfg config.Config) (*tls.Config, error) {
	var pool *x509.CertPool
	if cfg.StoreCA != "" {
		pem, err := os.ReadFile(cfg.StoreCA)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls: no certificates found in %s", cfg.StoreCA)
		}
	}
	name := cfg.StoreServerName
	if name == "" {
		if host, _, err := net.SplitHostPort(cfg.StoreAddr); err == nil {
			name = host
		} else {
			name = cfg.StoreAddr
		}
	}
	return &tls.Config{RootCAs: pool, ServerName: name, MinVersion: tls.VersionTLS12}, nil
}

package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
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

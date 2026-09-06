package tritium

import (
	"time"

	"github.com/we-be/tritium/internal/config"
)

// OptionsFromEnv reads a node's dotenv file and returns the options that
// reach that node from the same machine: its port on loopback, its AUTH
// password, and TLS verified against its CA when it has one. The one file
// serves the node and every client next to it.
func OptionsFromEnv(path string) (ClientOptions, error) {
	loc, err := config.LoadLocal(path)
	if err != nil {
		return ClientOptions{}, err
	}
	opts := ClientOptions{Address: loc.Addr, Password: loc.Password, Timeout: 5 * time.Second}
	if loc.CA != "" {
		if opts.TLS, err = TLSConfig(loc.CA); err != nil {
			return ClientOptions{}, err
		}
	}
	return opts, nil
}

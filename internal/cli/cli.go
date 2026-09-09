// Package cli is what every tritium command shares on its way to a node:
// the flags that say where it is and how to authenticate, and -config, a
// node's own dotenv file that fills them in so one file serves the node
// and the tools beside it.
package cli

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/we-be/tritium/pkg/tritium"
)

// Node is the flags that reach a node. User and Password default to
// $TRITIUM_USER and $TRITIUM_PASSWORD, which keeps a credential off the
// command line, where every process on the box can read it.
type Node struct {
	Config   string // a node's dotenv file
	Addr     string // the address flag, whatever it is called
	User     string
	Password string
	CA       string
	TLS      bool
	fs       *flag.FlagSet
	addrFlag string
}

// Flags registers the node flags on fs. The address flag is named by addr,
// since tritium-monitor takes a list of them as -nodes.
func Flags(fs *flag.FlagSet, addr, addrDefault, addrUsage string) *Node {
	n := &Node{fs: fs, addrFlag: addr}
	fs.StringVar(&n.Config, "config", "", "a node's dotenv file: fills -"+addr+", -user, -password and -ca from it (explicit flags win)")
	fs.StringVar(&n.Addr, addr, addrDefault, addrUsage)
	fs.StringVar(&n.User, "user", os.Getenv("TRITIUM_USER"), "AUTH as this user instead of the default one (default $TRITIUM_USER)")
	fs.StringVar(&n.Password, "password", os.Getenv("TRITIUM_PASSWORD"), "AUTH password (default $TRITIUM_PASSWORD, which keeps it off the command line)")
	fs.BoolVar(&n.TLS, "tls", false, "connect with TLS")
	fs.StringVar(&n.CA, "ca", "", "PEM bundle to verify the node against (implies -tls)")
	return n
}

// Options is what the parsed flags reach the node with. A flag given on the
// command line wins; -config fills the rest from the node's file, and what
// is left keeps its default. TLS is on when asked for, when a CA is given,
// or when the file says the node serves it.
func (n *Node) Options() (tritium.ClientOptions, error) {
	opts := tritium.ClientOptions{Address: n.Addr, User: n.User, Password: n.Password, Timeout: 5 * time.Second}
	if n.Config != "" {
		env, err := tritium.OptionsFromEnv(n.Config)
		if err != nil {
			return tritium.ClientOptions{}, err
		}
		given := map[string]bool{}
		n.fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
		if !given[n.addrFlag] {
			opts.Address = env.Address
		}
		if !given["user"] {
			opts.User = env.User
		}
		if !given["password"] {
			opts.Password = env.Password
		}
		if !given["ca"] {
			opts.TLS = env.TLS // the file's CA, or its word that the node serves TLS
		}
	}
	if opts.TLS == nil && (n.TLS || n.CA != "") {
		var err error
		if opts.TLS, err = tritium.TLSConfig(n.CA); err != nil {
			return tritium.ClientOptions{}, err
		}
	}
	return opts, nil
}

// Fail reports err as the command and exits 1.
func Fail(err error) {
	fmt.Fprintln(os.Stderr, filepath.Base(os.Args[0])+":", err)
	os.Exit(1)
}

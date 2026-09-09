package cli

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// A flag given on the command line wins over the node's file, which wins
// over the environment; the file's word that the node serves TLS is kept.
func TestOptionsPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.env")
	os.WriteFile(path, []byte("LISTEN_ADDRESS=:9090\nAUTH_PASSWORD=from-file\nTLS_CERT=c.pem\nTLS_KEY=k.pem\n"), 0o600)
	t.Setenv("TRITIUM_USER", "from-env")
	t.Setenv("TRITIUM_PASSWORD", "from-env")

	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	n := Flags(fs, "addr", "localhost:8080", "")
	fs.Parse([]string{"-config", path, "-user", "alice"})
	opts, err := n.Options()
	if err != nil || opts.Address != "127.0.0.1:9090" || opts.User != "alice" || opts.Password != "from-file" || opts.TLS == nil {
		t.Fatalf("with -config: %+v, %v", opts, err)
	}

	fs = flag.NewFlagSet("t", flag.ContinueOnError)
	n = Flags(fs, "addr", "localhost:8080", "")
	fs.Parse(nil)
	opts, err = n.Options()
	if err != nil || opts.Address != "localhost:8080" || opts.User != "from-env" || opts.Password != "from-env" || opts.TLS != nil {
		t.Fatalf("without: %+v, %v", opts, err)
	}
}

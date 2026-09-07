// Package resptest gives tests a RESP store: tritium's own in-memory store
// on a loopback port, or a real server when TRITIUM_RESP_ADDR is set.
package resptest

import (
	"net"
	"os"
	"testing"

	"github.com/we-be/tritium/internal/memstore"
)

// Addr returns a RESP server address for the test: $TRITIUM_RESP_ADDR if set
// (a real server, shared by every test), otherwise a fresh store.
func Addr(tb testing.TB) string {
	tb.Helper()
	if a := os.Getenv("TRITIUM_RESP_ADDR"); a != "" {
		return a
	}
	return Start(tb).Addr()
}

type Server struct {
	ln net.Listener
}

// Start serves a fresh store on a loopback port until the test ends.
func Start(tb testing.TB) *Server {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	st := memstore.New(memstore.Options{Version: "test"})
	go st.Serve(ln)
	tb.Cleanup(func() { ln.Close(); st.Close() })
	return &Server{ln: ln}
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

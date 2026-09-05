// Package resptest runs a tiny in-process RESP server so tests don't need a
// real Valkey, and points them at one when TRITIUM_RESP_ADDR is set.
package resptest

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// Addr returns a RESP server address for the test: $TRITIUM_RESP_ADDR if set
// (a real server, shared by every test), otherwise a fresh fake.
func Addr(tb testing.TB) string {
	tb.Helper()
	if a := os.Getenv("TRITIUM_RESP_ADDR"); a != "" {
		return a
	}
	return Start(tb).Addr()
}

// Server speaks just enough RESP for tritium: PING, SET, SETEX, GET, MGET,
// DEL and INFO.
type Server struct {
	ln net.Listener
	mu sync.Mutex
	kv map[string]entry
}

type entry struct {
	val []byte
	exp time.Time
}

// Start listens on a loopback port and stops when the test ends.
func Start(tb testing.TB) *Server {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	s := &Server{ln: ln, kv: map[string]entry{}}
	go s.serve()
	tb.Cleanup(func() { ln.Close() })
	return s
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

func (s *Server) serve() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	r := resp.NewReader(c)
	for {
		v, err := r.ReadValue()
		if err != nil {
			return
		}
		args, ok := toArgs(v)
		if !ok || len(args) == 0 {
			c.Write(errReply("ERR malformed command"))
			continue
		}
		c.Write(s.exec(args))
	}
}

func toArgs(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	args := make([]string, len(arr))
	for i, e := range arr {
		b, ok := e.([]byte)
		if !ok {
			return nil, false
		}
		args[i] = string(b)
	}
	return args, true
}

func (s *Server) exec(args []string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		return []byte("+PONG\r\n")
	case "SET":
		if len(args) != 3 {
			return errArgs(cmd)
		}
		s.kv[args[1]] = entry{val: []byte(args[2])}
		return []byte("+OK\r\n")
	case "SETEX":
		if len(args) != 4 {
			return errArgs(cmd)
		}
		ttl, err := strconv.Atoi(args[2])
		if err != nil || ttl <= 0 {
			return errReply("ERR invalid expire time in 'setex' command")
		}
		s.kv[args[1]] = entry{val: []byte(args[3]), exp: time.Now().Add(time.Duration(ttl) * time.Second)}
		return []byte("+OK\r\n")
	case "GET":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		return bulk(s.get(args[1]))
	case "MGET":
		out := fmt.Appendf(nil, "*%d\r\n", len(args)-1)
		for _, k := range args[1:] {
			out = append(out, bulk(s.get(k))...)
		}
		return out
	case "DEL":
		n := 0
		for _, k := range args[1:] {
			if _, ok := s.kv[k]; ok {
				delete(s.kv, k)
				n++
			}
		}
		return fmt.Appendf(nil, ":%d\r\n", n)
	case "INFO":
		return bulk([]byte("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"))
	}
	return errReply("ERR unknown command '" + args[0] + "'")
}

// get returns nil for a missing or expired key and a non-nil slice otherwise,
// mirroring a real server's null-vs-empty bulk reply.
func (s *Server) get(k string) []byte {
	e, ok := s.kv[k]
	if !ok {
		return nil
	}
	if !e.exp.IsZero() && time.Now().After(e.exp) {
		delete(s.kv, k)
		return nil
	}
	if e.val == nil {
		return []byte{}
	}
	return e.val
}

func bulk(b []byte) []byte {
	if b == nil {
		return []byte("$-1\r\n")
	}
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(b), b)
}

func errReply(msg string) []byte { return []byte("-" + msg + "\r\n") }

func errArgs(cmd string) []byte {
	return errReply("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
}

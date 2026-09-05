// Package resptest runs a tiny in-process RESP server so tests don't need a
// real Valkey, and points them at one when TRITIUM_RESP_ADDR is set.
package resptest

import (
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

// Server speaks just enough RESP to stand in for a backend store: PING, AUTH,
// SET, SETEX, GET, MGET, DEL, EXISTS, TTL and INFO.
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
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		c.Write(s.exec(args))
	}
}

func (s *Server) exec(args []string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		return resp.AppendSimpleString(nil, "PONG")
	case "AUTH":
		return resp.AppendSimpleString(nil, "OK")
	case "SET":
		if len(args) != 3 {
			return errArgs(cmd)
		}
		s.kv[args[1]] = entry{val: []byte(args[2])}
		return resp.AppendSimpleString(nil, "OK")
	case "SETEX":
		if len(args) != 4 {
			return errArgs(cmd)
		}
		ttl, err := strconv.Atoi(args[2])
		if err != nil || ttl <= 0 {
			return resp.AppendError(nil, "ERR invalid expire time in 'setex' command")
		}
		s.kv[args[1]] = entry{val: []byte(args[3]), exp: time.Now().Add(time.Duration(ttl) * time.Second)}
		return resp.AppendSimpleString(nil, "OK")
	case "GET":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		return resp.AppendBulk(nil, s.get(args[1]))
	case "MGET":
		out := resp.AppendArray(nil, len(args)-1)
		for _, k := range args[1:] {
			out = resp.AppendBulk(out, s.get(k))
		}
		return out
	case "DEL":
		var n int64
		for _, k := range args[1:] {
			if _, ok := s.kv[k]; ok {
				delete(s.kv, k)
				n++
			}
		}
		return resp.AppendInt(nil, n)
	case "EXISTS":
		var n int64
		for _, k := range args[1:] {
			if s.get(k) != nil {
				n++
			}
		}
		return resp.AppendInt(nil, n)
	case "TTL":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		e, ok := s.kv[args[1]]
		switch {
		case !ok || s.get(args[1]) == nil:
			return resp.AppendInt(nil, -2)
		case e.exp.IsZero():
			return resp.AppendInt(nil, -1)
		default:
			return resp.AppendInt(nil, int64(time.Until(e.exp).Seconds()))
		}
	case "INFO":
		return resp.AppendBulkString(nil, "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n")
	}
	return resp.AppendError(nil, "ERR unknown command '"+args[0]+"'")
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

func errArgs(cmd string) []byte {
	return resp.AppendError(nil, "ERR wrong number of arguments for '"+strings.ToLower(cmd)+"' command")
}

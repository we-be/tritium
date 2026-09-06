// Package resptest runs a tiny in-process RESP server so tests don't need a
// real Valkey, and points them at one when TRITIUM_RESP_ADDR is set.
package resptest

import (
	"math"
	"net"
	"os"
	"slices"
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

// Server speaks just enough RESP to stand in for a backend store: strings
// (SET with EX/NX, SETEX, GET, GETDEL, MGET), DEL, EXISTS, TTL, EXPIRE, SCAN, TYPE,
// sorted sets (ZADD, ZRANGEBYSCORE, ZREM, ZREMRANGEBYSCORE, ZCARD), PING,
// AUTH and INFO.
type Server struct {
	ln net.Listener
	mu sync.Mutex
	kv map[string]*entry
}

type entry struct {
	val  []byte
	zset map[string]float64 // set for sorted-set keys; val is nil then
	exp  time.Time
}

// Start listens on a loopback port and stops when the test ends.
func Start(tb testing.TB) *Server {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	s := &Server{ln: ln, kv: map[string]*entry{}}
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
		if len(args) < 3 {
			return errArgs(cmd)
		}
		e := &entry{val: []byte(args[2])}
		nx := false
		for i := 3; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "NX":
				nx = true
			case "EX":
				if i+1 >= len(args) {
					return resp.AppendError(nil, "ERR syntax error")
				}
				n, err := strconv.Atoi(args[i+1])
				if err != nil || n <= 0 {
					return resp.AppendError(nil, "ERR invalid expire time in 'set' command")
				}
				e.exp = time.Now().Add(time.Duration(n) * time.Second)
				i++
			default:
				return resp.AppendError(nil, "ERR syntax error")
			}
		}
		if nx && s.live(args[1]) != nil {
			return resp.AppendNull(nil)
		}
		s.kv[args[1]] = e
		return resp.AppendSimpleString(nil, "OK")
	case "SETEX":
		if len(args) != 4 {
			return errArgs(cmd)
		}
		ttl, err := strconv.Atoi(args[2])
		if err != nil || ttl <= 0 {
			return resp.AppendError(nil, "ERR invalid expire time in 'setex' command")
		}
		s.kv[args[1]] = &entry{val: []byte(args[3]), exp: time.Now().Add(time.Duration(ttl) * time.Second)}
		return resp.AppendSimpleString(nil, "OK")
	case "GET":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		return resp.AppendBulk(nil, s.get(args[1]))
	case "GETDEL":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		v := s.get(args[1])
		delete(s.kv, args[1])
		return resp.AppendBulk(nil, v)
	case "EXPIRE":
		if len(args) != 3 && len(args) != 4 {
			return errArgs(cmd)
		}
		n, err := strconv.Atoi(args[2])
		e := s.live(args[1])
		if err != nil || e == nil {
			return resp.AppendInt(nil, 0)
		}
		exp := time.Now().Add(time.Duration(n) * time.Second)
		if len(args) == 4 {
			switch strings.ToUpper(args[3]) {
			case "GT":
				if !e.exp.IsZero() && !exp.After(e.exp) {
					return resp.AppendInt(nil, 0)
				}
			case "LT":
				if !e.exp.IsZero() && !exp.Before(e.exp) {
					return resp.AppendInt(nil, 0)
				}
			case "NX":
				if !e.exp.IsZero() {
					return resp.AppendInt(nil, 0)
				}
			case "XX":
				if e.exp.IsZero() {
					return resp.AppendInt(nil, 0)
				}
			}
		}
		e.exp = exp
		return resp.AppendInt(nil, 1)
	case "SCAN": // one page holds everything: enough for a sync to walk
		keys := []any{}
		for k := range s.kv {
			if s.live(k) != nil {
				keys = append(keys, []byte(k))
			}
		}
		return resp.AppendValue(nil, []any{[]byte("0"), keys})
	case "TYPE":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		switch e := s.live(args[1]); {
		case e == nil:
			return resp.AppendSimpleString(nil, "none")
		case e.zset != nil:
			return resp.AppendSimpleString(nil, "zset")
		default:
			return resp.AppendSimpleString(nil, "string")
		}
	case "ZADD":
		nx := len(args) > 2 && strings.EqualFold(args[2], "NX")
		if nx {
			args = append(args[:2], args[3:]...)
		}
		if len(args) < 4 || len(args)%2 != 0 {
			return errArgs(cmd)
		}
		e := s.live(args[1])
		if e == nil {
			e = &entry{zset: map[string]float64{}}
			s.kv[args[1]] = e
		}
		if e.zset == nil {
			return resp.AppendError(nil, "WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		var added int64
		for i := 2; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i], 64)
			if err != nil {
				return resp.AppendError(nil, "ERR value is not a valid float")
			}
			if _, ok := e.zset[args[i+1]]; !ok {
				added++
			} else if nx {
				continue
			}
			e.zset[args[i+1]] = score
		}
		return resp.AppendInt(nil, added)
	case "ZRANGEBYSCORE":
		if len(args) < 4 {
			return errArgs(cmd)
		}
		lo, loEx, err1 := parseBound(args[2])
		hi, hiEx, err2 := parseBound(args[3])
		if err1 != nil || err2 != nil {
			return resp.AppendError(nil, "ERR min or max is not a float")
		}
		withScores, offset, count := false, 0, -1
		for i := 4; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "WITHSCORES":
				withScores = true
			case "LIMIT":
				if i+2 >= len(args) {
					return resp.AppendError(nil, "ERR syntax error")
				}
				offset, _ = strconv.Atoi(args[i+1])
				count, _ = strconv.Atoi(args[i+2])
				i += 2
			default:
				return resp.AppendError(nil, "ERR syntax error")
			}
		}
		members := s.zrange(args[1], lo, loEx, hi, hiEx)
		if offset > len(members) {
			offset = len(members)
		}
		members = members[offset:]
		if count >= 0 && count < len(members) {
			members = members[:count]
		}
		n := len(members)
		if withScores {
			n *= 2
		}
		out := resp.AppendArray(nil, n)
		e := s.live(args[1])
		for _, m := range members {
			out = resp.AppendBulkString(out, m)
			if withScores {
				out = resp.AppendBulkString(out, strconv.FormatFloat(e.zset[m], 'f', -1, 64))
			}
		}
		return out
	case "ZREM":
		if len(args) < 3 {
			return errArgs(cmd)
		}
		var n int64
		if e := s.live(args[1]); e != nil && e.zset != nil {
			for _, m := range args[2:] {
				if _, ok := e.zset[m]; ok {
					delete(e.zset, m)
					n++
				}
			}
		}
		return resp.AppendInt(nil, n)
	case "ZREMRANGEBYSCORE":
		if len(args) != 4 {
			return errArgs(cmd)
		}
		lo, loEx, err1 := parseBound(args[2])
		hi, hiEx, err2 := parseBound(args[3])
		if err1 != nil || err2 != nil {
			return resp.AppendError(nil, "ERR min or max is not a float")
		}
		members := s.zrange(args[1], lo, loEx, hi, hiEx)
		if e := s.live(args[1]); e != nil {
			for _, m := range members {
				delete(e.zset, m)
			}
		}
		return resp.AppendInt(nil, int64(len(members)))
	case "ZCARD":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		if e := s.live(args[1]); e != nil && e.zset != nil {
			return resp.AppendInt(nil, int64(len(e.zset)))
		}
		return resp.AppendInt(nil, 0)
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
			if s.live(k) != nil {
				n++
			}
		}
		return resp.AppendInt(nil, n)
	case "TTL":
		if len(args) != 2 {
			return errArgs(cmd)
		}
		e := s.live(args[1])
		switch {
		case e == nil:
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

// live returns the entry under k unless it is missing or expired.
func (s *Server) live(k string) *entry {
	e, ok := s.kv[k]
	if !ok {
		return nil
	}
	if !e.exp.IsZero() && time.Now().After(e.exp) {
		delete(s.kv, k)
		return nil
	}
	return e
}

// get returns nil for a missing or expired key and a non-nil slice otherwise,
// mirroring a real server's null-vs-empty bulk reply.
func (s *Server) get(k string) []byte {
	e := s.live(k)
	if e == nil || e.zset != nil {
		return nil
	}
	if e.val == nil {
		return []byte{}
	}
	return e.val
}

// zrange lists a sorted set's members with scores in [lo, hi], ordered by
// score then member.
func (s *Server) zrange(k string, lo float64, loEx bool, hi float64, hiEx bool) []string {
	e := s.live(k)
	if e == nil || e.zset == nil {
		return nil
	}
	var members []string
	for m, sc := range e.zset {
		if sc < lo || sc > hi || (loEx && sc == lo) || (hiEx && sc == hi) {
			continue
		}
		members = append(members, m)
	}
	slices.SortFunc(members, func(a, b string) int {
		if e.zset[a] != e.zset[b] {
			if e.zset[a] < e.zset[b] {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	})
	return members
}

// parseBound reads a ZRANGEBYSCORE bound: a float, -inf, +inf, or "(" for exclusive.
func parseBound(s string) (float64, bool, error) {
	exclusive := strings.HasPrefix(s, "(")
	s = strings.TrimPrefix(s, "(")
	switch s {
	case "-inf":
		return math.Inf(-1), exclusive, nil
	case "+inf", "inf":
		return math.Inf(1), exclusive, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, exclusive, err
}

func errArgs(cmd string) []byte {
	return resp.AppendError(nil, "ERR wrong number of arguments for '"+strings.ToLower(cmd)+"' command")
}

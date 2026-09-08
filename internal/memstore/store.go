// Package memstore is tritium's own store: strings and sorted sets in
// memory, every key with an optional expiry, served over RESP so a node
// uses it exactly as it would use Valkey. It is what a node runs when no
// store address is configured — one binary, nothing to run beside it — and
// what the tests run instead of a real server.
package memstore

import (
	"fmt"
	"hash/maphash"
	"math"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// A key belongs to one of these buckets for life, so SCAN can walk them in
// order and hand a key out exactly once no matter what is written meanwhile.
const buckets = 4096

// Charged per key and per sorted-set member beyond their own bytes: the
// entry, the map slots, the expiry item, the string headers. Measured
// against the live heap (TestHeapPerKey): a 64-byte value costs 274 bytes
// live, so the charge is what STORE_MAX_MEMORY really bounds.
const (
	keyOverhead    = 200
	memberOverhead = 48
)

type Options struct {
	MaxMemory int64  // bytes of keys and values kept; past it the soonest-expiring keys go. 0: no limit
	Version   string // what INFO reports
}

type Store struct {
	mu      sync.Mutex
	kv      map[string]*entry
	bucket  [buckets]map[string]struct{}
	seed    maphash.Seed
	exp     expiries // soonest first; an item whose gen no longer matches its key is stale
	gen     uint64
	used    int64
	max     int64
	tomb    map[string]tombstone // stamps of deleted strings, so an older write arriving late cannot bring one back
	version string
	started time.Time
	now     func() time.Time
	stop    chan struct{}
	once    sync.Once
}

type entry struct {
	val   []byte
	zset  map[string]float64 // set for a sorted set; val is nil then
	exp   time.Time
	gen   uint64 // bumped when the expiry changes, so an older heap item is ignored
	size  int64
	stamp uint64 // the write stamp a string was last set under; 0 for an unstamped write or a sorted set
}

func New(o Options) *Store {
	s := &Store{kv: map[string]*entry{}, tomb: map[string]tombstone{}, seed: maphash.MakeSeed(), max: o.MaxMemory, version: o.Version, now: time.Now, stop: make(chan struct{})}
	s.started = s.now()
	if o.MaxMemory > 0 {
		// The runtime keeps garbage up to the live heap by default, so a store
		// at its cap would sit at twice it in RSS; a soft limit above the cap
		// makes the collector work harder before that.
		debug.SetMemoryLimit(o.MaxMemory + o.MaxMemory/2)
	}
	go s.sweeper()
	return s
}

// Serve answers connections from ln until it is closed.
func (s *Store) Serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go s.ServeConn(c)
	}
}

// ServeConn answers one connection's commands in order until it closes.
func (s *Store) ServeConn(c net.Conn) {
	defer c.Close()
	r := resp.NewReader(c)
	var out []byte
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		out = s.exec(out[:0], args)
		if _, err := c.Write(out); err != nil {
			return
		}
		if strings.EqualFold(args[0], "QUIT") {
			return
		}
	}
}

// Close stops the expiry sweeper; connections end with their listener.
func (s *Store) Close() {
	s.once.Do(func() { close(s.stop) })
}

func (s *Store) exec(b []byte, args []string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweep(16)
	return s.run(b, args)
}

// run answers one command; the caller holds mu.
func (s *Store) run(b []byte, args []string) []byte {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		if len(args) == 2 {
			return resp.AppendBulkString(b, args[1])
		}
		return resp.AppendSimpleString(b, "PONG")
	case "ECHO":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		return resp.AppendBulkString(b, args[1])
	case "AUTH", "SELECT", "QUIT":
		return resp.AppendSimpleString(b, "OK")
	case "STAMPED":
		return s.stamped(b, args)
	case "STAMPOF":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		return resp.AppendInt(b, int64(s.stampOf(args[1])))
	case "SET":
		return s.set(b, args)
	case "SETEX":
		if len(args) != 4 {
			return errArgs(b, cmd)
		}
		ttl, err := strconv.Atoi(args[2])
		if err != nil || ttl <= 0 {
			return resp.AppendError(b, "ERR invalid expire time in 'setex' command")
		}
		return s.setString(b, args[1], []byte(args[3]), s.now().Add(time.Duration(ttl)*time.Second))
	case "GET":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		if s.isZSet(args[1]) {
			return wrongType(b)
		}
		return resp.AppendBulk(b, s.get(args[1]))
	case "GETDEL":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		if s.isZSet(args[1]) {
			return wrongType(b)
		}
		v := s.get(args[1])
		if e := s.kv[args[1]]; e != nil {
			s.remove(args[1], e)
		}
		return resp.AppendBulk(b, v)
	case "MGET":
		if len(args) < 2 {
			return errArgs(b, cmd)
		}
		b = resp.AppendArray(b, len(args)-1)
		for _, k := range args[1:] {
			b = resp.AppendBulk(b, s.get(k))
		}
		return b
	case "DEL":
		var n int64
		for _, k := range args[1:] {
			if e := s.live(k); e != nil {
				s.remove(k, e)
				n++
			}
		}
		return resp.AppendInt(b, n)
	case "EXISTS":
		var n int64
		for _, k := range args[1:] {
			if s.live(k) != nil {
				n++
			}
		}
		return resp.AppendInt(b, n)
	case "TTL", "PTTL":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		e := s.live(args[1])
		switch {
		case e == nil:
			return resp.AppendInt(b, -2)
		case e.exp.IsZero():
			return resp.AppendInt(b, -1)
		case cmd == "PTTL":
			return resp.AppendInt(b, int64(e.exp.Sub(s.now())/time.Millisecond))
		default:
			return resp.AppendInt(b, int64((e.exp.Sub(s.now())+500*time.Millisecond)/time.Second))
		}
	case "EXPIRE":
		return s.expire(b, args)
	case "TYPE":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		return resp.AppendSimpleString(b, typeOf(s.live(args[1])))
	case "SCAN":
		return s.scan(b, args)
	case "DBSIZE":
		s.sweep(math.MaxInt)
		return resp.AppendInt(b, int64(len(s.kv)))
	case "FLUSHALL", "FLUSHDB":
		for k, e := range s.kv {
			s.remove(k, e)
		}
		for k := range s.tomb {
			s.untomb(k)
		}
		s.exp = s.exp[:0]
		return resp.AppendSimpleString(b, "OK")
	case "ZADD":
		return s.zadd(b, args)
	case "ZRANGEBYSCORE":
		return s.zrangebyscore(b, args)
	case "ZREM":
		if len(args) < 3 {
			return errArgs(b, cmd)
		}
		var n int64
		if e := s.live(args[1]); e != nil && e.zset != nil {
			for _, m := range args[2:] {
				if _, ok := e.zset[m]; ok {
					s.zdel(e, m)
					n++
				}
			}
			s.dropEmpty(args[1], e)
		}
		return resp.AppendInt(b, n)
	case "ZREMRANGEBYSCORE":
		if len(args) != 4 {
			return errArgs(b, cmd)
		}
		lo, loEx, err1 := parseBound(args[2])
		hi, hiEx, err2 := parseBound(args[3])
		if err1 != nil || err2 != nil {
			return resp.AppendError(b, "ERR min or max is not a float")
		}
		members := s.zrange(args[1], lo, loEx, hi, hiEx)
		if e := s.live(args[1]); e != nil {
			for _, m := range members {
				s.zdel(e, m)
			}
			s.dropEmpty(args[1], e)
		}
		return resp.AppendInt(b, int64(len(members)))
	case "ZREMRANGEBYRANK":
		if len(args) != 4 {
			return errArgs(b, cmd)
		}
		start, err1 := strconv.Atoi(args[2])
		stop, err2 := strconv.Atoi(args[3])
		if err1 != nil || err2 != nil {
			return resp.AppendError(b, "ERR value is not an integer or out of range")
		}
		e := s.live(args[1])
		if e == nil || e.zset == nil {
			return resp.AppendInt(b, 0)
		}
		members := sorted(e)
		n := len(members)
		if start < 0 {
			start += n
		}
		if stop < 0 {
			stop += n
		}
		start = max(start, 0) // a stop still negative means the range is empty, as on a real server
		if start > stop || start >= n {
			return resp.AppendInt(b, 0)
		}
		stop = min(stop, n-1)
		for _, m := range members[start : stop+1] {
			s.zdel(e, m)
		}
		s.dropEmpty(args[1], e)
		return resp.AppendInt(b, int64(stop-start+1))
	case "ZCARD":
		if len(args) != 2 {
			return errArgs(b, cmd)
		}
		if e := s.live(args[1]); e != nil && e.zset != nil {
			return resp.AppendInt(b, int64(len(e.zset)))
		}
		return resp.AppendInt(b, 0)
	case "INFO":
		return resp.AppendBulkString(b, s.info())
	}
	return resp.AppendError(b, "ERR unknown command '"+args[0]+"'")
}

func (s *Store) set(b []byte, args []string) []byte {
	if len(args) < 3 {
		return errArgs(b, "SET")
	}
	var exp time.Time
	nx, xx, keep := false, false, false
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keep = true
		case "EX", "PX":
			if i+1 >= len(args) {
				return resp.AppendError(b, "ERR syntax error")
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				return resp.AppendError(b, "ERR invalid expire time in 'set' command")
			}
			unit := time.Second
			if strings.EqualFold(args[i], "PX") {
				unit = time.Millisecond
			}
			exp = s.now().Add(time.Duration(n) * unit)
			i++
		default:
			return resp.AppendError(b, "ERR syntax error")
		}
	}
	old := s.live(args[1])
	if (nx && old != nil) || (xx && old == nil) {
		return resp.AppendNull(b)
	}
	if keep && old != nil {
		exp = old.exp
	}
	return s.setString(b, args[1], []byte(args[2]), exp)
}

func (s *Store) setString(b []byte, k string, v []byte, exp time.Time) []byte {
	e := &entry{val: v, exp: exp, size: keyOverhead + int64(len(k)+len(v))}
	if !s.room(e.size - s.sizeOf(k)) {
		return errOOM(b)
	}
	s.put(k, e)
	return resp.AppendSimpleString(b, "OK")
}

func (s *Store) expire(b []byte, args []string) []byte {
	if len(args) != 3 && len(args) != 4 {
		return errArgs(b, "EXPIRE")
	}
	n, err := strconv.Atoi(args[2])
	e := s.live(args[1])
	if err != nil || e == nil {
		return resp.AppendInt(b, 0)
	}
	exp := s.now().Add(time.Duration(n) * time.Second)
	if len(args) == 4 {
		switch strings.ToUpper(args[3]) {
		case "GT":
			if !e.exp.IsZero() && !exp.After(e.exp) {
				return resp.AppendInt(b, 0)
			}
		case "LT":
			if !e.exp.IsZero() && !exp.Before(e.exp) {
				return resp.AppendInt(b, 0)
			}
		case "NX":
			if !e.exp.IsZero() {
				return resp.AppendInt(b, 0)
			}
		case "XX":
			if e.exp.IsZero() {
				return resp.AppendInt(b, 0)
			}
		}
	}
	s.setExpiry(args[1], e, exp)
	return resp.AppendInt(b, 1)
}

func (s *Store) info() string {
	var expiring int
	for _, e := range s.kv {
		if !e.exp.IsZero() {
			expiring++
		}
	}
	return fmt.Sprintf("# Server\r\ntritium_version:%s\r\nuptime_in_seconds:%d\r\n"+
		"# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\nmaxmemory_policy:volatile-ttl\r\n"+
		"# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"+
		"# Keyspace\r\ndb0:keys=%d,expires=%d,avg_ttl=0\r\ntombstones:%d\r\n",
		s.version, int64(s.now().Sub(s.started)/time.Second), s.used, s.max, len(s.kv), expiring, len(s.tomb))
}

// ── keys ──────────────────────────────────────────────────────────────────

// live returns the entry under k unless it is missing or expired.
func (s *Store) live(k string) *entry {
	e, ok := s.kv[k]
	if !ok {
		return nil
	}
	if !e.exp.IsZero() && !s.now().Before(e.exp) {
		s.remove(k, e)
		return nil
	}
	return e
}

// isZSet reports whether k holds a sorted set: what GET and GETDEL refuse
// with WRONGTYPE, as Redis does, rather than answer as a missing key.
func (s *Store) isZSet(k string) bool {
	e := s.live(k)
	return e != nil && e.zset != nil
}

func wrongType(b []byte) []byte {
	return resp.AppendError(b, "WRONGTYPE Operation against a key holding the wrong kind of value")
}

// get is the string under k: nil when missing, expired or a sorted set (MGET's
// answer for one; GET and GETDEL check the type first).
func (s *Store) get(k string) []byte {
	e := s.live(k)
	if e == nil || e.zset != nil {
		return nil
	}
	if e.val == nil {
		return []byte{}
	}
	return e.val
}

func (s *Store) sizeOf(k string) int64 {
	if e := s.kv[k]; e != nil {
		return e.size
	}
	return 0
}

// put makes e the value under k, whatever was there.
func (s *Store) put(k string, e *entry) {
	if old := s.kv[k]; old != nil {
		s.used -= old.size
	} else {
		i := s.at(k)
		if s.bucket[i] == nil {
			s.bucket[i] = map[string]struct{}{}
		}
		s.bucket[i][k] = struct{}{}
	}
	s.kv[k] = e
	s.used += e.size
	s.setExpiry(k, e, e.exp)
}

func (s *Store) remove(k string, e *entry) {
	s.used -= e.size
	delete(s.kv, k)
	delete(s.bucket[s.at(k)], k)
}

func (s *Store) at(k string) uint64 { return maphash.String(s.seed, k) % buckets }

func typeOf(e *entry) string {
	switch {
	case e == nil:
		return "none"
	case e.zset != nil:
		return "zset"
	default:
		return "string"
	}
}

func errArgs(b []byte, cmd string) []byte {
	return resp.AppendError(b, "ERR wrong number of arguments for '"+strings.ToLower(cmd)+"' command")
}

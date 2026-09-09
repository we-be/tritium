// Package memstore is tritium's own store: strings and sorted sets in
// memory, every key with an optional expiry, served over RESP so a node
// uses it exactly as it would use Valkey. It is what a node runs when no
// store address is configured — one binary, nothing to run beside it — and
// what the tests run instead of a real server.
package memstore

import (
	"hash/maphash"
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
	name := strings.ToUpper(args[0])
	cmd, ok := commands[name]
	if !ok {
		return resp.AppendError(b, "ERR unknown command '"+args[0]+"'")
	}
	if n := len(args) - 1; n < cmd.min || (cmd.max >= 0 && n > cmd.max) {
		return errArgs(b, name)
	}
	return cmd.fn(s, b, args)
}

// set handles SET key value [EX seconds | PX milliseconds] [NX | XX] [KEEPTTL].
func (s *Store) set(b []byte, args []string) []byte {
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

// expire handles EXPIRE key seconds [NX | XX | GT | LT].
func (s *Store) expire(b []byte, args []string) []byte {
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

// Package memstore is tritium's own store: strings and sorted sets in
// memory, every key with an optional expiry, served over RESP so a node
// uses it exactly as it would use Valkey. It is what a node runs when no
// store address is configured — one binary, nothing to run beside it — and
// what the tests run instead of a real server.
package memstore

import (
	"container/heap"
	"fmt"
	"hash/maphash"
	"math"
	"net"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// A key belongs to one of these buckets for life, so SCAN can walk them in
// order and hand a key out exactly once no matter what is written meanwhile.
const buckets = 4096

// scanPageMax bounds one SCAN page, whatever COUNT asks: a page is built under
// the store's lock, and KEYS is unsupported for the same reason.
const scanPageMax = 10000

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

// A tombstone outlives its key by this long: a write stamped before the
// delete can arrive that late from a partition's replay, no later.
const tombstoneTTL = 24 * time.Hour

type tombstone struct {
	stamp uint64
	at    time.Time
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

// Keys is how many keys are held, expired ones not yet swept included.
func (s *Store) Keys() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.kv)
}

// sweeper expires keys nobody reads, so their memory comes back.
func (s *Store) sweeper() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.mu.Lock()
			s.sweep(math.MaxInt)
			s.mu.Unlock()
		}
	}
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
		return resp.AppendBulk(b, s.get(args[1]))
	case "GETDEL":
		if len(args) != 2 {
			return errArgs(b, cmd)
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

// stamped runs STAMPED <stamp> <write...>: the write is applied only if its
// stamp is newer than the one the key was last written under — or deleted
// under, while its tombstone lives — and the key then carries that stamp.
// So writes to one key from anywhere settle the same way everywhere,
// whichever order they arrive in. Only strings are stamped: a sorted set's
// members are written independently, so its writes apply as they come.
func (s *Store) stamped(b []byte, args []string) []byte {
	if len(args) < 3 {
		return errArgs(b, "STAMPED")
	}
	n, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return resp.AppendError(b, "ERR invalid stamp")
	}
	inner := args[2:]
	switch strings.ToUpper(inner[0]) {
	case "SET", "SETEX":
		if len(inner) < 3 {
			return errArgs(b, inner[0])
		}
		if n <= s.stampOf(inner[1]) {
			return resp.AppendSimpleString(b, "OK") // an older write, already superseded here
		}
		out := s.run(b, inner)
		if e := s.kv[inner[1]]; e != nil && e.zset == nil {
			e.stamp = n
			s.untomb(inner[1])
		}
		return out
	case "DEL":
		var count int64
		for _, k := range inner[1:] {
			if n <= s.stampOf(k) {
				continue
			}
			if e := s.live(k); e != nil {
				s.remove(k, e)
				count++
			}
			s.entomb(k, n)
		}
		return resp.AppendInt(b, count)
	case "GETDEL":
		if len(inner) != 2 || n <= s.stampOf(inner[1]) {
			return resp.AppendNull(b)
		}
		out := s.run(b, inner)
		s.entomb(inner[1], n)
		return out
	default:
		return s.run(b, inner) // sorted-set writes and the rest: the stamp is not theirs to keep
	}
}

// stampOf is the stamp a key was last written under: its entry's, its
// tombstone's, or 0 for a key never written with one.
func (s *Store) stampOf(k string) uint64 {
	if e := s.live(k); e != nil {
		return e.stamp
	}
	if t, ok := s.tomb[k]; ok {
		return t.stamp
	}
	return 0
}

func (s *Store) entomb(k string, stamp uint64) {
	if _, ok := s.tomb[k]; !ok {
		s.used += keyOverhead + int64(len(k))
	}
	s.tomb[k] = tombstone{stamp: stamp, at: s.now()}
}

func (s *Store) untomb(k string) {
	if _, ok := s.tomb[k]; ok {
		s.used -= keyOverhead + int64(len(k))
		delete(s.tomb, k)
	}
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

func (s *Store) zadd(b []byte, args []string) []byte {
	nx := len(args) > 2 && strings.EqualFold(args[2], "NX")
	if nx {
		args = append(args[:2], args[3:]...)
	}
	if len(args) < 4 || len(args)%2 != 0 {
		return errArgs(b, "ZADD")
	}
	k := args[1]
	e := s.live(k)
	if e != nil && e.zset == nil {
		return resp.AppendError(b, "WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	scores := make([]float64, 0, (len(args)-2)/2)
	var need int64
	for i := 2; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return resp.AppendError(b, "ERR value is not a valid float")
		}
		scores = append(scores, score)
		if e == nil {
			need += memberOverhead + int64(len(args[i+1]))
		} else if _, ok := e.zset[args[i+1]]; !ok {
			need += memberOverhead + int64(len(args[i+1]))
		}
	}
	if e == nil {
		need += keyOverhead + int64(len(k))
	}
	if !s.room(need) {
		return errOOM(b)
	}
	if e = s.kv[k]; e == nil { // room may have evicted it
		e = &entry{zset: map[string]float64{}, size: keyOverhead + int64(len(k))}
		s.put(k, e)
	}
	var added int64
	for i := 3; i < len(args); i += 2 {
		m := args[i]
		if _, ok := e.zset[m]; !ok {
			added++
			grow := memberOverhead + int64(len(m))
			e.size += grow
			s.used += grow
		} else if nx {
			continue
		}
		e.zset[m] = scores[(i-3)/2]
	}
	return resp.AppendInt(b, added)
}

func (s *Store) zrangebyscore(b []byte, args []string) []byte {
	if len(args) < 4 {
		return errArgs(b, "ZRANGEBYSCORE")
	}
	lo, loEx, err1 := parseBound(args[2])
	hi, hiEx, err2 := parseBound(args[3])
	if err1 != nil || err2 != nil {
		return resp.AppendError(b, "ERR min or max is not a float")
	}
	withScores, offset, count := false, 0, -1
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return resp.AppendError(b, "ERR syntax error")
			}
			offset, _ = strconv.Atoi(args[i+1])
			count, _ = strconv.Atoi(args[i+2])
			i += 2
		default:
			return resp.AppendError(b, "ERR syntax error")
		}
	}
	members := s.zrange(args[1], lo, loEx, hi, hiEx)
	members = members[min(offset, len(members)):]
	if count >= 0 && count < len(members) {
		members = members[:count]
	}
	n := len(members)
	if withScores {
		n *= 2
	}
	b = resp.AppendArray(b, n)
	e := s.live(args[1])
	for _, m := range members {
		b = resp.AppendBulkString(b, m)
		if withScores {
			b = resp.AppendBulkString(b, strconv.FormatFloat(e.zset[m], 'f', -1, 64))
		}
	}
	return b
}

// scan walks the buckets from the cursor until it has COUNT keys, and hands
// back the next bucket as the cursor; 0 once the last bucket is done.
func (s *Store) scan(b []byte, args []string) []byte {
	if len(args) < 2 {
		return errArgs(b, "SCAN")
	}
	cursor, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return resp.AppendError(b, "ERR invalid cursor")
	}
	pattern, typ, count := "", "", 10
	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "TYPE":
			typ = strings.ToLower(args[i+1])
		case "COUNT":
			if count, err = strconv.Atoi(args[i+1]); err != nil || count < 1 {
				return resp.AppendError(b, "ERR value is not an integer or out of range")
			}
			count = min(count, scanPageMax) // a hint, not a way to ask for the whole keyspace under one lock
		default:
			return resp.AppendError(b, "ERR syntax error")
		}
	}
	var keys []any
	i := cursor
	for ; i < buckets && len(keys) < count; i++ {
		for k := range s.bucket[i] {
			e := s.live(k)
			if e == nil || (pattern != "" && !match(pattern, k)) || (typ != "" && typeOf(e) != typ) {
				continue
			}
			keys = append(keys, []byte(k))
		}
	}
	if i >= buckets {
		i = 0
	}
	return resp.AppendValue(b, []any{[]byte(strconv.FormatUint(i, 10)), keys})
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

// get is the string under k: nil when missing, expired or a sorted set.
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

func (s *Store) setExpiry(k string, e *entry, exp time.Time) {
	s.gen++
	e.gen, e.exp = s.gen, exp
	if !exp.IsZero() {
		heap.Push(&s.exp, item{at: exp.UnixNano(), key: k, gen: e.gen})
	}
}

// sweep removes up to limit expired keys, soonest first, and tombstones
// past their time.
func (s *Store) sweep(limit int) {
	now := s.now().UnixNano()
	for limit > 0 && len(s.exp) > 0 && s.exp[0].at <= now {
		it := heap.Pop(&s.exp).(item)
		if e := s.kv[it.key]; e != nil && e.gen == it.gen {
			s.remove(it.key, e)
			limit--
		}
	}
	if limit > 1000 { // the full sweep, once a second: tombstones are few and unindexed
		cutoff := s.now().Add(-tombstoneTTL)
		for k, t := range s.tomb {
			if t.at.Before(cutoff) {
				s.untomb(k)
			}
		}
	}
}

// room makes need more bytes fit under the limit by evicting the keys that
// would expire soonest; false when only keys without an expiry are left.
func (s *Store) room(need int64) bool {
	for s.max > 0 && s.used+need > s.max {
		if len(s.exp) == 0 {
			return false
		}
		it := heap.Pop(&s.exp).(item)
		if e := s.kv[it.key]; e != nil && e.gen == it.gen {
			s.remove(it.key, e)
		}
	}
	return true
}

func (s *Store) zdel(e *entry, m string) {
	shrink := memberOverhead + int64(len(m))
	delete(e.zset, m)
	e.size -= shrink
	s.used -= shrink
}

// dropEmpty removes a sorted set its last member left, as a real server does.
func (s *Store) dropEmpty(k string, e *entry) {
	if len(e.zset) == 0 {
		s.remove(k, e)
	}
}

// zrange lists a sorted set's members with scores in [lo, hi], ordered by
// score then member.
func (s *Store) zrange(k string, lo float64, loEx bool, hi float64, hiEx bool) []string {
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
	sortMembers(e, members)
	return members
}

func sorted(e *entry) []string {
	members := make([]string, 0, len(e.zset))
	for m := range e.zset {
		members = append(members, m)
	}
	sortMembers(e, members)
	return members
}

func sortMembers(e *entry, members []string) {
	slices.SortFunc(members, func(a, b string) int {
		if e.zset[a] != e.zset[b] {
			if e.zset[a] < e.zset[b] {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	})
}

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

func errArgs(b []byte, cmd string) []byte {
	return resp.AppendError(b, "ERR wrong number of arguments for '"+strings.ToLower(cmd)+"' command")
}

func errOOM(b []byte) []byte {
	return resp.AppendError(b, "OOM command not allowed when used memory > 'maxmemory'")
}

// ── expiries ──────────────────────────────────────────────────────────────

type item struct {
	at  int64
	key string
	gen uint64
}

type expiries []item

func (h expiries) Len() int           { return len(h) }
func (h expiries) Less(i, j int) bool { return h[i].at < h[j].at }
func (h expiries) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiries) Push(x any)        { *h = append(*h, x.(item)) }
func (h *expiries) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}

// ── glob ──────────────────────────────────────────────────────────────────

// match is the server's glob: * ? [set] [^set] [a-z] and \ escapes. It
// walks both strings once and backtracks only to the last *, so a pattern
// built to make a recursive matcher branch at every star (the CVE-2022-36021
// shape against Redis) costs O(len(pattern) * len(key)) here, not more.
func match(p, s string) bool {
	if !strings.ContainsAny(p, `*?[\`) {
		return p == s
	}
	pi, si := 0, 0
	starP, starS := -1, 0 // where the last * was, and where s stood then
	for si < len(s) {
		if pi < len(p) {
			switch p[pi] {
			case '*':
				starP, starS = pi, si
				pi++
				continue
			case '?':
				pi++
				si++
				continue
			case '[':
				if end := strings.IndexByte(p[pi+1:], ']'); end >= 0 {
					set := p[pi+1 : pi+1+end]
					neg := strings.HasPrefix(set, "^")
					if inSet(strings.TrimPrefix(set, "^"), s[si]) != neg {
						pi += end + 2
						si++
						continue
					}
					break // no match here: fall back to the last star
				}
				if s[si] == '[' {
					pi++
					si++
					continue
				}
			case '\\':
				if pi+1 < len(p) && p[pi+1] == s[si] {
					pi += 2
					si++
					continue
				}
			default:
				if p[pi] == s[si] {
					pi++
					si++
					continue
				}
			}
		}
		if starP < 0 {
			return false
		}
		starS++ // let the last * swallow one more byte and try again from there
		pi, si = starP+1, starS
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

func inSet(set string, c byte) bool {
	for i := 0; i < len(set); i++ {
		switch {
		case set[i] == '\\' && i+1 < len(set):
			i++
			if set[i] == c {
				return true
			}
		case i+2 < len(set) && set[i+1] == '-':
			if set[i] <= c && c <= set[i+2] {
				return true
			}
			i += 2
		case set[i] == c:
			return true
		}
	}
	return false
}

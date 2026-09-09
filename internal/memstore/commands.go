package memstore

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// The command table: what run dispatches through, and the string and key
// commands. Sorted sets are in zset.go, SCAN in scan.go, STAMPED in stamps.go.

// command is one table entry: how many arguments it takes after its name,
// and what answers it. fn receives the whole command, name first.
type command struct {
	min, max int // arguments after the name; max -1 is unbounded
	fn       func(*Store, []byte, []string) []byte
}

// commands is filled in init: STAMPED dispatches the write it carries back
// through run, which would make a literal refer to itself.
var commands map[string]command

func init() {
	commands = map[string]command{
		"PING":             {0, 1, (*Store).ping},
		"ECHO":             {1, 1, (*Store).echo},
		"AUTH":             {0, -1, (*Store).ok},
		"SELECT":           {0, -1, (*Store).ok},
		"QUIT":             {0, -1, (*Store).ok},
		"STAMPED":          {2, -1, (*Store).stamped},
		"STAMPOF":          {1, 1, (*Store).stampof},
		"SET":              {2, -1, (*Store).set},
		"SETEX":            {3, 3, (*Store).setex},
		"GET":              {1, 1, (*Store).getCmd},
		"GETDEL":           {1, 1, (*Store).getdel},
		"MGET":             {1, -1, (*Store).mget},
		"DEL":              {1, -1, (*Store).del},
		"EXISTS":           {1, -1, (*Store).exists},
		"TTL":              {1, 1, (*Store).ttl},
		"PTTL":             {1, 1, (*Store).ttl},
		"EXPIRE":           {2, 3, (*Store).expire},
		"TYPE":             {1, 1, (*Store).typ},
		"SCAN":             {1, -1, (*Store).scan},
		"DBSIZE":           {0, 0, (*Store).dbsize},
		"FLUSHALL":         {0, 1, (*Store).flush},
		"FLUSHDB":          {0, 1, (*Store).flush},
		"ZADD":             {3, -1, (*Store).zadd},
		"ZRANGEBYSCORE":    {3, -1, (*Store).zrangebyscore},
		"ZREM":             {2, -1, (*Store).zrem},
		"ZREMRANGEBYSCORE": {3, 3, (*Store).zremrangebyscore},
		"ZREMRANGEBYRANK":  {3, 3, (*Store).zremrangebyrank},
		"ZCARD":            {1, 1, (*Store).zcard},
		"INFO":             {0, -1, (*Store).info},
	}
}

func (s *Store) ping(b []byte, args []string) []byte {
	if len(args) == 2 {
		return resp.AppendBulkString(b, args[1])
	}
	return resp.AppendSimpleString(b, "PONG")
}

func (s *Store) echo(b []byte, args []string) []byte {
	return resp.AppendBulkString(b, args[1])
}

// ok is the answer to what a client says on connect and has no meaning
// here: there is one database and nothing to authenticate against.
func (s *Store) ok(b []byte, args []string) []byte {
	return resp.AppendSimpleString(b, "OK")
}

func (s *Store) stampof(b []byte, args []string) []byte {
	return resp.AppendInt(b, int64(s.stampOf(args[1])))
}

func (s *Store) setex(b []byte, args []string) []byte {
	ttl, err := strconv.Atoi(args[2])
	if err != nil || ttl <= 0 {
		return resp.AppendError(b, "ERR invalid expire time in 'setex' command")
	}
	return s.setString(b, args[1], []byte(args[3]), s.now().Add(time.Duration(ttl)*time.Second))
}

func (s *Store) getCmd(b []byte, args []string) []byte {
	if s.isZSet(args[1]) {
		return wrongType(b)
	}
	return resp.AppendBulk(b, s.get(args[1]))
}

func (s *Store) getdel(b []byte, args []string) []byte {
	if s.isZSet(args[1]) {
		return wrongType(b)
	}
	v := s.get(args[1])
	if e := s.kv[args[1]]; e != nil {
		s.remove(args[1], e)
	}
	return resp.AppendBulk(b, v)
}

func (s *Store) mget(b []byte, args []string) []byte {
	b = resp.AppendArray(b, len(args)-1)
	for _, k := range args[1:] {
		b = resp.AppendBulk(b, s.get(k))
	}
	return b
}

func (s *Store) del(b []byte, args []string) []byte {
	var n int64
	for _, k := range args[1:] {
		if e := s.live(k); e != nil {
			s.remove(k, e)
			n++
		}
	}
	return resp.AppendInt(b, n)
}

func (s *Store) exists(b []byte, args []string) []byte {
	var n int64
	for _, k := range args[1:] {
		if s.live(k) != nil {
			n++
		}
	}
	return resp.AppendInt(b, n)
}

// ttl answers TTL in seconds, rounded, and PTTL in milliseconds: -1 for a
// key that never expires, -2 for none.
func (s *Store) ttl(b []byte, args []string) []byte {
	e := s.live(args[1])
	switch {
	case e == nil:
		return resp.AppendInt(b, -2)
	case e.exp.IsZero():
		return resp.AppendInt(b, -1)
	case strings.EqualFold(args[0], "PTTL"):
		return resp.AppendInt(b, int64(e.exp.Sub(s.now())/time.Millisecond))
	default:
		return resp.AppendInt(b, int64((e.exp.Sub(s.now())+500*time.Millisecond)/time.Second))
	}
}

func (s *Store) typ(b []byte, args []string) []byte {
	return resp.AppendSimpleString(b, typeOf(s.live(args[1])))
}

func (s *Store) dbsize(b []byte, args []string) []byte {
	s.sweep(math.MaxInt)
	return resp.AppendInt(b, int64(len(s.kv)))
}

func (s *Store) flush(b []byte, args []string) []byte {
	for k, e := range s.kv {
		s.remove(k, e)
	}
	for k := range s.tomb {
		s.untomb(k)
	}
	s.exp = s.exp[:0]
	return resp.AppendSimpleString(b, "OK")
}

func (s *Store) info(b []byte, args []string) []byte {
	var expiring int
	for _, e := range s.kv {
		if !e.exp.IsZero() {
			expiring++
		}
	}
	return resp.AppendBulkString(b, fmt.Sprintf("# Server\r\ntritium_version:%s\r\nuptime_in_seconds:%d\r\n"+
		"# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\nmaxmemory_policy:volatile-ttl\r\n"+
		"# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"+
		"# Keyspace\r\ndb0:keys=%d,expires=%d,avg_ttl=0\r\ntombstones:%d\r\n",
		s.version, int64(s.now().Sub(s.started)/time.Second), s.used, s.max, len(s.kv), expiring, len(s.tomb)))
}

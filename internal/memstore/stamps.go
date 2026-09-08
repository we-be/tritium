package memstore

import (
	"strconv"
	"strings"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// Write stamps and tombstones: a write lands only if its stamp is newer than
// the key's last, and a deleted key remembers its stamp so an older write
// arriving late cannot bring it back.

// A tombstone outlives its key by this long: a write stamped before the
// delete can arrive that late from a partition's replay, no later.
const tombstoneTTL = 24 * time.Hour

type tombstone struct {
	stamp uint64
	at    time.Time
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
		if s.isZSet(inner[1]) {
			return wrongType(b)
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

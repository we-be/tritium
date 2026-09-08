package memstore

import (
	"container/heap"
	"math"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// Expiry: a heap of what expires when, swept on time and under memory pressure.

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

func (h expiries) Len() int { return len(h) }

func (h expiries) Less(i, j int) bool { return h[i].at < h[j].at }

func (h expiries) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *expiries) Push(x any) { *h = append(*h, x.(item)) }

func (h *expiries) Pop() any {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}

// ── glob ──────────────────────────────────────────────────────────────────

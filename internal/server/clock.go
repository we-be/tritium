package server

import (
	"hash/fnv"
	"sync"
	"time"
)

// clock stamps writes. A stamp is a hybrid logical clock packed into 64
// bits — 40 of milliseconds since 2026, 12 of a count within the
// millisecond, 12 of this node — so stamps compare as integers, two nodes
// tie only in the same millisecond at the same count with the same low
// bits, and a node that has seen a peer's later stamp stamps later still,
// whatever its own wall clock says. Every node then settles two writes to
// one key the same way: the higher stamp wins, which is the later write as
// far as the fleet's clocks agree.
type clock struct {
	mu   sync.Mutex
	ms   uint64
	seq  uint64
	node uint64
}

const stampEpoch = 1767225600000 // 2026-01-01T00:00:00Z, in milliseconds

func newClock(addr string) *clock {
	h := fnv.New32a()
	h.Write([]byte(addr))
	return &clock{node: uint64(h.Sum32()) & 0xfff}
}

// next is a stamp later than any this node has issued or seen.
func (c *clock) next() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := uint64(max(time.Now().UnixMilli()-stampEpoch, 0))
	if now > c.ms {
		c.ms, c.seq = now, 0
	} else {
		c.seq++
		if c.seq >= 1<<12 {
			c.ms, c.seq = c.ms+1, 0
		}
	}
	return c.ms<<24 | c.seq<<12 | c.node
}

// maxStampAhead is how far past this node's own clock a peer's stamp may
// reach: a stamp written into the far future would win every later write
// to its key for as long as the key lived, and drag this clock along.
const maxStampAhead = uint64(time.Hour / time.Millisecond)

// observe moves the clock past a stamp a peer wrote, so what this node
// writes after seeing it is stamped after it. A stamp more than an hour
// ahead of this node's clock is refused instead: the peer's clock is
// wrong, or the peer is not what it claims.
func (c *clock) observe(stamp uint64) bool {
	ms, seq := stamp>>24, (stamp>>12)&0xfff
	now := uint64(max(time.Now().UnixMilli()-stampEpoch, 0))
	if ms > now+maxStampAhead {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms > c.ms || (ms == c.ms && seq > c.seq) {
		c.ms, c.seq = ms, seq
	}
	return true
}

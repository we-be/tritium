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
	mu    sync.Mutex
	ms    uint64
	seq   uint64
	node  uint64
	ahead uint64 // what observe will reach past this node's clock for
}

const stampEpoch = 1767225600000 // 2026-01-01T00:00:00Z, in milliseconds

func newClock(addr string) *clock {
	h := fnv.New32a()
	h.Write([]byte(addr))
	return &clock{node: uint64(h.Sum32()) & 0xfff, ahead: maxStampAhead}
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

// maxStampAhead is how far past its own clock a node reaches for a peer's
// stamp by default: a stamp written into the far future would win every
// later write to its key for as long as the key lived, and drag this clock
// along.
const maxStampAhead = uint64(time.Hour / time.Millisecond)

// reach sets how far past this node's clock observe will take a peer's
// stamp; maxStampAhead is the default. A test uses it to stand in for the
// skew itself, which it cannot make: no node stamps further ahead than its
// own observe would reach, so the bound has to move instead of the clock.
func (c *clock) reach(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ahead = uint64(d / time.Millisecond)
}

// observe moves the clock past a stamp a peer wrote, so what this node
// writes after seeing it is stamped after it. A stamp further ahead of this
// node's clock than it reaches for is refused instead: the peer's clock is
// wrong, or the peer is not what it claims. The sender holds the replica on
// that refusal and repairs it once the clocks agree, so what it answered
// the client is not lost meanwhile.
func (c *clock) observe(stamp uint64) bool {
	ms, seq := stamp>>24, (stamp>>12)&0xfff
	now := uint64(max(time.Now().UnixMilli()-stampEpoch, 0))
	c.mu.Lock()
	defer c.mu.Unlock()
	if ms > now+c.ahead {
		return false
	}
	if ms > c.ms || (ms == c.ms && seq > c.seq) {
		c.ms, c.seq = ms, seq
	}
	return true
}

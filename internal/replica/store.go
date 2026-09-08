// Package replica is the RESP-backed replicated Store a node writes
// through: its own primary, plus every peer's store as a replica that each
// write is fanned out to — waited on, or fed from a queue — held and
// repaired when it stops answering, and resynced when it comes back.
package replica

import (
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// storage.ErrNotFound is returned by Get for a missing or expired key. Its text is
// part of the wire contract: servers put it in GetReply.Error.

// Store writes through to a primary RESP server and fans every write out to
// replica servers (the primaries of the other nodes in the cluster).
type Store struct {
	primary  *pool
	size     int
	writes   atomic.Int64           // fan-outs so far: the writes this node carried out as owner
	via      Transport              // how replicas are reached; the primary's own by default
	queue    int                    // asynchronous replication: fan-outs a replica may have queued; 0 waits for every replica
	far      func(addr string) bool // replicas fed from a queue of farDepth even when queue is 0: the ones across a link
	farDepth int
	mu       sync.RWMutex
	replicas []*pool
	// holdHook and repairHook let a caller (the cluster's event log) learn
	// of a replica newly held or fully repaired without this package
	// depending on internal/server; see pool.onHold and Repair.
	holdHook   func(addr string)
	repairHook func(addr string, keys int)
	// stamp, when set, marks every string write with STAMPED <n> so it
	// settles the same way on every store (the memstore's rule: the higher
	// stamp wins, a delete leaves a tombstone). Replicas always get the
	// stamped form; the primary only if it understands it — an external
	// store takes the plain write and keeps no stamps.
	stamp         func() uint64
	primaryStamps bool
	// parked is what a replica removed while held still missed, by address:
	// a peer detached during a partition is re-attached when the link is
	// back, and a first-meeting sync only fills its gaps — the keys written
	// meanwhile would otherwise stay stale there until they expire.
	parked map[string]backlog
}

// backlog is what a held replica has missed.
type backlog struct {
	keys    map[string]struct{}
	spilled bool
}

// NewStore connects poolSize connections to the primary at addr, failing
// fast if it is unreachable. The password, if any, is sent as AUTH to the
// primary, and, until SetReplicaTransport says otherwise, to every replica.
func NewStore(addr string, poolSize int, password string) (*Store, error) {
	return NewStoreVia(direct(password), addr, poolSize)
}

// NewStoreTLS is NewStore over TLS: tlsCfg should carry the CA pool to
// verify the primary against (nil RootCAs means the system roots) and its
// ServerName. The dial still honors dialTimeout.
func NewStoreTLS(addr string, poolSize int, password string, tlsCfg *tls.Config) (*Store, error) {
	t := direct(password)
	t.Dial = func(addr string) (net.Conn, error) {
		return tls.DialWithDialer(&net.Dialer{Timeout: dialTimeout}, "tcp", addr, tlsCfg)
	}
	return NewStoreVia(t, addr, poolSize)
}

// NewStoreVia is NewStore over a transport of the caller's: how a node
// reaches the store it embeds, which has no address to dial.
func NewStoreVia(via Transport, addr string, poolSize int) (*Store, error) {
	if poolSize < 1 {
		poolSize = 1
	}
	p, err := newPool(addr, poolSize, via)
	if err != nil {
		return nil, fmt.Errorf("primary: %w", err)
	}
	return &Store{primary: p, size: poolSize, via: via, parked: map[string]backlog{}}, nil
}

// SetReplicaTransport is how replicas added from now on are reached.
func (s *Store) SetReplicaTransport(t Transport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.via = t
}

// SetAsync makes writes return once the primary has them, with replicas
// added from now on fed in order from a queue of up to depth fan-outs;
// zero restores waiting for every replica before answering. Over a slow
// link a write no longer pays the round trip, at the price of a moment in
// which a peer has not seen it yet; a peer that falls depth behind is held
// and repaired like one that stopped answering.
func (s *Store) SetAsync(depth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queue = depth
}

// Async reports whether replicas are fed asynchronously.
func (s *Store) Async() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queue > 0
}

// SetAsyncFor makes the replicas added from now on that far names be fed
// from a queue of depth fan-outs whatever SetAsync says: what a node does
// for a peer on another network, so a write waits on the peers beside it
// and never on one across the internet.
func (s *Store) SetAsyncFor(depth int, far func(addr string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.far, s.farDepth = far, depth
}

// Writes is how many writes this node has carried out as owner — its own
// and the ones forwarded to it — since it started; replicated-in writes
// are not counted.
func (s *Store) Writes() int64 { return s.writes.Load() }

// Queued lists the replicas fed from a queue rather than waited on.
func (s *Store) Queued() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for _, r := range s.replicas {
		if r.queue != nil {
			out = append(out, r.addr)
		}
	}
	return out
}

// SetHoldHook is called for every replica added from now on, once per
// transition into held. See pool.onHold for the calling convention.
func (s *Store) SetHoldHook(fn func(addr string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdHook = fn
}

// SetRepairHook is called after Repair fully replays a held replica's
// backlog, with how many keys were replayed.
func (s *Store) SetRepairHook(fn func(addr string, keys int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repairHook = fn
}

// SetStamper makes every string write carry a stamp from next; primary
// says whether this store's own primary keeps stamps.
func (s *Store) SetStamper(next func() uint64, primary bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stamp, s.primaryStamps = next, primary
}

// Stamps reports whether writes are stamped, and whether the primary keeps
// the stamps (an external store does not).
func (s *Store) Stamps() (on, primary bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stamp != nil, s.primaryStamps
}

// stamped returns a write as the primary and the replicas should see it.
func (s *Store) stamped(args ...string) (local, remote resp.Command) {
	s.mu.RLock()
	next, primary := s.stamp, s.primaryStamps
	s.mu.RUnlock()
	plain := resp.NewCommand(args...)
	if next == nil {
		return plain, plain
	}
	stamped := resp.NewCommand(append([]string{"STAMPED", strconv.FormatUint(next(), 10)}, args...)...)
	if primary {
		return stamped, stamped
	}
	return plain, stamped
}

// Apply runs one write on the primary only — what a peer's replicated
// write becomes here, never fanned out again.
func (s *Store) Apply(cmd resp.Command) (any, error) {
	return s.primary.do(cmd)
}

// Set stores value under key for ttl seconds, then replicates the write.
func (s *Store) Set(key string, value []byte, ttl int) error {
	local, remote := s.stamped("SETEX", key, strconv.Itoa(ttl), string(value))
	v, err := s.primary.do(local)
	if err != nil {
		return fmt.Errorf("primary: %w", err)
	}
	if v != "OK" {
		return fmt.Errorf("primary: unexpected reply %v", v)
	}
	s.replicate(remote)
	return nil
}

// Get returns the value under key, or storage.ErrNotFound.
func (s *Store) Get(key string) ([]byte, error) {
	v, err := s.primary.do(resp.NewCommand("GET", key))
	if err != nil {
		return nil, fmt.Errorf("primary: %w", err)
	}
	switch b := v.(type) {
	case nil:
		return nil, storage.ErrNotFound
	case []byte:
		return b, nil
	default:
		return nil, fmt.Errorf("primary: unexpected reply %T", v)
	}
}

// Delete removes keys and returns how many existed, then replicates.
func (s *Store) Delete(keys ...string) (int64, error) {
	local, remote := s.stamped(append([]string{"DEL"}, keys...)...)
	n, err := s.primary.integer(local)
	if err != nil {
		return 0, err
	}
	s.replicate(remote)
	return n, nil
}

// Query runs one command on the primary without replicating it: reads, or
// writes whose replication the caller decides on after seeing the reply.
func (s *Store) Query(args ...string) (any, error) {
	v, err := s.primary.do(resp.NewCommand(args...))
	if err != nil {
		return v, fmt.Errorf("primary: %w", err)
	}
	return v, nil
}

// Mutate pipelines cmds on the primary, then replicates them all if none
// failed. Replies are returned even when one is a server error.
func (s *Store) Mutate(cmds ...resp.Command) ([]any, error) {
	out, err := s.primary.doAll(cmds)
	if err != nil {
		return out, fmt.Errorf("primary: %w", err)
	}
	s.replicateAll(cmds)
	return out, nil
}

// Replicate fans cmds out to the replicas without touching the primary.
func (s *Store) Replicate(cmds ...resp.Command) {
	s.replicateAll(cmds)
}

// Exists returns how many of keys are present.
func (s *Store) Exists(keys ...string) (int64, error) {
	return s.primary.integer(resp.NewCommand(append([]string{"EXISTS"}, keys...)...))
}

// TTL returns the seconds left on key: -1 for no expiry, -2 for no key.
func (s *Store) TTL(key string) (int64, error) {
	return s.primary.integer(resp.NewCommand("TTL", key))
}

func (s *Store) replicate(cmd resp.Command) {
	s.replicateAll([]resp.Command{cmd})
}

// replicateAll runs cmds on every replica concurrently. Failures are logged,
// not returned: the primary write already succeeded. A replica the transport
// fails to reach is held from then on — its keys are noted for Repair, and
// no write waits on it — while one that answers with an error is merely
// refusing this write.
func (s *Store) replicateAll(cmds []resp.Command) {
	s.writes.Add(1)
	s.mu.RLock()
	replicas := slices.Clone(s.replicas)
	s.mu.RUnlock()

	var wg sync.WaitGroup
	for _, r := range replicas {
		if r.queue != nil {
			r.enqueue(cmds)
			continue
		}
		wg.Go(func() {
			if r.isHeld() {
				r.hold(cmds)
				return
			}
			r.send(cmds)
		})
	}
	wg.Wait()
}

// AddReplica starts fanning writes out to addr. Adding an address twice is a no-op.
func (s *Store) AddReplica(addr string) error {
	if s.hasReplica(addr) {
		return nil
	}
	s.mu.RLock()
	via := s.via
	s.mu.RUnlock()
	p, err := newPool(addr, s.size, via)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slices.ContainsFunc(s.replicas, func(r *pool) bool { return r.addr == addr }) {
		p.close()
		return nil
	}
	p.onHold = s.holdHook
	if s.queue > 0 {
		p.async(s.queue)
	} else if s.far != nil && s.far(addr) {
		p.async(s.farDepth)
	}
	if b, ok := s.parked[addr]; ok { // back from a partition: held until its backlog is replayed
		p.held, p.missed, p.spilled = true, b.keys, b.spilled
		delete(s.parked, addr)
	}
	s.replicas = append(s.replicas, p)
	return nil
}

// RemoveReplica stops replicating to addr and reports whether it was known.
func (s *Store) RemoveReplica(addr string) bool {
	s.mu.Lock()
	i := slices.IndexFunc(s.replicas, func(r *pool) bool { return r.addr == addr })
	var p *pool
	if i >= 0 {
		p = s.replicas[i]
		s.replicas = slices.Delete(s.replicas, i, i+1)
		p.mu.Lock()
		if p.held {
			s.parked[addr] = backlog{p.missed, p.spilled}
		}
		p.mu.Unlock()
	}
	s.mu.Unlock()
	if p == nil {
		return false
	}
	p.close()
	return true
}

// Replicas lists the replica addresses currently receiving writes.
func (s *Store) Replicas() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.replicas))
	for i, r := range s.replicas {
		out[i] = r.addr
	}
	return out
}

// Held lists the replicas that stopped answering and are awaiting a repair.
func (s *Store) Held() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for _, r := range s.replicas {
		if r.isHeld() {
			out = append(out, r.addr)
		}
	}
	return out
}

func (s *Store) hasReplica(addr string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.ContainsFunc(s.replicas, func(r *pool) bool { return r.addr == addr })
}

// Close drops every pooled connection. In-flight operations finish on their
// own connection and are then discarded.
func (s *Store) Close() error {
	s.primary.close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.replicas {
		r.close()
	}
	s.replicas = nil
	return nil
}

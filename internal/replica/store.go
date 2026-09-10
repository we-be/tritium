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

	"github.com/we-be/tritium/internal/config"
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
	// relayVia names the replica that carries writes on to peers this node
	// cannot reach itself — a hub both can reach — and relayTargets says
	// which those are as each batch goes out; none means a plain write.
	relayVia     string
	relayTargets func() []string
	mu           sync.RWMutex
	replicas     []*pool
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
	// rights answers what the replica at an address may hold, so a peer this
	// node holds to rights on the accept side is held to the same ones on
	// the send side. nil, and a nil answer, mean every key.
	rights func(addr string) *config.Rights
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

// SetReplicaRights is how the fan-out learns what each replica may hold: a
// write whose key a replica's read rights do not name is not sent to it, and
// a DEL naming several is cut down to the ones it may. fn is asked per batch,
// so a peer that says who it is after it was attached is still held to it.
func (s *Store) SetReplicaRights(fn func(addr string) *config.Rights) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rights = fn
}

// Withheld is, per replica, how many fan-outs it did not get in full because
// its rights do not name the keys.
func (s *Store) Withheld() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.replicas))
	for _, r := range s.replicas {
		out[r.addr] = r.withheld.Load()
	}
	return out
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

// SetRelay makes the replica added later at via carry writes on to the
// peers targets names — the ones this node cannot deliver to itself — by
// prefixing what it sends via with RELAY n addr…; targets is consulted per
// batch, so a peer that comes back within reach drops out on its own. Call
// it before replicas are added, like SetAsyncFor.
func (s *Store) SetRelay(via string, targets func() []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.relayVia, s.relayTargets = via, targets
}

// relayDecorate prefixes a batch with the relay targets of the moment.
func (s *Store) relayDecorate(cmds []resp.Command) []resp.Command {
	s.mu.RLock()
	targets := s.relayTargets
	s.mu.RUnlock()
	if targets == nil {
		return cmds
	}
	to := targets()
	if len(to) == 0 {
		return cmds
	}
	args := append([]string{"RELAY", strconv.Itoa(len(to))}, to...)
	out := make([]resp.Command, len(cmds))
	for i, c := range cmds {
		out[i] = prefixed(c, args)
	}
	return out
}

// prefixed puts args in front of cmd's own.
func prefixed(cmd resp.Command, args []string) resp.Command {
	for i := len(args) - 1; i >= 0; i-- {
		cmd = resp.Prefix(cmd, args[i])
	}
	return cmd
}

// ReplicateTo sends cmds to the replica at addr alone — what a node does
// with a write it was asked to relay — from a queue when that replica is
// fed from one, else in the background; a held replica notes them for its
// repair. An address that is not a replica is ignored.
func (s *Store) ReplicateTo(addr string, cmds ...resp.Command) {
	s.mu.RLock()
	i := slices.IndexFunc(s.replicas, func(r *pool) bool { return r.addr == addr })
	var r *pool
	if i >= 0 {
		r = s.replicas[i]
	}
	s.mu.RUnlock()
	if r == nil {
		return
	}
	if r.queue != nil {
		r.deliver(cmds) // in order: a queued replica's batches must not race each other
		return
	}
	go r.deliver(cmds)
}

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
	_, err := s.set(key, value, ttl, "")
	return err
}

// set writes key on the primary and fans it out to every replica but
// except, returning the form the replicas got.
func (s *Store) set(key string, value []byte, ttl int, except string) (resp.Command, error) {
	local, remote := s.stamped("SETEX", key, strconv.Itoa(ttl), string(value))
	v, err := s.primary.do(local)
	if err != nil {
		return nil, fmt.Errorf("primary: %w", err)
	}
	if v != "OK" {
		return nil, fmt.Errorf("primary: unexpected reply %v", v)
	}
	s.replicateAllExcept([]resp.Command{remote}, except)
	return remote, nil
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
	n, _, err := s.delete(keys, "")
	return n, err
}

// delete removes keys on the primary and fans the deletion out to every
// replica but except, returning the form the replicas got.
func (s *Store) delete(keys []string, except string) (int64, resp.Command, error) {
	local, remote := s.stamped(append([]string{"DEL"}, keys...)...)
	n, err := s.primary.integer(local)
	if err != nil {
		return 0, nil, err
	}
	s.replicateAllExcept([]resp.Command{remote}, except)
	return n, remote, nil
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
	return s.mutate(cmds, "")
}

// mutate runs cmds on the primary and fans them out, as they are, to every
// replica but except.
func (s *Store) mutate(cmds []resp.Command, except string) ([]any, error) {
	out, err := s.primary.doAll(cmds)
	if err != nil {
		return out, fmt.Errorf("primary: %w", err)
	}
	s.replicateAllExcept(cmds, except)
	return out, nil
}

// Forwarded is the Store as a write another node forwarded here sees it:
// the write lands on the primary and fans out to every replica but the
// node that forwarded, which applies what Sent reports itself — so a
// forward costs one round trip, not the forward and then the fan-out back.
type Forwarded struct {
	s    *Store
	from string
	sent []resp.Command
}

// Forwarded is this store as seen from a write the node at from forwarded.
func (s *Store) Forwarded(from string) *Forwarded { return &Forwarded{s: s, from: from} }

func (f *Forwarded) Set(key string, value []byte, ttl int) error {
	remote, err := f.s.set(key, value, ttl, f.from)
	if err == nil {
		f.sent = append(f.sent, remote)
	}
	return err
}

func (f *Forwarded) Delete(keys ...string) (int64, error) {
	n, remote, err := f.s.delete(keys, f.from)
	if err == nil {
		f.sent = append(f.sent, remote)
	}
	return n, err
}

func (f *Forwarded) Mutate(cmds ...resp.Command) ([]any, error) {
	out, err := f.s.mutate(cmds, f.from)
	if err == nil {
		f.sent = append(f.sent, cmds...)
	}
	return out, err
}

func (f *Forwarded) Replicate(cmds ...resp.Command) {
	f.sent = append(f.sent, cmds...)
	f.s.replicateAllExcept(cmds, f.from)
}

func (f *Forwarded) Query(args ...string) (any, error) { return f.s.Query(args...) }

// Sent is what went to the other replicas, for the forwarder to apply.
func (f *Forwarded) Sent() []resp.Command { return f.sent }

// Replicate fans cmds out to the replicas without touching the primary.
func (s *Store) Replicate(cmds ...resp.Command) {
	s.replicateAllExcept(cmds, "")
}

// Exists returns how many of keys are present.
func (s *Store) Exists(keys ...string) (int64, error) {
	return s.primary.integer(resp.NewCommand(append([]string{"EXISTS"}, keys...)...))
}

// TTL returns the seconds left on key: -1 for no expiry, -2 for no key.
func (s *Store) TTL(key string) (int64, error) {
	return s.primary.integer(resp.NewCommand("TTL", key))
}

// replicateAllExcept runs cmds on every replica concurrently but the one
// at except — the node a forwarded write came from, which applies it
// itself; "" leaves nobody out. Failures are logged, not returned: the
// primary write already succeeded. A replica the transport fails to reach
// is held from then on — its keys are noted for Repair, and no write waits
// on it — while one that answers with an error is merely refusing this
// write.
func (s *Store) replicateAllExcept(cmds []resp.Command, except string) {
	s.writes.Add(1)
	s.mu.RLock()
	replicas := slices.Clone(s.replicas)
	s.mu.RUnlock()

	var wg sync.WaitGroup
	for _, r := range replicas {
		if r.addr == except {
			continue
		}
		if r.queue != nil {
			r.deliver(cmds)
			continue
		}
		wg.Go(func() { r.deliver(cmds) })
	}
	wg.Wait()
}

// AddReplica starts fanning writes out to addr. Adding an address twice is a no-op.
func (s *Store) AddReplica(addr string) error {
	if s.hasReplica(addr) {
		return nil
	}
	s.mu.RLock()
	via, rights := s.via, s.rights
	s.mu.RUnlock()
	p, err := newPool(addr, s.size, via)
	if err != nil {
		return err
	}
	if rights != nil {
		p.rights = func() *config.Rights { return rights(addr) }
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slices.ContainsFunc(s.replicas, func(r *pool) bool { return r.addr == addr }) {
		p.close()
		return nil
	}
	p.onHold = s.holdHook
	if addr == s.relayVia {
		p.decorate = s.relayDecorate
	}
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

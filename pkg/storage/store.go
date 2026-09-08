package storage

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// ErrNotFound is returned by Get for a missing or expired key. Its text is
// part of the wire contract: servers put it in GetReply.Error.
var ErrNotFound = errors.New("key not found")

const (
	dialTimeout    = 5 * time.Second
	primaryTimeout = 5 * time.Second // one round trip to our own store
	replicaTimeout = 2 * time.Second // one batch to a peer: a peer that accepts and never answers must not stall our writes (chaos, 2026-09-06)
)

// conn is a pooled connection with its own buffered reader, so bytes buffered
// past one reply can never leak to the next user of the connection.
type conn struct {
	net.Conn
	r *resp.Reader
}

// Transport is how a pool reaches its server: a dialer, the AUTH to send on
// a fresh connection (nil for none), and a command to wrap every write in.
// The primary is reached directly with the store password; replicas are
// reached however the owner says — a tritium node reaches its peers' nodes
// over TLS as the peer user and wraps writes in TRITIUM.REPLICATE, so a
// peer's store never has to be reachable from anywhere but its own node.
type Transport struct {
	Dial    func(addr string) (net.Conn, error)
	Auth    resp.Command
	Wrap    string
	Timeout time.Duration // per batch, write plus every reply; zero means primaryTimeout
}

func direct(password string) Transport {
	t := Transport{Dial: func(addr string) (net.Conn, error) { return net.DialTimeout("tcp", addr, dialTimeout) }, Timeout: primaryTimeout}
	if password != "" {
		t.Auth = resp.NewCommand("AUTH", password)
	}
	return t
}

// pool is a fixed-size pool of connections to one RESP server. A slot holds
// nil after a transport error and is redialed on next use. Once closed it
// answers every get with an error at once: a fan-out that was in flight to
// a peer as it was detached used to block on an empty pool forever, and
// every write on the node with it (found by the chaos test).
type pool struct {
	addr  string
	via   Transport
	slots chan *conn
	done  chan struct{}
	once  sync.Once

	// A replica that failed a write is held: writes to it are noted by key
	// and not attempted until a repair replays them. Otherwise every write
	// waits out the deadline on a peer that is frozen or gone, and what it
	// missed before the cluster noticed is never sent again.
	mu      sync.Mutex
	held    bool
	missed  map[string]struct{}
	spilled bool // more keys than missed may hold: the repair copies everything

	// Under asynchronous replication writes are queued here and sent by one
	// writer goroutine, in order, coalesced into batches; nil means every
	// write waits for this replica's answer.
	queue chan job

	// onHold, when set, is called once on the transition into held — not on
	// every write while it stays held. It runs synchronously under mu, so it
	// must never block: the event log's emit is a non-blocking channel send.
	onHold func(addr string)
}

// job is one fan-out on an asynchronous replica's queue, or, with no
// commands, a flush: done is closed once everything queued before it has
// been dealt with.
type job struct {
	cmds []resp.Command
	done chan struct{}
}

// missedCap bounds what a held replica remembers; past it the repair is a
// full copy instead of a replay.
const missedCap = 10000

var errPoolClosed = errors.New("connection pool closed")

func newPool(addr string, size int, via Transport) (*pool, error) {
	p := &pool{addr: addr, via: via, slots: make(chan *conn, size), done: make(chan struct{})}
	for range size {
		c, err := p.dial()
		if err != nil {
			p.close()
			return nil, err
		}
		p.slots <- c
	}
	return p, nil
}

func (p *pool) dial() (*conn, error) {
	c, err := p.via.Dial(p.addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}
	pc := &conn{Conn: c, r: resp.NewReader(c)}
	if p.via.Auth != nil {
		if _, err := p.via.Auth.Do(pc, pc.r); err != nil {
			c.Close()
			return nil, fmt.Errorf("auth %s: %w", p.addr, err)
		}
	}
	return pc, nil
}

func (p *pool) get() (*conn, error) {
	var c *conn
	select {
	case c = <-p.slots:
	case <-p.done:
		return nil, errPoolClosed
	}
	if c != nil {
		return c, nil
	}
	c, err := p.dial()
	if err != nil {
		p.slots <- nil
		return nil, err
	}
	return c, nil
}

// put returns c to the pool, or drops it if the last operation broke it. A
// closed pool drops it either way.
func (p *pool) put(c *conn, err error) {
	if err != nil {
		c.Close()
		c = nil
	}
	select {
	case <-p.done:
		if c != nil {
			c.Close()
		}
	default:
		p.slots <- c
	}
}

// async makes this replica's writes queue up to depth fan-outs and returns
// to the caller at once; the writer sends them in order.
func (p *pool) async(depth int) {
	p.queue = make(chan job, depth)
	go p.writer()
}

// enqueue hands a fan-out to the writer. A full queue means the peer is not
// keeping up: the replica is held and the repair catches it up.
func (p *pool) enqueue(cmds []resp.Command) {
	if p.isHeld() {
		p.hold(cmds)
		return
	}
	select {
	case p.queue <- job{cmds: cmds}:
	default:
		p.hold(cmds)
		slog.Warn("replica queue full, holding writes for it until a repair", "addr", p.addr)
	}
}

// flush waits until everything queued so far has been sent or held, so a
// repair never lands under an older queued write.
func (p *pool) flush() {
	done := make(chan struct{})
	select {
	case p.queue <- job{done: done}:
	case <-p.done:
		return
	}
	select {
	case <-done:
	case <-p.done:
	}
}

// writer drains the queue, coalescing whatever is waiting into one batch.
// A held replica's queue is noted, not sent: the repair replays it.
func (p *pool) writer() {
	for {
		var j job
		select {
		case <-p.done:
			return
		case j = <-p.queue:
		}
		var batch []resp.Command
		var flushes []chan struct{}
		for {
			batch = append(batch, j.cmds...)
			if j.done != nil {
				flushes = append(flushes, j.done)
			}
			if len(batch) >= 256 {
				break
			}
			select {
			case j = <-p.queue:
				continue
			default:
			}
			break
		}
		if len(batch) > 0 {
			if p.isHeld() {
				p.hold(batch)
			} else {
				p.send(batch)
			}
		}
		for _, f := range flushes {
			close(f)
		}
	}
}

// send runs one fan-out on the replica. A transport failure holds it; an
// error reply is the peer refusing this write and nothing more.
func (p *pool) send(cmds []resp.Command) {
	_, err := p.doAll(cmds)
	if err == nil {
		return
	}
	var se *resp.ServerError
	if errors.As(err, &se) {
		slog.Warn("replica rejected write", "addr", p.addr, "err", err)
		return
	}
	p.hold(cmds)
	slog.Warn("replica write failed, holding writes for it until a repair", "addr", p.addr, "err", err)
}

// isHeld reports whether writes to this replica are being noted, not sent.
func (p *pool) isHeld() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held
}

// hold stops sending to this replica and notes the keys of cmds as missed.
func (p *pool) hold(cmds []resp.Command) {
	p.mu.Lock()
	defer p.mu.Unlock()
	wasHeld := p.held
	p.held = true
	if !wasHeld && p.onHold != nil {
		p.onHold(p.addr)
	}
	if p.spilled {
		return
	}
	if p.missed == nil {
		p.missed = map[string]struct{}{}
	}
	for _, c := range cmds {
		for _, k := range keysOf(c) {
			p.missed[k] = struct{}{}
		}
	}
	if len(p.missed) > missedCap {
		p.missed, p.spilled = nil, true
	}
}

// forget drops keys from the missed set and releases the replica once
// nothing is missing.
func (p *pool) forget(keys []string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range keys {
		delete(p.missed, k)
	}
	if len(p.missed) == 0 && !p.spilled {
		p.held, p.missed = false, nil
		return true
	}
	return false
}

// keysOf names the keys a replicated write touches.
func keysOf(cmd resp.Command) []string {
	args, err := resp.NewReader(bytes.NewReader(cmd)).ReadCommand()
	if err != nil {
		return nil
	}
	if len(args) > 2 && strings.EqualFold(args[0], "STAMPED") { // the stamp is not a key
		args = args[2:]
	}
	if len(args) < 2 {
		return nil
	}
	if strings.EqualFold(args[0], "DEL") {
		return args[1:]
	}
	return args[1:2]
}

func (p *pool) close() {
	p.once.Do(func() { close(p.done) })
	for {
		select {
		case c := <-p.slots:
			if c != nil {
				c.Close()
			}
		default:
			return
		}
	}
}

// do runs cmd on a pooled connection. A server error reply leaves the
// connection healthy; a transport error retires it.
func (p *pool) do(cmd resp.Command) (any, error) {
	out, err := p.doAll([]resp.Command{cmd})
	if out == nil {
		return nil, err
	}
	return out[0], err
}

// doAll pipelines cmds on one connection. Every reply is returned; the
// first server error, if any, is the error. A transport error retires the
// connection; if it struck before any reply came back the batch is tried
// once more on a fresh connection — a pooled connection to a peer that
// restarted is dead on first use, and every write here is idempotent.
func (p *pool) doAll(cmds []resp.Command) ([]any, error) {
	var buf []byte
	for _, cmd := range cmds {
		if p.via.Wrap != "" {
			cmd = resp.Prefix(cmd, p.via.Wrap)
		}
		buf = append(buf, cmd...)
	}
	// every pooled connection may be dead at once (the peer restarted), and each
	// failure retires one, so one attempt per slot plus a fresh dial covers it
	for attempt := 1; ; attempt++ {
		c, err := p.get()
		if err != nil {
			return nil, err
		}
		out, first, err := p.exchange(c, buf, len(cmds))
		if err == nil {
			return out, first
		}
		var ne net.Error
		if out != nil || attempt > cap(p.slots) || (errors.As(err, &ne) && ne.Timeout()) {
			// replies were read, a fresh connection failed too, or the server is there
			// but not answering: retrying that would only stall the caller longer
			return nil, err
		}
	}
}

// exchange writes one batch and reads its replies under one deadline. out
// is nil when the transport failed before the first reply, so the caller
// knows nothing was applied on that connection.
func (p *pool) exchange(c *conn, buf []byte, n int) (out []any, first, err error) {
	timeout := p.via.Timeout
	if timeout <= 0 {
		timeout = primaryTimeout
	}
	c.SetDeadline(time.Now().Add(timeout))
	defer c.SetDeadline(time.Time{})
	if _, err := c.Write(buf); err != nil {
		p.put(c, err)
		return nil, nil, fmt.Errorf("write: %w", err)
	}
	for i := range n {
		v, err := c.r.ReadValue()
		if err != nil && !errors.As(err, new(*resp.ServerError)) {
			p.put(c, err)
			if i == 0 {
				return nil, nil, err
			}
			return []any{}, nil, err
		}
		if out == nil {
			out = make([]any, n)
		}
		if err != nil && first == nil {
			first = err
		}
		out[i] = v
	}
	p.put(c, nil)
	return out, first, nil
}

// Store writes through to a primary RESP server and fans every write out to
// replica servers (the primaries of the other nodes in the cluster).
type Store struct {
	primary  *pool
	size     int
	via      Transport // how replicas are reached; the primary's own by default
	queue    int       // asynchronous replication: fan-outs a replica may have queued; 0 waits for every replica
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

// Get returns the value under key, or ErrNotFound.
func (s *Store) Get(key string) ([]byte, error) {
	v, err := s.primary.do(resp.NewCommand("GET", key))
	if err != nil {
		return nil, fmt.Errorf("primary: %w", err)
	}
	switch b := v.(type) {
	case nil:
		return nil, ErrNotFound
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

// integer runs cmd on the primary and expects an integer reply.
func (p *pool) integer(cmd resp.Command) (int64, error) {
	v, err := p.do(cmd)
	if err != nil {
		return 0, fmt.Errorf("primary: %w", err)
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("primary: unexpected reply %T", v)
	}
	return n, nil
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

// Repair replays, on every replica held after a failed write, the keys it
// missed meanwhile — our current copy of each, or its deletion — and
// releases it once nothing is missing. Meant for a periodic tick: a replica
// that still does not answer stays held for the next one. Returns the keys
// replayed.
func (s *Store) Repair() int {
	s.mu.RLock()
	replicas := slices.Clone(s.replicas)
	hook := s.repairHook
	s.mu.RUnlock()
	total := 0
	for _, r := range replicas {
		if !r.isHeld() {
			continue
		}
		n, err := s.repair(r)
		total += n
		if err != nil {
			slog.Debug("replica repair failed, still held", "addr", r.addr, "keys", n, "err", err)
			continue
		}
		slog.Info("replica repaired", "addr", r.addr, "keys", n)
		if hook != nil && n > 0 {
			hook(r.addr, n)
		}
	}
	return total
}

func (s *Store) repair(p *pool) (int, error) {
	if p.queue != nil {
		p.flush() // whatever was queued before the hold is noted now, never sent after the replay
	}
	p.mu.Lock()
	spilled := p.spilled
	p.mu.Unlock()
	if spilled {
		n, err := s.Sync(p.addr, true)
		if err != nil {
			return n, err
		}
		p.mu.Lock()
		p.held, p.missed, p.spilled = false, nil, false
		p.mu.Unlock()
		return n, nil
	}
	n := 0
	for range 5 { // writes noted during a round are replayed by the next; a few rounds, then the next tick
		p.mu.Lock()
		keys := slices.Collect(maps.Keys(p.missed))
		p.mu.Unlock()
		for batch := range slices.Chunk(keys, 200) {
			cmds, _, err := s.copyCommands(batch, true, true)
			if err != nil {
				return n, err
			}
			if len(cmds) > 0 {
				if _, err := p.doAll(cmds); err != nil {
					var se *resp.ServerError
					if !errors.As(err, &se) {
						return n, err
					}
				}
			}
			n += len(batch)
		}
		if p.forget(keys) {
			return n, nil
		}
	}
	return n, nil
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
	}
	if b, ok := s.parked[addr]; ok { // back from a partition: held until its backlog is replayed
		p.held, p.missed, p.spilled = true, b.keys, b.spilled
		delete(s.parked, addr)
	}
	s.replicas = append(s.replicas, p)
	return nil
}

// Sync copies every key on the primary to the store at addr — a peer back
// from an outage missed every write made meanwhile, and a newcomer holds
// nothing. With overwrite the primary's copy wins (the peer was down, so ours
// is the newer one); without it only keys the peer lacks are filled, so a
// node that just started never clobbers what the survivors hold. Best
// effort, a SCAN page at a time — three pipelined round trips per page, not
// per key — returning the keys copied and the first error.
func (s *Store) Sync(addr string, overwrite bool) (int, error) {
	s.mu.RLock()
	via := s.via
	s.mu.RUnlock()
	dst, err := newPool(addr, 1, via)
	if err != nil {
		return 0, err
	}
	defer dst.close()
	n, cursor := 0, "0"
	var first error
	for {
		v, err := s.primary.do(resp.NewCommand("SCAN", cursor, "COUNT", "200"))
		if err != nil {
			return n, err
		}
		page, ok := v.([]any)
		if !ok || len(page) != 2 {
			return n, fmt.Errorf("unexpected SCAN reply %T", v)
		}
		next, _ := page[0].([]byte)
		raw, _ := page[1].([]any)
		keys := make([]string, 0, len(raw))
		for _, k := range raw {
			key, _ := k.([]byte)
			keys = append(keys, string(key))
		}
		cmds, copied, err := s.copyCommands(keys, overwrite, false)
		if err != nil {
			return n, err
		}
		if len(cmds) > 0 {
			if _, err := dst.doAll(cmds); err != nil && first == nil {
				first = err
			}
		}
		n += copied
		cursor = string(next)
		if cursor == "0" {
			return n, first
		}
	}
}

// copyCommands is what recreates keys elsewhere with their remaining TTL,
// read in two pipelined round trips: every key's type and TTL, then every
// value. Keys that are gone or of a kind tritium does not write are
// skipped — or, with replay, a gone key becomes a DEL, since a replay is
// of writes the other side missed and one of them may have been the
// delete. With overwrite the copy is exact: a sorted set is rebuilt from
// scratch so members removed here go there too. Returns the commands and
// how many keys they cover.
func (s *Store) copyCommands(keys []string, overwrite, replay bool) ([]resp.Command, int, error) {
	if len(keys) == 0 {
		return nil, 0, nil
	}
	_, stamps := s.Stamps()
	per := 2
	if stamps {
		per = 3
	}
	probe := make([]resp.Command, 0, per*len(keys))
	for _, k := range keys {
		probe = append(probe, resp.NewCommand("TYPE", k), resp.NewCommand("TTL", k))
		if stamps {
			probe = append(probe, resp.NewCommand("STAMPOF", k))
		}
	}
	replies, err := s.primary.doAll(probe)
	if err != nil {
		return nil, 0, err
	}
	type want struct {
		key, typ string
		ttl      int64
		stamp    uint64
	}
	var wants []want
	var reads []resp.Command
	var out []resp.Command
	for i, k := range keys {
		typ, _ := replies[per*i].(string)
		ttl, _ := replies[per*i+1].(int64)
		var stamp uint64
		if stamps {
			n, _ := replies[per*i+2].(int64)
			stamp = uint64(n)
		}
		switch {
		case typ == "none" && replay:
			// a delete the other side missed: with its stamp, so it also beats an older write that reaches there later
			if stamp > 0 {
				out = append(out, resp.NewCommand("STAMPED", strconv.FormatUint(stamp, 10), "DEL", k))
			} else {
				out = append(out, resp.NewCommand("DEL", k))
			}
		case ttl <= 0: // gone, or a key without an expiry: not ours
		case typ == "string":
			wants = append(wants, want{k, typ, ttl, stamp})
			reads = append(reads, resp.NewCommand("GET", k))
		case typ == "zset":
			wants = append(wants, want{k, typ, ttl, 0})
			reads = append(reads, resp.NewCommand("ZRANGEBYSCORE", k, "-inf", "+inf", "WITHSCORES"))
		}
	}
	copied := len(out)
	if len(reads) == 0 {
		return out, copied, nil
	}
	values, err := s.primary.doAll(reads)
	if err != nil {
		return nil, 0, err
	}
	for i, w := range wants {
		exp := strconv.FormatInt(w.ttl, 10)
		switch w.typ {
		case "string":
			val, ok := values[i].([]byte)
			if !ok {
				continue // expired between the two reads
			}
			args := []string{"SET", w.key, string(val), "EX", exp}
			switch {
			case w.stamp > 0: // the stamp decides there, whichever side wrote last
				args = append([]string{"STAMPED", strconv.FormatUint(w.stamp, 10)}, args...)
			case !overwrite:
				args = append(args, "NX")
			}
			out = append(out, resp.NewCommand(args...))
		case "zset":
			pairs, _ := values[i].([]any)
			if len(pairs) < 2 {
				continue
			}
			args := []string{"ZADD", w.key}
			if !overwrite {
				args = append(args, "NX")
			}
			for j := 0; j+1 < len(pairs); j += 2 {
				member, _ := pairs[j].([]byte)
				score, _ := pairs[j+1].([]byte)
				args = append(args, string(score), string(member))
			}
			expire := []string{"EXPIRE", w.key, exp}
			if overwrite {
				out = append(out, resp.NewCommand("DEL", w.key))
			} else {
				expire = append(expire, "GT")
			}
			out = append(out, resp.NewCommand(args...), resp.NewCommand(expire...))
		}
		copied++
	}
	return out, copied, nil
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

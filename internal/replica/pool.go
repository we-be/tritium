package replica

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
)

// A pool is the connections to one RESP server — the primary, or one
// peer's store reached through its node — and what a replica remembers
// while it is held: the keys it missed, or that there were too many.

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
	// decorate, when set, rewrites a batch as it goes out — the store uses
	// it to ask a hub to carry the write on to peers this node cannot reach
	// — while what a hold notes stays the plain command.
	decorate func([]resp.Command) []resp.Command

	mu      sync.Mutex
	held    bool
	missed  map[string]struct{}
	spilled bool // more keys than missed may hold: the repair copies everything

	// Under asynchronous replication writes are queued here and sent by one
	// writer goroutine, in order, coalesced into batches; nil means every
	// write waits for this replica's answer.
	queue chan job

	// rights, when set, answers what this replica may hold; a nil answer is
	// every key. It is asked per batch rather than read once at attach,
	// since a peer says who it is when it announces itself, which may be
	// after we have already attached it.
	rights   func() *config.Rights
	withheld atomic.Int64 // fan-outs this replica did not get in full, its rights not naming the keys

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
	out := cmds
	if p.decorate != nil {
		out = p.decorate(cmds)
	}
	_, err := p.doAll(out)
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
	return KeysOf(args)
}

// KeysOf names the keys a replicated write touches, stamp or not: every
// argument of a DEL, the first of anything else.
func KeysOf(args []string) []string {
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

// deliver hands one fan-out to this replica: queued where it is fed from a
// queue, noted where it is held, sent otherwise. It is the one door a batch
// comes in by, so it is the one place the replica's rights are applied.
func (p *pool) deliver(cmds []resp.Command) {
	if cmds = p.scope(cmds); len(cmds) == 0 {
		return
	}
	if p.queue != nil {
		p.enqueue(cmds)
		return
	}
	if p.isHeld() {
		p.hold(cmds)
		return
	}
	p.send(cmds)
}

// scope cuts a batch down to what this replica may hold. What is dropped
// here is never held either: a repair replays what a peer missed, and a key
// its rights do not name is not something it missed.
func (p *pool) scope(cmds []resp.Command) []resp.Command {
	if p.rights == nil {
		return cmds
	}
	r := p.rights()
	if r == nil {
		return cmds
	}
	out := make([]resp.Command, 0, len(cmds))
	for _, c := range cmds {
		kept, whole := scoped(c, r)
		if kept != nil {
			out = append(out, kept)
		}
		if !whole {
			p.withheld.Add(1)
		}
	}
	return out
}

// scopeKeys is keys cut down to the ones this replica may hold: what a
// resync copies to it, and what a repair replays. keys itself is left alone,
// since a repair still forgets every key it noted — one outside the
// replica's rights is not a key it is waiting for.
func (p *pool) scopeKeys(keys []string) []string {
	if p.rights == nil {
		return keys
	}
	r := p.rights()
	if r == nil {
		return keys
	}
	return slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return !r.MayRead(k) })
}

// scoped is cmd as a replica holding rights should see it, and whether the
// replica gets the whole of it: cmd itself when every key it names is one
// the replica may hold, a DEL cut down to the ones it may, and nil when
// nothing is left — or when the keys cannot be named at all, since a right
// that cannot be checked is a right that is refused.
func scoped(cmd resp.Command, r *config.Rights) (resp.Command, bool) {
	args, err := resp.NewReader(bytes.NewReader(cmd)).ReadCommand()
	if err != nil {
		return nil, false
	}
	var stamp []string
	if len(args) > 2 && strings.EqualFold(args[0], "STAMPED") {
		stamp, args = args[:2], args[2:]
	}
	if len(args) < 2 {
		return nil, false
	}
	if !strings.EqualFold(args[0], "DEL") {
		if r.MayRead(args[1]) {
			return cmd, true
		}
		return nil, false
	}
	keep := make([]string, 1, len(args))
	keep[0] = args[0]
	for _, k := range args[1:] {
		if r.MayRead(k) {
			keep = append(keep, k)
		}
	}
	switch len(keep) {
	case 1:
		return nil, false
	case len(args):
		return cmd, true
	}
	return resp.NewCommand(slices.Concat(stamp, keep)...), false
}

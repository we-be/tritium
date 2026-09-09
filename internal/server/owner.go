package server

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/replica"
	"github.com/we-be/tritium/internal/resp"
)

// Every key has one owner among the live nodes, picked by rendezvous
// hashing over their addresses weighted by each node's electronegativity —
// a node competes with as many points as its weight, so one of weight 2
// owns twice the keys of one of weight 1, and one of weight 0, the cloud
// hub that carries replicas and orders nothing, owns none — and a write to
// a key is carried out by its owner: a node handed a client's write for a key it does not own forwards
// it there as TRITIUM.FORWARD, and the owner applies it and fans it out to
// everyone, the sender included, before answering. So the writes to one key
// are ordered in one place, and SET NX means what it says across the
// cluster instead of per node. Reads stay local.
//
// A node that cannot be reached is no owner: when the forward fails the
// write takes the old path — applied here, fanned out from here — and once
// the peer is held or gone the hash no longer picks it. That window, and
// the moment two nodes' views of the membership differ, is where two
// writers can still both win an NX; it lasts a replication timeout, not a
// key's lifetime. With REPLICATION=async the owner answers before its
// fan-out lands, so a client reading its own write back on another node
// may be a moment early.

const forwardTimeout = 2 * time.Second // like a replica batch: an owner that accepts and never answers must not stall a client longer

// owned are the single-key writes a node forwards to the key's owner; DEL
// is split by key in its own handler.
var owned = map[string]bool{"SET": true, "SETEX": true, "GETDEL": true, "EXPIRE": true, "ZADD": true, "ZREM": true, "ZREMRANGEBYSCORE": true}

// ownerOf names the node that orders writes to key: "" when it is this one.
// Held replicas are passed over — a forward to one would only wait out the
// deadline — so during a hold the sides may briefly disagree. So are nodes
// of weight 0; if no live node has a weight, the write stays here. And so
// is a peer served over connections it opened: its fan-out reaches only
// what it can dial, and a write of ours must not shrink to that — unless we
// are a node of weight 0, which orders nothing and hands every write to
// its peers, whose reach is then their own affair.
func (s *Server) ownerOf(key string) string {
	if !s.cfg.Ownership || s.cluster == nil {
		return ""
	}
	local := s.cluster.addr()
	weights := map[string]int{local: s.cfg.Weight()}
	held := s.store.Held()
	for _, addr := range s.store.Replicas() {
		if slices.Contains(held, addr) || (s.links.has(addr) && s.cfg.Weight() > 0) {
			continue
		}
		weights[addr] = s.cluster.weightOf(addr)
	}
	if best := owner(key, weights); best != "" && best != local {
		return best
	}
	return ""
}

// owner is the rendezvous winner for key among nodes, each competing with
// as many hashed points as its weight: every node computes the same, so
// they agree on the owner whenever they agree on the members and their
// weights. "" when no node has a weight.
func owner(key string, weights map[string]int) string {
	best, top := "", uint64(0)
	for addr, weight := range weights {
		for point := range weight {
			if sc := score(addr, point, key); sc > top || (sc == top && addr > best) {
				best, top = addr, sc
			}
		}
	}
	return best
}

// score is one of a node's rendezvous points for a key. FNV alone ranks
// keys that differ in a trailing byte almost identically, so its sum is
// mixed once more before comparing.
func score(addr string, point int, key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(addr))
	h.Write([]byte{0})
	h.Write([]byte(strconv.Itoa(point)))
	h.Write([]byte{0})
	h.Write([]byte(key))
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	return x ^ x>>33
}

// peerConns keeps a few authenticated connections to each peer for the
// short exchanges — a forward, a gossip round — apart from the replication
// pool, so none of them queues behind a fan-out and none pays a dial and a
// handshake per exchange.
type peerConns struct {
	mu   sync.Mutex
	idle map[string][]*pconn
}

type pconn struct {
	net.Conn
	r *resp.Reader
}

// forward runs args on the owner at addr. A *resp.ServerError is the
// owner's answer to the command and goes to the client; any other error
// means the owner was not reached and the write should happen here.
func (s *Server) forward(addr string, args []string) (any, error) {
	c, err := s.peerConn(addr)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(forwardTimeout))
	v, err := resp.NewCommand(append([]string{"TRITIUM.FORWARD", "FROM", s.cluster.addr()}, args...)...).Do(c, c.r)
	c.SetDeadline(time.Time{})
	var se *resp.ServerError
	if err != nil && !errors.As(err, &se) {
		c.Close()
		return nil, err
	}
	if se != nil && (strings.HasPrefix(se.Msg, "ERR unknown command") || strings.HasPrefix(se.Msg, "ERR TRITIUM.FORWARD does not carry") || strings.HasPrefix(se.Msg, "NOPERM") || strings.HasPrefix(se.Msg, "NOAUTH") || strings.HasPrefix(se.Msg, "ERR primary:")) {
		s.pconns.put(addr, c, s.cfg.PoolSize)
		// An older node (the command, or the form of it we sent, is new to
		// it), one we are not a peer of, or one whose own store is gone (it
		// is stopping): nothing was applied there, so write here.
		return nil, errors.New("owner refused the forward: " + se.Msg)
	}
	s.pconns.put(addr, c, s.cfg.PoolSize)
	if arr, ok := v.([]any); ok && len(arr) > 0 {
		// An owner that understood FROM: the client's reply, then what it
		// sent the other replicas — applied here, so this node holds the
		// write before the client hears of it, without a second round trip.
		if raw, ok := arr[0].([]byte); ok {
			for _, e := range arr[1:] {
				if cmd := commandOf(e); cmd != nil {
					if _, err := s.store.Apply(cmd); err != nil {
						slog.Warn("forwarded write not applied here", "owner", addr, "err", err)
					}
				}
			}
			return resp.NewReader(bytes.NewReader(raw)).ReadValue()
		}
	}
	return v, err
}

// commandOf rebuilds a command from its wire form in a reply: an array of
// bulk strings. Anything else is nil.
func commandOf(v any) resp.Command {
	parts, ok := v.([]any)
	if !ok || len(parts) == 0 {
		return nil
	}
	args := make([]string, 0, len(parts))
	for _, p := range parts {
		b, ok := p.([]byte)
		if !ok {
			return nil
		}
		args = append(args, string(b))
	}
	return resp.NewCommand(args...)
}

// peerConn is an authenticated connection to the peer at addr: an idle one
// from the pool, or a fresh dial.
func (s *Server) peerConn(addr string) (*pconn, error) {
	if c := s.pconns.take(addr); c != nil {
		return c, nil
	}
	conn, err := s.dialPeer(addr)
	if err != nil {
		return nil, err
	}
	c := &pconn{Conn: conn, r: resp.NewReader(conn)}
	if pw := s.peerPassword(); pw != "" {
		c.SetDeadline(time.Now().Add(peerTimeout))
		if _, err := resp.NewCommand("AUTH", "peer", pw).Do(c, c.r); err != nil {
			c.Close()
			return nil, fmt.Errorf("auth %s: %w", addr, err)
		}
		c.SetDeadline(time.Time{})
	}
	return c, nil
}

func (p *peerConns) take(addr string) *pconn {
	p.mu.Lock()
	defer p.mu.Unlock()
	idle := p.idle[addr]
	if len(idle) == 0 {
		return nil
	}
	c := idle[len(idle)-1]
	p.idle[addr] = idle[:len(idle)-1]
	return c
}

func (p *peerConns) put(addr string, c *pconn, keep int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.idle == nil {
		p.idle = map[string][]*pconn{}
	}
	if len(p.idle[addr]) >= max(keep, 1) {
		c.Close()
		return
	}
	p.idle[addr] = append(p.idle[addr], c)
}

func (p *peerConns) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, idle := range p.idle {
		for _, c := range idle {
			c.Close()
		}
	}
	p.idle = nil
}

// forwardTo carries one owned write to its owner and renders the answer for
// this client. ok is false when the owner was not reached: the caller then
// writes locally, as every node did before ownership.
func (s *session) forwardTo(owner string, args []string) (reply []byte, ok bool) {
	v, err := s.srv.forward(owner, args)
	var se *resp.ServerError
	switch {
	case err == nil:
		s.srv.forwarded.Add(1)
		if v == nil {
			return s.null(), true
		}
		return resp.AppendValue(nil, v), true
	case errors.As(err, &se):
		s.srv.forwarded.Add(1)
		return resp.AppendError(nil, se.Msg), true
	default:
		s.srv.fallbacks.Add(1)
		slog.Debug("owner unreachable, writing locally", "owner", owner, "err", err)
		return nil, false
	}
}

// forwardHandler applies a write another node sent us as the key's owner.
// The inner command is dispatched here with forwarding off, so a disagreement
// about who owns the key can never bounce it back and forth.
func (s *session) forwardHandler(args []string) []byte {
	var from string
	if strings.EqualFold(args[0], "FROM") {
		if len(args) < 3 {
			return errArity("TRITIUM.FORWARD")
		}
		from, args = args[1], args[2:]
	}
	name := strings.ToUpper(args[0])
	if !owned[name] && name != "DEL" {
		return resp.AppendError(nil, "ERR TRITIUM.FORWARD does not carry '"+args[0]+"'")
	}
	s.forwarded = true
	if from != "" {
		s.fwd = s.srv.store.Forwarded(from)
	}
	reply, _ := s.dispatch(args)
	fwd := s.fwd
	s.forwarded, s.fwd = false, nil
	if fwd == nil {
		return reply
	}
	// The reply as the client should see it, then each command the other
	// replicas got, for the forwarder to apply itself.
	sent := fwd.Sent()
	out := resp.AppendArray(nil, 1+len(sent))
	out = resp.AppendBulk(out, reply)
	for _, cmd := range sent {
		out = append(out, cmd...)
	}
	return out
}

// gossip handles TRITIUM.GOSSIP <node-json>: learn the caller, reply with
// our view.
// replicatable is what a peer may write through us: the writes our own
// fan-out produces, nothing that reads or reaches beyond the store.
var replicatable = map[string]bool{"SET": true, "SETEX": true, "DEL": true, "EXPIRE": true,
	"ZADD": true, "ZREM": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true}

// replicate applies a peer's write to this node's store only. It is how a
// peer's SET reaches us without ever dialing our store, and it never fans
// out again: the peer already sent it to everyone — except the peers it
// names with RELAY, which it could not reach itself and which this node,
// a hub both can reach, sends the plain write on to from its own pools.
func (s *session) replicate(args []string) []byte {
	var relay []string
	if strings.EqualFold(args[0], "RELAY") {
		if len(args) < 4 {
			return errArity("TRITIUM.REPLICATE")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 1 || len(args) < 3+n {
			return resp.AppendError(nil, "ERR invalid RELAY count")
		}
		relay, args = args[2:2+n], args[2+n:]
	}
	relayed := resp.NewCommand(args...) // as the sender wrote it, stamp and all
	inner := strings.ToUpper(args[0])
	if inner == "STAMPED" { // a stamped write: our clock moves past it, and a store that keeps no stamps gets it plain
		if len(args) < 3 {
			return errArity("TRITIUM.REPLICATE")
		}
		stamp, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return resp.AppendError(nil, "ERR invalid stamp")
		}
		inner = strings.ToUpper(args[2])
		if !replicatable[inner] {
			return resp.AppendError(nil, "ERR TRITIUM.REPLICATE does not carry '"+args[2]+"'")
		}
		if !s.srv.clock.observe(stamp) {
			return resp.AppendError(nil, "ERR stamp too far ahead of this node's clock")
		}
		if _, primary := s.srv.store.Stamps(); !primary {
			args = args[2:]
		}
	}
	if !replicatable[inner] {
		return resp.AppendError(nil, "ERR TRITIUM.REPLICATE does not carry '"+args[0]+"'")
	}
	if s.who.limited() { // a peer held to rights writes only the keys they name, and relays nothing
		if relay != nil {
			return replyNoPerm
		}
		for _, key := range replica.KeysOf(args) {
			if !s.who.rights.MayWrite(key) {
				return noPermKey(s.who.name, key)
			}
		}
	}
	v, err := s.srv.store.Apply(resp.NewCommand(args...))
	if err != nil {
		return errMsg(err)
	}
	for _, addr := range relay {
		s.srv.store.ReplicateTo(addr, relayed)
	}
	return resp.AppendValue(nil, v)
}

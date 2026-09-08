package server

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// Every key has one owner among the live nodes, picked by rendezvous
// hashing over their addresses, and a write to a key is carried out by its
// owner: a node handed a client's write for a key it does not own forwards
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
// deadline — so during a hold the sides may briefly disagree. So is a peer
// served over connections it opened: it cannot dial the others, so what it
// applies reaches only the nodes it can, and a write of ours must not
// shrink to that.
func (s *Server) ownerOf(key string) string {
	if !s.cfg.Ownership || s.cluster == nil {
		return ""
	}
	local := s.cluster.addr()
	best, top := local, score(local, key)
	held := s.store.Held()
	for _, addr := range s.store.Replicas() {
		if slices.Contains(held, addr) || s.links.has(addr) {
			continue
		}
		if sc := score(addr, key); sc > top || (sc == top && addr > best) {
			best, top = addr, sc
		}
	}
	if best == local {
		return ""
	}
	return best
}

// score is the rendezvous weight of a node for a key: every node computes
// the same, so they agree on the owner whenever they agree on the members.
// FNV alone ranks keys that differ in a trailing byte almost identically,
// so its sum is mixed once more before comparing.
func score(addr, key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(addr))
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
	v, err := resp.NewCommand(append([]string{"TRITIUM.FORWARD"}, args...)...).Do(c, c.r)
	c.SetDeadline(time.Time{})
	var se *resp.ServerError
	if err != nil && !errors.As(err, &se) {
		c.Close()
		return nil, err
	}
	if se != nil && (strings.HasPrefix(se.Msg, "ERR unknown command") || strings.HasPrefix(se.Msg, "NOPERM") || strings.HasPrefix(se.Msg, "NOAUTH") || strings.HasPrefix(se.Msg, "ERR primary:")) {
		s.pconns.put(addr, c, s.cfg.PoolSize)
		// An older node, one we are not a peer of, or one whose own store is
		// gone (it is stopping): nothing was applied there, so write here.
		return nil, errors.New("owner refused the forward: " + se.Msg)
	}
	s.pconns.put(addr, c, s.cfg.PoolSize)
	return v, err
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
	name := strings.ToUpper(args[0])
	if !owned[name] && name != "DEL" {
		return resp.AppendError(nil, "ERR TRITIUM.FORWARD does not carry '"+args[0]+"'")
	}
	s.forwarded = true
	reply, _ := s.dispatch(args)
	s.forwarded = false
	return reply
}

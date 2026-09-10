package server

import (
	"fmt"
	"strings"
	"sync"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
)

// A principal is who a connection speaks as once it has authenticated, and
// the one place a command is allowed or refused. The node's own two
// credentials hold every right: "default", a client with the whole keyspace,
// and "peer", another node. A USER_ holds only what its rights name and is
// never a node; a PEER_ is a node held to what its rights name. Rights that
// cannot be checked are rights that are refused, so a command the table
// does not classify, or whose keys it cannot find, is NOPERM to a limited
// principal.

type principal struct {
	name   string
	rights *config.Rights // nil: every key
	node   bool           // another node: may run the peer commands
}

var (
	asDefault = &principal{name: "default"}
	asPeer    = &principal{name: "peer", node: true}
)

// limited reports whether the principal is held to named rights.
func (p *principal) limited() bool { return p.rights != nil }

// access says what a command does to its keys, so a limited principal's
// rights can be checked against them. The zero value is a command that
// touches keys the table cannot name, or none a user may: refused.
type access struct {
	read, write bool
	all         bool // every argument is a key; otherwise only the first
	open        bool // touches no key: every principal may run it
	node        bool // a node's business whatever its rights; never a user's
}

var (
	open      = access{open: true}
	nodes     = access{node: true}
	reads     = access{read: true}
	readsAll  = access{read: true, all: true}
	writes    = access{write: true}
	writesAll = access{write: true, all: true}
	readWrite = access{read: true, write: true}
)

// allow is the gate every command passes after authentication: a peer
// command needs a node on the connection, and a limited principal reaches
// only the keys its rights name. nil means run it.
func (s *session) allow(cmd command, name string, args []string) []byte {
	if cmd.peer { // its handler checks a scoped peer's keys itself
		if !s.isPeer() {
			return replyNoPerm
		}
		return nil
	}
	return s.who.allow(name, cmd.access, args)
}

func (p *principal) allow(name string, a access, args []string) []byte {
	if !p.limited() || a.open || a.node && p.node {
		return nil
	}
	if !a.read && !a.write {
		return resp.AppendError(nil, fmt.Sprintf("NOPERM User %s has no permissions to run the '%s' command", p.name, name))
	}
	keys := args
	if !a.all && len(args) > 1 {
		keys = args[:1]
	}
	for _, key := range keys {
		if a.read && !p.rights.MayRead(key) || a.write && !p.rights.MayWrite(key) {
			return noPermKey(p.name, key)
		}
	}
	return nil
}

func noPermKey(name, key string) []byte {
	return resp.AppendError(nil, fmt.Sprintf("NOPERM User %s has no permissions to access the '%s' key", name, key))
}

// acl answers ACL WHOAMI, so a client can see which identity a connection
// carries without guessing from what it is refused.
func (s *session) acl(args []string) []byte {
	if len(args) != 1 || !strings.EqualFold(args[0], "WHOAMI") {
		return resp.AppendError(nil, fmt.Sprintf("ERR unknown subcommand '%s'. Try ACL WHOAMI.", args[0]))
	}
	return resp.AppendBulkString(nil, s.who.name)
}

// scopes is what each peer this node fans writes out to may hold, by the
// address it announced. A PEER_<name> authenticates here and then says where
// it is, in a gossip or a link, and the fan-out asks this by address — so a
// node held to rights on the accept side is held to the same ones on the
// send side. An address nobody scoped answers nil, every key, which is what
// a fleet peer is.
type scopes struct {
	mu sync.Mutex
	by map[string]*config.Rights
}

func newScopes() *scopes { return &scopes{by: map[string]*config.Rights{}} }

// note records what the peer at addr may hold. The last word wins: a node
// that comes back on another credential says what it is now, and the
// unscoped peer clears whatever a scoped one left behind.
func (s *scopes) note(addr string, r *config.Rights) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r == nil {
		delete(s.by, addr)
		return
	}
	s.by[addr] = r
}

// of is the lookup the replica fan-out holds.
func (s *scopes) of(addr string) *config.Rights {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.by[addr]
}

// noteScope binds what this connection speaks as to the address it has just
// announced. Only a node announces one; a client never reaches here.
func (s *session) noteScope(addr string) {
	if s.who != nil && s.who.node {
		s.srv.scopes.note(addr, s.who.rights)
	}
}

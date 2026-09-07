package server

import (
	"fmt"
	"strings"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
)

// Per-user rights. Besides the two built-in identities — "default", a client
// with the whole keyspace, and "peer", another node — a node may configure
// users that get strictly less: USER_<name>=<password>:<rights> names the key
// prefixes that user may read and write, and nothing outside them answers.
// It is what lets a credential live somewhere the node's own password should
// not, such as a Lambda that publishes its presence and reads its signals.
//
// Peer commands are never a user's to run, and neither is a command that
// names no key, or names keys this table does not know how to find: rights
// that cannot be checked are rights that are refused.

// keyed says where a command's keys are and what it does to them. Commands
// absent from the table are refused to a user outright.
type keyed struct {
	read  bool
	write bool
	all   bool // every argument is a key; otherwise only the first
}

var keyedCommands = map[string]keyed{
	"GET":              {read: true},
	"MGET":             {read: true, all: true},
	"EXISTS":           {read: true, all: true},
	"TTL":              {read: true},
	"ZRANGEBYSCORE":    {read: true},
	"ZCARD":            {read: true},
	"GETDEL":           {read: true, write: true},
	"SET":              {write: true},
	"SETEX":            {write: true},
	"DEL":              {write: true, all: true},
	"EXPIRE":           {write: true},
	"ZADD":             {write: true},
	"ZREM":             {write: true},
	"ZREMRANGEBYSCORE": {write: true},
}

// openCommands touch no key, so every user may run them: the handshake and
// the health of the node. The cluster view — every member's address — is
// the node's own business, not a lesser credential's.
var openCommands = map[string]bool{
	"PING": true, "ECHO": true, "INFO": true, "CLIENT": true,
	"COMMAND": true, "SELECT": true, "ACL": true,
}

// allow returns the reply refusing a command this session's user may not run,
// or nil when it may. The built-in identities have no restrictions.
func (s *session) allow(name string, args []string) []byte {
	u := s.user
	if u == nil {
		return nil
	}
	k, ok := keyedCommands[name]
	if !ok {
		if openCommands[name] {
			return nil
		}
		return resp.AppendError(nil, fmt.Sprintf("NOPERM User %s has no permissions to run the '%s' command", u.Name, name))
	}
	keys := args
	if !k.all && len(args) > 1 {
		keys = args[:1]
	}
	for _, key := range keys {
		if k.read && !config.May(u.Read, key) {
			return noPermKey(u.Name, key)
		}
		if k.write && !config.May(u.Write, key) {
			return noPermKey(u.Name, key)
		}
	}
	return nil
}

func noPermKey(user, key string) []byte {
	return resp.AppendError(nil, fmt.Sprintf("NOPERM User %s has no permissions to access the '%s' key", user, key))
}

// acl answers ACL WHOAMI, so a client can see which identity a connection
// carries without guessing from what it is refused.
func (s *session) acl(args []string) []byte {
	if len(args) != 1 || !strings.EqualFold(args[0], "WHOAMI") {
		return resp.AppendError(nil, fmt.Sprintf("ERR unknown subcommand '%s'. Try ACL WHOAMI.", args[0]))
	}
	return resp.AppendBulkString(nil, s.whoami())
}

func (s *session) whoami() string {
	switch {
	case s.user != nil:
		return s.user.Name
	case s.peer:
		return "peer"
	default:
		return "default"
	}
}

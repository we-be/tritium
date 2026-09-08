package server

import (
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// A session is one client or peer connection: served from accept (or from a
// link handed over by TRITIUM.PEERLINK) until it closes, authenticated once,
// then dispatched command by command.

// session is one client connection. It speaks RESP2 until the client asks
// for RESP3 with HELLO 3; for what tritium sends, the two differ only in how
// nulls and the HELLO reply are encoded.
//
// Two identities exist: the "default" user, a client, and the "peer" user,
// another node. Only peers may change membership with TRITIUM.GOSSIP.
// A node may configure further users with fewer rights (see acl.go).
type session struct {
	srv       *Server
	conn      net.Conn
	r         *resp.Reader
	id        int64
	proto     int
	authed    bool
	peer      bool
	fails     int               // AUTHs refused on this connection; it is closed after maxAuthFailures
	user      *config.User      // nil: one of the built-in identities, with no restrictions
	linked    *storage.NodeInfo // set by TRITIUM.PEERLINK: this connection is handed to the peer
	forwarded bool              // this command came from another node as TRITIUM.FORWARD: apply it here, whoever owns the key

	// What CLIENT LIST reports about this connection, written by its own
	// goroutine and read by another's, so under mu.
	mu      sync.Mutex
	name    string // CLIENT SETNAME
	started time.Time
	last    time.Time // when the last command arrived
	lastCmd string
}

// touch notes a command arriving, for CLIENT LIST's idle and cmd.
func (s *session) touch(name string) {
	s.mu.Lock()
	s.last, s.lastCmd = time.Now(), strings.ToLower(name)
	s.mu.Unlock()
}

// describe is this connection's CLIENT LIST line.
func (s *session) describe(now time.Time) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	who := "default"
	switch {
	case s.peer:
		who = "peer"
	case s.user != nil:
		who = s.user.Name
	case !s.authed:
		who = ""
	}
	return fmt.Sprintf("id=%d addr=%s name=%s age=%d idle=%d user=%s cmd=%s", s.id, s.conn.RemoteAddr(), s.name, int(now.Sub(s.started).Seconds()), int(now.Sub(s.last).Seconds()), who, s.lastCmd)
}

func (s *Server) serveConn(c net.Conn) { s.serveConnWith(c, resp.NewReader(c)) }

// serveConnWith serves a connection whose reader may already hold buffered
// bytes — the case for one this node opened and handed over with
// TRITIUM.PEERLINK, where the roles on the socket have just been swapped.
func (s *Server) serveConnWith(c net.Conn, r *resp.Reader) {
	s.active.Add(1)
	defer s.active.Add(-1)
	defer func() { // one connection's bug must not take the node with it
		if p := recover(); p != nil {
			slog.Error("connection panicked", "remote", c.RemoteAddr(), "panic", p, "stack", string(debug.Stack()))
		}
	}()
	handed := false
	defer func() {
		if !handed {
			c.Close()
		}
	}()

	now := time.Now()
	sess := &session{srv: s, conn: c, r: r, id: s.clientSeq.Add(1), proto: 2, authed: s.cfg.Password == "", started: now, last: now}
	s.sessMu.Lock()
	if s.sessions == nil {
		s.sessions = map[int64]*session{}
	}
	s.sessions[sess.id] = sess
	s.sessMu.Unlock()
	defer func() {
		s.sessMu.Lock()
		delete(s.sessions, sess.id)
		s.sessMu.Unlock()
	}()
	for {
		if !sess.authed {
			c.SetReadDeadline(time.Now().Add(authTimeout))
		} else {
			c.SetReadDeadline(time.Time{}) // wait for the next command with no deadline: idle is legitimate
			if err := sess.r.Peek(); err != nil {
				return
			}
			c.SetReadDeadline(time.Now().Add(commandTimeout))
		}
		args, err := sess.r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrInvalidCommand) || errors.Is(err, resp.ErrInvalidType) {
				c.Write(resp.AppendError(nil, "ERR Protocol error: expected a command array"))
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		reply, quit := sess.dispatch(args)
		if _, err := c.Write(reply); err != nil || quit {
			return
		}
		if sess.linked != nil { // the peer serves this socket from here on
			s.handOff(*sess.linked, c, sess.r)
			handed = true
			return
		}
	}
}

func (s *session) dispatch(args []string) (reply []byte, quit bool) {
	name := strings.ToUpper(args[0])
	s.touch(name)
	switch name {
	case "QUIT":
		return replyOK, true
	case "AUTH":
		if s.srv.guesses.blocked(hostOf(s.conn.RemoteAddr().String())) {
			return resp.AppendError(nil, "ERR too many failed attempts from this address; try again later"), true
		}
		if r := s.authenticate(args[1:]); r != nil {
			return r, s.fails >= maxAuthFailures
		}
		return replyOK, false
	case "HELLO":
		return s.hello(args[1:]), s.fails >= maxAuthFailures
	}
	if !s.authed {
		return replyNoAuth, false
	}
	if peerOnly[name] && !s.isPeer() {
		return replyNoPerm, false
	}
	if r := s.allow(name, args[1:]); r != nil {
		return r, false
	}
	cmd, ok := commands[name]
	if !ok {
		return resp.AppendError(nil, fmt.Sprintf("ERR unknown command '%s'", args[0])), false
	}
	if n := len(args) - 1; n < cmd.min || (cmd.max >= 0 && n > cmd.max) {
		return errArity(name), false
	}
	if owned[name] && !s.forwarded {
		if owner := s.srv.ownerOf(args[1]); owner != "" {
			if reply, ok := s.forwardTo(owner, args); ok {
				return reply, false
			}
		}
	}
	if cmd.passthrough {
		return cmd.fn(s, append([]string{name}, args[1:]...)), false
	}
	return cmd.fn(s, args[1:]), false
}

func errArity(name string) []byte {
	return resp.AppendError(nil, "ERR wrong number of arguments for '"+strings.ToLower(name)+"' command")
}

// errMsg passes a backing store's own error reply through verbatim and
// wraps anything else as ERR.
func errMsg(err error) []byte {
	var se *resp.ServerError
	if errors.As(err, &se) {
		return resp.AppendError(nil, se.Msg)
	}
	return resp.AppendError(nil, "ERR "+err.Error())
}

// authenticate handles AUTH [username] password for the "default" and
// "peer" users and returns nil on success.
func (s *session) authenticate(args []string) []byte {
	if len(args) < 1 || len(args) > 2 {
		return errArity("AUTH")
	}
	user, password := "default", args[len(args)-1]
	if len(args) == 2 {
		user = args[0]
	}
	var want string
	switch user {
	case "default":
		if s.srv.cfg.Password == "" {
			return resp.AppendError(nil, "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
		}
		want = s.srv.cfg.Password
	case "peer":
		want = s.srv.peerPassword()
	default:
		u, ok := s.srv.cfg.Users[user]
		if !ok {
			u.Password = strings.Repeat("\x00", len(password)) // an unknown user takes as long as a wrong password
		}
		if subtle.ConstantTimeCompare([]byte(password), []byte(u.Password)) != 1 || !ok {
			return s.refuse()
		}
		s.authed, s.peer, s.user = true, false, &u
		return nil
	}
	if want == "" || subtle.ConstantTimeCompare([]byte(password), []byte(want)) != 1 {
		return s.refuse()
	}
	s.authed, s.peer, s.user = true, user == "peer", nil
	return nil
}

// maxAuthFailures is how many refused AUTHs a connection gets before it is
// closed: guessing has to reconnect every few tries, and a run of them is
// visible in the log.
const maxAuthFailures = 5

func (s *session) refuse() []byte {
	s.fails++
	addr := s.conn.RemoteAddr().String()
	slog.Info("AUTH refused", "client", addr, "failures_on_connection", s.fails)
	if s.srv.guesses.note(hostOf(addr)) {
		slog.Warn("shutting an address out after repeated AUTH failures", "client", addr, "for", guessLockout)
	}
	if s.fails >= maxAuthFailures {
		slog.Warn("closing a connection after repeated AUTH failures", "client", addr)
	}
	return replyWrongPass
}

func hostOf(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// isPeer reports whether the connection may change cluster membership: it
// authenticated as the peer user when a password is configured, and under
// TLS_CLIENT_AUTH it presented a certificate the listener verified.
func (s *session) isPeer() bool {
	if s.user != nil {
		return false // a configured user is never a node, however the node is set up
	}
	if s.srv.peerPassword() != "" && !s.peer {
		return false
	}
	if s.srv.cfg.TLSClientAuth {
		tc, ok := s.conn.(*tls.Conn)
		if !ok || len(tc.ConnectionState().VerifiedChains) == 0 {
			return false
		}
	}
	return true
}

// hello handles HELLO [protover [AUTH username password] [SETNAME name]]
// and switches the connection to RESP3 when asked.
func (s *session) hello(args []string) []byte {
	proto := s.proto
	if len(args) > 0 {
		switch args[0] {
		case "2", "3":
			proto = int(args[0][0] - '0')
		default:
			return resp.AppendError(nil, "NOPROTO unsupported protocol version")
		}
		for i := 1; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "AUTH":
				if i+2 >= len(args) {
					return resp.AppendError(nil, "ERR Syntax error in HELLO option 'AUTH'")
				}
				if r := s.authenticate(args[i+1 : i+3]); r != nil {
					return r
				}
				i += 2
			case "SETNAME":
				if i+1 >= len(args) {
					return resp.AppendError(nil, "ERR Syntax error in HELLO option 'SETNAME'")
				}
				i++
			default:
				return resp.AppendError(nil, "ERR Syntax error in HELLO option '"+args[i]+"'")
			}
		}
	}
	if !s.authed {
		return replyNoAuth
	}
	s.proto = proto
	var b []byte
	if proto == 3 {
		b = resp.AppendMap(nil, 6)
	} else {
		b = resp.AppendArray(nil, 12)
	}
	b = resp.AppendBulkString(b, "server")
	b = resp.AppendBulkString(b, "tritium")
	b = resp.AppendBulkString(b, "version")
	b = resp.AppendBulkString(b, Version)
	b = resp.AppendBulkString(b, "proto")
	b = resp.AppendInt(b, int64(proto))
	b = resp.AppendBulkString(b, "id")
	b = resp.AppendInt(b, s.id)
	b = resp.AppendBulkString(b, "mode")
	b = resp.AppendBulkString(b, "cluster")
	b = resp.AppendBulkString(b, "role")
	b = resp.AppendBulkString(b, "master")
	return b
}

func (s *session) ping(args []string) []byte {
	if len(args) == 1 {
		return resp.AppendBulkString(nil, args[0])
	}
	return resp.AppendSimpleString(nil, "PONG")
}

func (s *session) echo(args []string) []byte {
	return resp.AppendBulkString(nil, args[0])
}

// client accepts the CLIENT subcommands connection libraries send on
// connect; nothing is recorded.
// client handles CLIENT: SETNAME and GETNAME so a connection can say what
// it is — worker, bridge, CLI — and LIST so a node can say who is
// connected. LIST is for the node's own identity and peers; a user with
// prefix rights learns nothing about other connections.
func (s *session) client(args []string) []byte {
	switch strings.ToUpper(args[0]) {
	case "SETNAME":
		if len(args) != 2 || args[1] == "" || strings.ContainsAny(args[1], " \t\r\n") {
			return resp.AppendError(nil, "ERR Client names cannot contain spaces, newlines or special characters.")
		}
		s.mu.Lock()
		s.name = args[1]
		s.mu.Unlock()
		return replyOK
	case "GETNAME":
		s.mu.Lock()
		name := s.name
		s.mu.Unlock()
		if name == "" {
			return s.null()
		}
		return resp.AppendBulkString(nil, name)
	case "SETINFO":
		return replyOK
	case "ID":
		return resp.AppendInt(nil, s.id)
	case "LIST":
		if s.user != nil {
			return replyNoPerm
		}
		return resp.AppendBulkString(nil, s.srv.clientList())
	}
	return resp.AppendError(nil, fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", args[0]))
}

// clientList is every connection being served, one line each, by id.
func (s *Server) clientList() string {
	now := time.Now()
	s.sessMu.Lock()
	ids := slices.Sorted(maps.Keys(s.sessions))
	lines := make([]string, 0, len(ids))
	for _, id := range ids {
		lines = append(lines, s.sessions[id].describe(now))
	}
	s.sessMu.Unlock()
	return strings.Join(lines, "\n") + "\n"
}

// command answers COMMAND [DOCS|INFO ...] with an empty array, which is
// enough for valkey-cli to start without complaint.
func (s *session) command(args []string) []byte {
	return resp.AppendArray(nil, 0)
}

func (s *session) selectDB(args []string) []byte {
	if args[0] == "0" {
		return replyOK
	}
	return resp.AppendError(nil, "ERR DB index is out of range")
}

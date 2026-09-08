package server

import (
	"cmp"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// ttlCommand is what a replicated sorted-set write is followed by, so every
// key still expires.
func ttlCommand(key string) resp.Command {
	return resp.NewCommand("EXPIRE", key, strconv.Itoa(DefaultTTL))
}

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
}

var (
	replyOK        = resp.AppendSimpleString(nil, "OK")
	replyNoAuth    = resp.AppendError(nil, "NOAUTH Authentication required.")
	replyWrongPass = resp.AppendError(nil, "WRONGPASS invalid username-password pair or user is disabled.")
	replyNoPerm    = resp.AppendError(nil, "NOPERM only cluster peers may run this command")
)

type command struct {
	min, max    int // argument counts after the name; max -1 means unbounded
	fn          func(*session, []string) []byte
	passthrough bool // fn receives the command name as args[0]
}

// commands are dispatched after authentication. AUTH, HELLO and QUIT are
// handled before it in dispatch.
var commands = map[string]command{
	"PING":              {min: 0, max: 1, fn: (*session).ping},
	"ECHO":              {min: 1, max: 1, fn: (*session).echo},
	"SET":               {min: 2, max: -1, fn: (*session).set},
	"SETEX":             {min: 3, max: 3, fn: (*session).setex},
	"GET":               {min: 1, max: 1, fn: (*session).get},
	"GETDEL":            {min: 1, max: 1, fn: (*session).getdel},
	"MGET":              {min: 1, max: -1, fn: (*session).mget},
	"DEL":               {min: 1, max: -1, fn: (*session).del},
	"EXISTS":            {min: 1, max: -1, fn: (*session).exists},
	"TTL":               {min: 1, max: 1, fn: (*session).ttl},
	"EXPIRE":            {min: 2, max: 3, fn: (*session).expire},
	"ZADD":              {min: 3, max: -1, fn: (*session).zadd},
	"ZRANGEBYSCORE":     {min: 3, max: -1, fn: (*session).query, passthrough: true},
	"ZREM":              {min: 2, max: -1, fn: (*session).mutate, passthrough: true},
	"ZREMRANGEBYSCORE":  {min: 3, max: 3, fn: (*session).mutate, passthrough: true},
	"ZCARD":             {min: 1, max: 1, fn: (*session).query, passthrough: true},
	"SCAN":              {min: 1, max: -1, fn: (*session).query, passthrough: true},
	"TYPE":              {min: 1, max: 1, fn: (*session).query, passthrough: true},
	"DBSIZE":            {min: 0, max: 0, fn: (*session).query, passthrough: true},
	"INFO":              {min: 0, max: -1, fn: (*session).info},
	"CLIENT":            {min: 1, max: -1, fn: (*session).client},
	"COMMAND":           {min: 0, max: -1, fn: (*session).command},
	"SELECT":            {min: 1, max: 1, fn: (*session).selectDB},
	"ACL":               {min: 1, max: -1, fn: (*session).acl},
	"TRITIUM.NODES":     {min: 0, max: 0, fn: (*session).nodes},
	"TRITIUM.GOSSIP":    {min: 1, max: 1, fn: (*session).gossip},
	"TRITIUM.REPLICATE": {min: 2, max: -1, fn: (*session).replicate},
	"TRITIUM.PEERLINK":  {min: 1, max: 1, fn: (*session).peerlink},
}

// TRITIUM.FORWARD dispatches the command it carries, so it joins the table
// at init rather than in the literal that dispatch reads.
func init() {
	commands["TRITIUM.FORWARD"] = command{min: 2, max: -1, fn: (*session).forwardHandler}
}

// peerOnly commands change membership or write straight into the store, so
// only another node may run them.
var peerOnly = map[string]bool{"TRITIUM.GOSSIP": true, "TRITIUM.REPLICATE": true, "TRITIUM.PEERLINK": true, "TRITIUM.FORWARD": true}

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

	sess := &session{srv: s, conn: c, r: r, id: s.clientSeq.Add(1), proto: 2, authed: s.cfg.Password == ""}
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
			s.handOff(*sess.linked, c)
			handed = true
			return
		}
	}
}

func (s *session) dispatch(args []string) (reply []byte, quit bool) {
	name := strings.ToUpper(args[0])
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

// set handles SET key value [EX seconds | PX milliseconds] [NX]. A write
// without an expiry gets DefaultTTL; XX, KEEPTTL and GET are not supported.
func (s *session) set(args []string) []byte {
	key, value := args[0], args[1]
	ttl := DefaultTTL
	nx := false
	for i := 2; i < len(args); i++ {
		switch opt := strings.ToUpper(args[i]); opt {
		case "NX":
			nx = true
		case "EX", "PX":
			if i+1 >= len(args) {
				return resp.AppendError(nil, "ERR syntax error")
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				return resp.AppendError(nil, "ERR invalid expire time in 'set' command")
			}
			if opt == "PX" {
				n = (n + 999) / 1000
			}
			ttl = n
			i++
		default:
			return resp.AppendError(nil, "ERR syntax error")
		}
	}
	ttl = s.userTTL(ttl)
	if !nx {
		return s.write(key, value, ttl)
	}
	// NX is decided by the primary; replicas only hear about it if it won.
	q, rep := s.stampedPair([]string{"SET", key, value, "EX", strconv.Itoa(ttl), "NX"}, []string{"SETEX", key, strconv.Itoa(ttl), value})
	v, err := s.srv.store.Query(q...)
	if err != nil {
		return errMsg(err)
	}
	if v == nil {
		return s.null()
	}
	s.srv.store.Replicate(resp.NewCommand(rep...))
	s.srv.bytes.Add(int64(len(value)))
	return replyOK
}

// stampedPair stamps a write the primary runs itself and the form its
// replicas get, with one stamp for both; without stamps they pass through.
func (s *session) stampedPair(local, remote []string) ([]string, []string) {
	on, primary := s.srv.store.Stamps()
	if !on {
		return local, remote
	}
	n := strconv.FormatUint(s.srv.clock.next(), 10)
	remote = append([]string{"STAMPED", n}, remote...)
	if primary {
		local = append([]string{"STAMPED", n}, local...)
	}
	return local, remote
}

func (s *session) setex(args []string) []byte {
	ttl, err := strconv.Atoi(args[1])
	if err != nil || ttl <= 0 {
		return resp.AppendError(nil, "ERR invalid expire time in 'setex' command")
	}
	return s.write(args[0], args[2], s.userTTL(ttl))
}

// userTTL caps what a user with prefix rights may ask for: with eviction
// ordered by expiry, a key living for years would outlast everyone else's
// in the store and push theirs out first.
func (s *session) userTTL(ttl int) int {
	if s.user != nil && ttl > DefaultTTL {
		return DefaultTTL
	}
	return ttl
}

func (s *session) write(key, value string, ttl int) []byte {
	if err := s.srv.store.Set(key, []byte(value), ttl); err != nil {
		return errMsg(err)
	}
	s.srv.bytes.Add(int64(len(value)))
	return replyOK
}

// null is the protocol's null reply.
func (s *session) null() []byte {
	if s.proto == 3 {
		return []byte("_\r\n")
	}
	return resp.AppendNull(nil)
}

func (s *session) get(args []string) []byte {
	v, err := s.srv.store.Get(args[0])
	if errors.Is(err, storage.ErrNotFound) {
		return s.null()
	}
	if err != nil {
		return errMsg(err)
	}
	s.srv.bytes.Add(int64(len(v)))
	return resp.AppendBulk(nil, v)
}

// getdel reads and removes a key in one step on the primary, then tells
// the replicas to drop it.
func (s *session) getdel(args []string) []byte {
	q, rep := s.stampedPair([]string{"GETDEL", args[0]}, []string{"DEL", args[0]})
	v, err := s.srv.store.Query(q...)
	if err != nil {
		return errMsg(err)
	}
	if v == nil {
		return s.null()
	}
	s.srv.store.Replicate(resp.NewCommand(rep...))
	b, _ := v.([]byte)
	s.srv.bytes.Add(int64(len(b)))
	return resp.AppendBulk(nil, b)
}

func (s *session) mget(args []string) []byte {
	return s.query(append([]string{"MGET"}, args...))
}

// del removes keys, each through its owner: the keys of one DEL may belong
// to several nodes, so they are grouped and the counts added up.
func (s *session) del(args []string) []byte {
	if s.forwarded || !s.srv.cfg.Ownership {
		return integer(s.srv.store.Delete(args...))
	}
	byOwner := map[string][]string{}
	for _, k := range args {
		owner := s.srv.ownerOf(k)
		byOwner[owner] = append(byOwner[owner], k)
	}
	var total int64
	for owner, keys := range byOwner {
		if owner != "" {
			v, err := s.srv.forward(owner, append([]string{"DEL"}, keys...))
			var se *resp.ServerError
			if errors.As(err, &se) {
				return resp.AppendError(nil, se.Msg)
			}
			if err == nil {
				n, _ := v.(int64)
				total += n
				s.srv.forwarded.Add(1)
				continue
			}
			s.srv.fallbacks.Add(1)
		}
		n, err := s.srv.store.Delete(keys...)
		if err != nil {
			return errMsg(err)
		}
		total += n
	}
	return resp.AppendInt(nil, total)
}

// zadd handles ZADD key score member [score member ...], plain form only,
// and refreshes the set's TTL so it expires like everything else.
func (s *session) zadd(args []string) []byte {
	if len(args)%2 != 1 {
		return resp.AppendError(nil, "ERR syntax error")
	}
	for i := 1; i < len(args); i += 2 {
		if _, err := strconv.ParseFloat(args[i], 64); err != nil {
			return resp.AppendError(nil, "ERR value is not a valid float")
		}
	}
	out, err := s.srv.store.Mutate(resp.NewCommand(append([]string{"ZADD"}, args...)...), ttlCommand(args[0]))
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendValue(nil, out[0])
}

// query passes a read-only command through to the primary. The command name
// is args[0] when called directly by dispatch. SCAN, TYPE and DBSIZE go
// through here too, reading the local primary like GET; the cursor SCAN
// hands back is the store's own, opaque to us. KEYS stays unsupported: it
// has no cursor and is O(n) on a real store.
func (s *session) query(args []string) []byte {
	v, err := s.srv.store.Query(args...)
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendValue(nil, v)
}

// mutate passes a write through to the primary and replicates it.
func (s *session) mutate(args []string) []byte {
	out, err := s.srv.store.Mutate(resp.NewCommand(args...))
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendValue(nil, out[0])
}

// expire handles EXPIRE key seconds [NX|XX|GT|LT]. Seconds must be
// positive: keys are removed with DEL, not by expiring them into the past.
func (s *session) expire(args []string) []byte {
	n, err := strconv.Atoi(args[1])
	if err != nil || n <= 0 {
		return resp.AppendError(nil, "ERR invalid expire time in 'expire' command")
	}
	args[1] = strconv.Itoa(s.userTTL(n))
	if len(args) == 3 {
		switch strings.ToUpper(args[2]) {
		case "NX", "XX", "GT", "LT":
		default:
			return resp.AppendError(nil, "ERR Unsupported option "+args[2])
		}
	}
	return s.mutate(append([]string{"EXPIRE"}, args...))
}

func (s *session) exists(args []string) []byte {
	return integer(s.srv.store.Exists(args...))
}

func (s *session) ttl(args []string) []byte {
	return integer(s.srv.store.TTL(args[0]))
}

func integer(n int64, err error) []byte {
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendInt(nil, n)
}

// info renders INFO [section ...] in the usual "# Section" layout.
func (s *session) info(args []string) []byte {
	local := s.srv.cluster.localCopy()
	stats := s.srv.Stats()
	_, port, _ := net.SplitHostPort(s.srv.Addr())
	sections := []struct{ name, body string }{
		{"server", fmt.Sprintf("server_name:tritium\r\ntritium_version:%s\r\ntcp_port:%s\r\n", Version, port)},
		{"clients", fmt.Sprintf("connected_clients:%d\r\n", stats.ActiveConnections)},
		{"stats", fmt.Sprintf("bytes_transferred:%d\r\n", stats.BytesTransferred)},
		{"replication", "role:master\r\n"},
		{"tritium", fmt.Sprintf("node_id:%s\r\nnode_addr:%s\r\nversion:%s\r\nseeds:%s\r\nstore:%s\r\nstore_tls:%s\r\ncluster_nodes:%d\r\nreplicas:%d\r\nheld_replicas:%d\r\nqueued_replicas:%d\r\nreplication:%s\r\nkey_ownership:%s\r\nforwarded:%d\r\nforward_fallbacks:%d\r\nstamps:%s\r\nevents:%d\r\n",
			local.ID, local.Addr, Version, strings.Join(local.Seeds, ","), local.StoreAddr, onOff(s.srv.cfg.StoreTLS), len(s.srv.Nodes()), stats.Replicas, stats.Held, len(s.srv.store.Queued()), replicationMode(s.srv.store.Async()), onOff(s.srv.cfg.Ownership), s.srv.forwarded.Load(), s.srv.fallbacks.Load(), stampsMode(s.srv.store.Stamps()), s.srv.eventsKept(local.ID))},
		{"store", s.srv.storeInfo()},
	}

	all := len(args) == 0
	for _, a := range args {
		switch strings.ToLower(a) {
		case "all", "default", "everything":
			all = true
		}
	}
	var sb strings.Builder
	for _, sec := range sections {
		if !all && !containsFold(args, sec.name) {
			continue
		}
		if s.user != nil && (sec.name == "tritium" || sec.name == "store") {
			continue // a user gets the node's health, not its address, its seeds, or its store
		}
		sb.WriteString("# " + strings.ToUpper(sec.name[:1]) + sec.name[1:] + "\r\n" + sec.body + "\r\n")
	}
	return resp.AppendBulkString(nil, sb.String())
}

// storeInfo is the primary store as seen through this node — the only way
// to see it once stores bind to loopback — as store_* fields: whether it
// answers, and what its own INFO says about version, uptime, memory and keys.
func (s *Server) storeInfo() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "store_addr:%s\r\n", s.cfg.StoreLabel())
	v, err := s.store.Query("INFO", "server", "memory", "keyspace")
	raw, _ := v.([]byte)
	if err != nil {
		fmt.Fprintf(&sb, "store_status:unreachable\r\nstore_error:%s\r\n", strings.ReplaceAll(err.Error(), "\n", " "))
		return sb.String()
	}
	sb.WriteString("store_status:ok\r\n")
	fields := map[string]string{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if k, val, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			fields[k] = val
		}
	}
	if ver := cmp.Or(fields["tritium_version"], fields["valkey_version"], fields["redis_version"]); ver != "" {
		fmt.Fprintf(&sb, "store_version:%s\r\n", ver)
	}
	for _, f := range []string{"uptime_in_seconds", "used_memory", "maxmemory", "maxmemory_policy"} {
		if val, ok := fields[f]; ok {
			fmt.Fprintf(&sb, "store_%s:%s\r\n", f, val)
		}
	}
	if db, ok := fields["db0"]; ok { // keys=N,expires=N,avg_ttl=N
		if _, n, ok := strings.Cut(db, "keys="); ok {
			n, _, _ = strings.Cut(n, ",")
			fmt.Fprintf(&sb, "store_keys:%s\r\n", n)
		}
	}
	return sb.String()
}

// stampsMode is how INFO reports write stamps: kept by this node's own
// store, or minted for peers only because an external store keeps none.
func stampsMode(on, primary bool) string {
	switch {
	case on && primary:
		return "kept"
	case on:
		return "pass-through"
	}
	return "off"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func replicationMode(async bool) string {
	if async {
		return "async"
	}
	return "sync"
}

func containsFold(list []string, s string) bool {
	for _, l := range list {
		if strings.EqualFold(l, s) {
			return true
		}
	}
	return false
}

// client accepts the CLIENT subcommands connection libraries send on
// connect; nothing is recorded.
func (s *session) client(args []string) []byte {
	switch strings.ToUpper(args[0]) {
	case "SETNAME", "SETINFO":
		return replyOK
	case "ID":
		return resp.AppendInt(nil, s.id)
	case "GETNAME":
		return s.null()
	}
	return resp.AppendError(nil, fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", args[0]))
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

func (s *session) nodes(args []string) []byte {
	return viewJSON(s.srv.cluster.snapshot())
}

// gossip handles TRITIUM.GOSSIP <node-json>: learn the caller, reply with
// our view.
// replicatable is what a peer may write through us: the writes our own
// fan-out produces, nothing that reads or reaches beyond the store.
var replicatable = map[string]bool{"SET": true, "SETEX": true, "DEL": true, "EXPIRE": true,
	"ZADD": true, "ZREM": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true}

// replicate applies a peer's write to this node's store only. It is how a
// peer's SET reaches us without ever dialing our store, and it never fans
// out again: the peer already sent it to everyone.
func (s *session) replicate(args []string) []byte {
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
	v, err := s.srv.store.Apply(resp.NewCommand(args...))
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendValue(nil, v)
}

// peerlink handles TRITIUM.PEERLINK <node-json>: the caller cannot be dialed,
// so it opened this connection for us to send on. The reply is the last thing
// we write as its server; serveConnWith parks the socket afterwards.
func (s *session) peerlink(args []string) []byte {
	var n storage.NodeInfo
	if err := json.Unmarshal([]byte(args[0]), &n); err != nil || n.ID == "" || n.Addr == "" {
		return resp.AppendError(nil, "ERR invalid node info")
	}
	if r := s.certNames(n.Addr); r != nil {
		return r
	}
	s.linked = &n
	return replyOK
}

// certNames refuses a peer that announces an address its certificate does
// not name, under TLS_CLIENT_AUTH: with a stolen peer password alone, a
// node could otherwise claim another member's address and be handed its
// replication. Without client certificates there is nothing to check.
func (s *session) certNames(addr string) []byte {
	if !s.srv.cfg.TLSClientAuth {
		return nil
	}
	tc, ok := s.conn.(*tls.Conn)
	if !ok || len(tc.ConnectionState().PeerCertificates) == 0 {
		return replyNoPerm
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return resp.AppendError(nil, "ERR invalid node address")
	}
	if err := tc.ConnectionState().PeerCertificates[0].VerifyHostname(host); err != nil {
		return resp.AppendError(nil, "NOPERM the peer's certificate does not name "+host)
	}
	return nil
}

func (s *session) gossip(args []string) []byte {
	var n storage.NodeInfo
	if err := json.Unmarshal([]byte(args[0]), &n); err != nil || n.ID == "" {
		return resp.AppendError(nil, "ERR invalid node info")
	}
	if r := s.certNames(n.Addr); r != nil {
		return r
	}
	s.srv.cluster.learn(n)
	return viewJSON(s.srv.cluster.snapshot())
}

func viewJSON(view map[string]storage.NodeInfo) []byte {
	b, err := json.Marshal(view)
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendBulk(nil, b)
}

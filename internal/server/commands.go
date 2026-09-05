package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// session is one client connection. It speaks RESP2 until the client asks
// for RESP3 with HELLO 3; for what tritium sends, the two differ only in how
// nulls and the HELLO reply are encoded.
type session struct {
	srv    *Server
	conn   net.Conn
	r      *resp.Reader
	id     int64
	proto  int
	authed bool
}

var (
	replyOK     = resp.AppendSimpleString(nil, "OK")
	replyNoAuth = resp.AppendError(nil, "NOAUTH Authentication required.")
)

type command struct {
	min, max int // argument counts after the name; max -1 means unbounded
	fn       func(*session, []string) []byte
}

// commands are dispatched after authentication. AUTH, HELLO and QUIT are
// handled before it in dispatch.
var commands = map[string]command{
	"PING":           {0, 1, (*session).ping},
	"ECHO":           {1, 1, (*session).echo},
	"SET":            {2, -1, (*session).set},
	"SETEX":          {3, 3, (*session).setex},
	"GET":            {1, 1, (*session).get},
	"DEL":            {1, -1, (*session).del},
	"EXISTS":         {1, -1, (*session).exists},
	"TTL":            {1, 1, (*session).ttl},
	"INFO":           {0, -1, (*session).info},
	"CLIENT":         {1, -1, (*session).client},
	"COMMAND":        {0, -1, (*session).command},
	"SELECT":         {1, 1, (*session).selectDB},
	"TRITIUM.NODES":  {0, 0, (*session).nodes},
	"TRITIUM.GOSSIP": {1, 1, (*session).gossip},
}

func (s *Server) serveConn(c net.Conn) {
	s.active.Add(1)
	defer s.active.Add(-1)
	defer c.Close()

	sess := &session{srv: s, conn: c, r: resp.NewReader(c), id: s.clientSeq.Add(1), proto: 2, authed: s.cfg.Password == ""}
	for {
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
	}
}

func (s *session) dispatch(args []string) (reply []byte, quit bool) {
	name := strings.ToUpper(args[0])
	switch name {
	case "QUIT":
		return replyOK, true
	case "AUTH":
		if r := s.authenticate(args[1:]); r != nil {
			return r, false
		}
		return replyOK, false
	case "HELLO":
		return s.hello(args[1:]), false
	}
	if !s.authed {
		return replyNoAuth, false
	}
	cmd, ok := commands[name]
	if !ok {
		return resp.AppendError(nil, fmt.Sprintf("ERR unknown command '%s'", args[0])), false
	}
	if n := len(args) - 1; n < cmd.min || (cmd.max >= 0 && n > cmd.max) {
		return errArity(name), false
	}
	return cmd.fn(s, args[1:]), false
}

func errArity(name string) []byte {
	return resp.AppendError(nil, "ERR wrong number of arguments for '"+strings.ToLower(name)+"' command")
}

func errMsg(err error) []byte {
	return resp.AppendError(nil, "ERR "+err.Error())
}

// authenticate handles AUTH [username] password and returns nil on success.
func (s *session) authenticate(args []string) []byte {
	if len(args) < 1 || len(args) > 2 {
		return errArity("AUTH")
	}
	if s.srv.cfg.Password == "" {
		return resp.AppendError(nil, "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	if len(args) == 2 && args[0] != "default" {
		return resp.AppendError(nil, "WRONGPASS invalid username-password pair or user is disabled.")
	}
	if subtle.ConstantTimeCompare([]byte(args[len(args)-1]), []byte(s.srv.cfg.Password)) != 1 {
		return resp.AppendError(nil, "WRONGPASS invalid username-password pair or user is disabled.")
	}
	s.authed = true
	return nil
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

// set handles SET key value [EX seconds | PX milliseconds]. A write without
// an expiry gets DefaultTTL; NX, XX, KEEPTTL and GET are not supported.
func (s *session) set(args []string) []byte {
	key, value := args[0], args[1]
	ttl := DefaultTTL
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i])
		if (opt != "EX" && opt != "PX") || i+1 >= len(args) {
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
	}
	return s.write(key, value, ttl)
}

func (s *session) setex(args []string) []byte {
	ttl, err := strconv.Atoi(args[1])
	if err != nil || ttl <= 0 {
		return resp.AppendError(nil, "ERR invalid expire time in 'setex' command")
	}
	return s.write(args[0], args[2], ttl)
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

func (s *session) del(args []string) []byte {
	return integer(s.srv.store.Delete(args...))
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
		{"tritium", fmt.Sprintf("node_id:%s\r\nnode_addr:%s\r\nstore:%s\r\ncluster_nodes:%d\r\nreplicas:%d\r\n",
			local.ID, local.Addr, local.StoreAddr, len(s.srv.Nodes()), len(s.srv.store.Replicas()))},
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
		sb.WriteString("# " + strings.ToUpper(sec.name[:1]) + sec.name[1:] + "\r\n" + sec.body + "\r\n")
	}
	return resp.AppendBulkString(nil, sb.String())
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
func (s *session) gossip(args []string) []byte {
	var n storage.NodeInfo
	if err := json.Unmarshal([]byte(args[0]), &n); err != nil || n.ID == "" {
		return resp.AppendError(nil, "ERR invalid node info")
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

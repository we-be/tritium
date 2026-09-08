package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// ttlCommand is what a replicated sorted-set write is followed by, so every
// key still expires.
func ttlCommand(key string) resp.Command {
	return resp.NewCommand("EXPIRE", key, strconv.Itoa(DefaultTTL))
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
	v, err := s.w().Query(q...)
	if err != nil {
		return errMsg(err)
	}
	if v == nil {
		return s.null()
	}
	s.w().Replicate(resp.NewCommand(rep...))
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
	if err := s.w().Set(key, []byte(value), ttl); err != nil {
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
	v, err := s.w().Query(q...)
	if err != nil {
		return errMsg(err)
	}
	if v == nil {
		return s.null()
	}
	s.w().Replicate(resp.NewCommand(rep...))
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
		return integer(s.w().Delete(args...))
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
		n, err := s.w().Delete(keys...)
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
	out, err := s.w().Mutate(resp.NewCommand(append([]string{"ZADD"}, args...)...), ttlCommand(args[0]))
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
	v, err := s.w().Query(args...)
	if err != nil {
		return errMsg(err)
	}
	return resp.AppendValue(nil, v)
}

// mutate passes a write through to the primary and replicates it.
func (s *session) mutate(args []string) []byte {
	out, err := s.w().Mutate(resp.NewCommand(args...))
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

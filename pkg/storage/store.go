package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// ErrNotFound is returned by Get for a missing or expired key. Its text is
// part of the wire contract: servers put it in GetReply.Error.
var ErrNotFound = errors.New("key not found")

const dialTimeout = 5 * time.Second

// conn is a pooled connection with its own buffered reader, so bytes buffered
// past one reply can never leak to the next user of the connection.
type conn struct {
	net.Conn
	r *resp.Reader
}

// pool is a fixed-size pool of connections to one RESP server. A slot holds
// nil after a transport error and is redialed on next use.
type pool struct {
	addr     string
	password string
	slots    chan *conn
}

func newPool(addr string, size int, password string) (*pool, error) {
	p := &pool{addr: addr, password: password, slots: make(chan *conn, size)}
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
	c, err := net.DialTimeout("tcp", p.addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", p.addr, err)
	}
	pc := &conn{Conn: c, r: resp.NewReader(c)}
	if p.password != "" {
		if _, err := resp.NewCommand("AUTH", p.password).Do(pc, pc.r); err != nil {
			c.Close()
			return nil, fmt.Errorf("auth %s: %w", p.addr, err)
		}
	}
	return pc, nil
}

func (p *pool) get() (*conn, error) {
	c := <-p.slots
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

// put returns c to the pool, or drops it if the last operation broke it.
func (p *pool) put(c *conn, err error) {
	if err != nil {
		c.Close()
		c = nil
	}
	p.slots <- c
}

func (p *pool) close() {
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
	c, err := p.get()
	if err != nil {
		return nil, err
	}
	v, err := cmd.Do(c, c.r)
	if err != nil && !errors.As(err, new(*resp.ServerError)) {
		p.put(c, err)
	} else {
		p.put(c, nil)
	}
	return v, err
}

// Store writes through to a primary RESP server and fans every write out to
// replica servers (the primaries of the other nodes in the cluster).
type Store struct {
	primary  *pool
	size     int
	password string
	mu       sync.RWMutex
	replicas []*pool
}

// NewStore connects poolSize connections to the primary at addr, failing
// fast if it is unreachable. The password, if any, is sent as AUTH to the
// primary and to every replica.
func NewStore(addr string, poolSize int, password string) (*Store, error) {
	if poolSize < 1 {
		poolSize = 1
	}
	p, err := newPool(addr, poolSize, password)
	if err != nil {
		return nil, fmt.Errorf("primary: %w", err)
	}
	return &Store{primary: p, size: poolSize, password: password}, nil
}

// Set stores value under key for ttl seconds, then replicates the write.
func (s *Store) Set(key string, value []byte, ttl int) error {
	cmd := resp.NewCommand("SETEX", key, strconv.Itoa(ttl), string(value))
	v, err := s.primary.do(cmd)
	if err != nil {
		return fmt.Errorf("primary: %w", err)
	}
	if v != "OK" {
		return fmt.Errorf("primary: unexpected reply %v", v)
	}
	s.replicate(cmd)
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
	cmd := resp.NewCommand(append([]string{"DEL"}, keys...)...)
	n, err := s.primary.integer(cmd)
	if err != nil {
		return 0, err
	}
	s.replicate(cmd)
	return n, nil
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

// replicate runs cmd on every replica concurrently. Failures are logged, not
// returned: the primary write already succeeded.
func (s *Store) replicate(cmd resp.Command) {
	s.mu.RLock()
	replicas := slices.Clone(s.replicas)
	s.mu.RUnlock()

	var wg sync.WaitGroup
	for _, r := range replicas {
		wg.Go(func() {
			if _, err := r.do(cmd); err != nil {
				slog.Warn("replica write failed", "addr", r.addr, "err", err)
			}
		})
	}
	wg.Wait()
}

// AddReplica starts fanning writes out to addr. Adding an address twice is a no-op.
func (s *Store) AddReplica(addr string) error {
	if s.hasReplica(addr) {
		return nil
	}
	p, err := newPool(addr, s.size, s.password)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if slices.ContainsFunc(s.replicas, func(r *pool) bool { return r.addr == addr }) {
		p.close()
		return nil
	}
	s.replicas = append(s.replicas, p)
	return nil
}

// RemoveReplica stops replicating to addr and reports whether it was known.
func (s *Store) RemoveReplica(addr string) bool {
	s.mu.Lock()
	i := slices.IndexFunc(s.replicas, func(r *pool) bool { return r.addr == addr })
	var p *pool
	if i >= 0 {
		p = s.replicas[i]
		s.replicas = slices.Delete(s.replicas, i, i+1)
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

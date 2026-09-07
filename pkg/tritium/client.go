// Package tritium is a Go client for a tritium node. Tritium speaks RESP, so
// any Redis or Valkey client library works too; this one is dependency free,
// knows the TRITIUM.* cluster commands, and can encrypt values end to end.
package tritium

import (
	"cmp"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// ErrNotFound is returned by Get for a missing or expired key.
var ErrNotFound = storage.ErrNotFound

type ClientOptions struct {
	Address  string        // host:port of a node; default localhost:8080
	Timeout  time.Duration // dial timeout and per-call deadline; default 10s
	Password string        // sent as AUTH on every connection when set
	TLS      *tls.Config   // connect with TLS when set; ServerName defaults to the address host
	Key      []byte        // KeySize bytes; when set, values are encrypted client-side (see crypto.go)
}

// Client holds one connection to a node and serializes calls on it. A
// transport error drops the connection; the next call redials.
type Client struct {
	opts ClientOptions
	box  *box // nil: values pass through in the clear
	mu   sync.Mutex
	conn net.Conn
	r    *resp.Reader
}

// NewClient connects to a node. A nil opts uses the defaults.
func NewClient(opts *ClientOptions) (*Client, error) {
	o := ClientOptions{Address: "localhost:8080", Timeout: 10 * time.Second}
	if opts != nil {
		if opts.Address != "" {
			o.Address = opts.Address
		}
		if opts.Timeout > 0 {
			o.Timeout = opts.Timeout
		}
		o.Password, o.TLS, o.Key = opts.Password, opts.TLS, opts.Key
	}
	c := &Client{opts: o}
	if o.Key != nil {
		var err error
		if c.box, err = newBox(o.Key); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	d := net.Dialer{Timeout: c.opts.Timeout}
	var conn net.Conn
	var err error
	if c.opts.TLS != nil {
		conn, err = tls.DialWithDialer(&d, "tcp", c.opts.Address, c.opts.TLS)
	} else {
		conn, err = d.Dial("tcp", c.opts.Address)
	}
	if err != nil {
		return fmt.Errorf("tritium: connect %s: %w", c.opts.Address, err)
	}
	c.conn, c.r = conn, resp.NewReader(conn)
	if c.opts.Password != "" {
		if _, err := c.call("AUTH", c.opts.Password); err != nil {
			c.drop()
			return err
		}
	}
	return nil
}

func (c *Client) drop() {
	if c.conn != nil {
		c.conn.Close()
		c.conn, c.r = nil, nil
	}
}

// do runs one command, reconnecting first if the last call broke the
// connection.
func (c *Client) do(args ...string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return nil, err
		}
	}
	return c.call(args...)
}

func (c *Client) call(args ...string) (any, error) {
	c.conn.SetDeadline(time.Now().Add(c.opts.Timeout))
	v, err := resp.NewCommand(args...).Do(c.conn, c.r)
	if err != nil {
		var se *resp.ServerError
		if errors.As(err, &se) {
			return nil, fmt.Errorf("tritium: %s", se.Msg)
		}
		c.drop()
		return nil, fmt.Errorf("tritium: %s: %w", args[0], err)
	}
	return v, nil
}

// Set stores value under key. ttl is in seconds; nil uses the server's default.
func (c *Client) Set(key string, value []byte, ttl *int) error {
	if c.box != nil {
		value = c.box.seal(key, value)
	}
	var err error
	if ttl == nil {
		_, err = c.do("SET", key, string(value))
	} else {
		_, err = c.do("SETEX", key, strconv.Itoa(*ttl), string(value))
	}
	return err
}

// Get returns the value under key, or ErrNotFound.
func (c *Client) Get(key string) ([]byte, error) {
	v, err := c.do("GET", key)
	if err != nil {
		return nil, err
	}
	switch b := v.(type) {
	case nil:
		return nil, ErrNotFound
	case []byte:
		if c.box != nil {
			return c.box.open(key, b)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("tritium: GET: unexpected reply %T", v)
	}
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(key string) (bool, error) {
	v, err := c.do("DEL", key)
	if err != nil {
		return false, err
	}
	n, _ := v.(int64)
	return n > 0, nil
}

// Scan walks the keyspace one page at a time: start with cursor 0 and keep
// calling with the next it returns until that comes back 0. match is a glob
// pattern ("" scans everything); count is a hint for page size (0 uses the
// store's default). The cursor is opaque to the caller — hold whatever
// comes back and pass it straight through.
func (c *Client) Scan(cursor uint64, match string, count int) (keys []string, next uint64, err error) {
	args := []string{"SCAN", strconv.FormatUint(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.Itoa(count))
	}
	v, err := c.do(args...)
	if err != nil {
		return nil, 0, err
	}
	page, ok := v.([]any)
	if !ok || len(page) != 2 {
		return nil, 0, fmt.Errorf("tritium: SCAN: unexpected reply %T", v)
	}
	rawCursor, ok := page[0].([]byte)
	if !ok {
		return nil, 0, fmt.Errorf("tritium: SCAN: unexpected cursor %T", page[0])
	}
	if next, err = strconv.ParseUint(string(rawCursor), 10, 64); err != nil {
		return nil, 0, fmt.Errorf("tritium: SCAN: invalid cursor %q", rawCursor)
	}
	raw, ok := page[1].([]any)
	if !ok {
		return nil, 0, fmt.Errorf("tritium: SCAN: unexpected keys %T", page[1])
	}
	keys = make([]string, 0, len(raw))
	for _, k := range raw {
		b, ok := k.([]byte)
		if !ok {
			return nil, 0, fmt.Errorf("tritium: SCAN: unexpected key %T", k)
		}
		keys = append(keys, string(b))
	}
	return keys, next, nil
}

// Type returns the store's type name for key: "string", "zset", or "none"
// if it doesn't exist.
func (c *Client) Type(key string) (string, error) {
	v, err := c.do("TYPE", key)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("tritium: TYPE: unexpected reply %T", v)
	}
	return s, nil
}

// DBSize returns how many keys the node's local store currently holds.
func (c *Client) DBSize() (int64, error) {
	v, err := c.do("DBSIZE")
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("tritium: DBSIZE: unexpected reply %T", v)
	}
	return n, nil
}

// Nodes returns the node's view of the cluster, keyed by node ID.
func (c *Client) Nodes() (map[string]storage.NodeInfo, error) {
	v, err := c.do("TRITIUM.NODES")
	if err != nil {
		return nil, err
	}
	raw, _ := v.([]byte)
	var nodes map[string]storage.NodeInfo
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, fmt.Errorf("tritium: nodes: %w", err)
	}
	return nodes, nil
}

// Events reads and merges every node's cluster event log — attach, detach,
// hold, repair, stall, evict, resync, start — from this node's local store,
// oldest first: every node's writes replicate everywhere, so one read
// covers the whole fleet. nodeIDs, from Nodes(), names which logs to read;
// since bounds how far back to look.
func (c *Client) Events(nodeIDs []string, since time.Duration) ([]storage.Event, error) {
	min := strconv.FormatInt(time.Now().Add(-since).UnixMilli(), 10)
	var events []storage.Event
	for _, id := range nodeIDs {
		v, err := c.do("ZRANGEBYSCORE", storage.EventsKeyPrefix+id, min, "+inf")
		if err != nil {
			return nil, err
		}
		raw, _ := v.([]any)
		for _, m := range raw {
			b, ok := m.([]byte)
			if !ok {
				continue
			}
			var ev storage.Event
			if err := json.Unmarshal(b, &ev); err == nil {
				events = append(events, ev)
			}
		}
	}
	slices.SortFunc(events, func(a, b storage.Event) int { return cmp.Compare(a.At, b.At) })
	return events, nil
}

func (c *Client) Ping() error {
	_, err := c.do("PING")
	return err
}

// Do runs any command and returns the decoded reply: string, int64, []byte,
// []any or nil. Values are not sealed or opened; use Set and Get for that.
func (c *Client) Do(args ...string) (any, error) {
	return c.do(args...)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.r = nil, nil
	return err
}

// TLSConfig returns a client TLS config that verifies nodes against the PEM
// bundle at caFile, or against the system roots when caFile is empty.
func TLSConfig(caFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tritium: %w", err)
	}
	cfg.RootCAs = x509.NewCertPool()
	if !cfg.RootCAs.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tritium: no certificates found in %s", caFile)
	}
	return cfg, nil
}

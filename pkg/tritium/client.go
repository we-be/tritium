// Package tritium is the Go client for a tritium node.
package tritium

import (
	"fmt"
	"net"
	"net/rpc"
	"time"

	"github.com/we-be/tritium/pkg/storage"
)

// ErrNotFound is returned by Get for a missing or expired key.
var ErrNotFound = storage.ErrNotFound

// Client talks to one tritium node over its RPC port.
type Client struct {
	rpc *rpc.Client
}

type ClientOptions struct {
	Address string        // host:port of a node; default localhost:8080
	Timeout time.Duration // dial timeout; default 10s. Calls themselves have no deadline.
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
	}
	conn, err := net.DialTimeout("tcp", o.Address, o.Timeout)
	if err != nil {
		return nil, fmt.Errorf("tritium: connect %s: %w", o.Address, err)
	}
	return &Client{rpc: rpc.NewClient(conn)}, nil
}

// Set stores value under key. ttl is in seconds; nil uses the server's default.
func (c *Client) Set(key string, value []byte, ttl *int) error {
	var reply storage.SetReply
	if err := c.rpc.Call("Store.Set", &storage.SetArgs{Key: key, Value: value, TTL: ttl}, &reply); err != nil {
		return fmt.Errorf("tritium: set: %w", err)
	}
	if reply.Error != "" {
		return fmt.Errorf("tritium: set: %s", reply.Error)
	}
	return nil
}

// Get returns the value under key, or ErrNotFound.
func (c *Client) Get(key string) ([]byte, error) {
	var reply storage.GetReply
	if err := c.rpc.Call("Store.Get", &storage.GetArgs{Key: key}, &reply); err != nil {
		return nil, fmt.Errorf("tritium: get: %w", err)
	}
	switch reply.Error {
	case "":
		return reply.Value, nil
	case ErrNotFound.Error():
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("tritium: get: %s", reply.Error)
	}
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(key string) (bool, error) {
	var reply storage.DeleteReply
	if err := c.rpc.Call("Store.Delete", &storage.DeleteArgs{Key: key}, &reply); err != nil {
		return false, fmt.Errorf("tritium: delete: %w", err)
	}
	if reply.Error != "" {
		return false, fmt.Errorf("tritium: delete: %s", reply.Error)
	}
	return reply.Deleted, nil
}

// Nodes returns the node's view of the cluster, keyed by node ID.
func (c *Client) Nodes() (map[string]storage.NodeInfo, error) {
	var reply map[string]storage.NodeInfo
	if err := c.rpc.Call("Store.GetClusterNodes", struct{}{}, &reply); err != nil {
		return nil, fmt.Errorf("tritium: nodes: %w", err)
	}
	return reply, nil
}

func (c *Client) Close() error {
	return c.rpc.Close()
}

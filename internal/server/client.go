package server

import (
	"fmt"
	"net/rpc"
	"time"
)

// Client represents a Tritium RPC client
type Client struct {
	rpc *rpc.Client
}

// ClientOptions contains options for creating a new client
type ClientOptions struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a new Tritium client
func NewClient(opts *ClientOptions) (*Client, error) {
	if opts == nil {
		opts = &ClientOptions{
			Address: "localhost:8080",
			Timeout: 10 * time.Second,
		}
	}

	client, err := rpc.Dial("tcp", opts.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to tritium server: %w", err)
	}

	return &Client{
		rpc: client,
	}, nil
}

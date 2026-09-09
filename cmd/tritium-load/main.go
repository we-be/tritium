// Command tritium-load drives a node at a rate and reports latency
// percentiles per command and, with -peer, replication lag to another node.
//
//	tritium-load -config node.env -duration 10s -rate 500 -peer other:8080
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/we-be/tritium/internal/cli"
	"github.com/we-be/tritium/internal/load"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	node := cli.Flags(flag.CommandLine, "addr", "localhost:8080", "node address")
	peer := flag.String("peer", "", "another node's address: measure how long writes take to show up there (same password and CA)")
	rate := flag.Int("rate", 500, "operations per second across all connections; 0 = unbounded")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	conns := flag.Int("conns", 8, "concurrent connections")
	keys := flag.Int("keys", 1000, "key space")
	size := flag.Int("size", 64, "value size in bytes")
	ttl := flag.Int("ttl", 60, "seconds each written key lives")
	mix := flag.String("mix", "50:45:5", "relative weights of SET:GET:ZADD")
	flag.Parse()

	var o load.Options
	if n, err := fmt.Sscanf(*mix, "%d:%d:%d", &o.Mix[0], &o.Mix[1], &o.Mix[2]); n != 3 || err != nil {
		cli.Fail(fmt.Errorf("-mix wants SET:GET:ZADD weights, got %q", *mix))
	}
	o.Rate, o.Duration, o.Conns, o.Keys, o.Size, o.TTL = *rate, *duration, *conns, *keys, *size, *ttl

	opts, err := node.Options()
	if err != nil {
		cli.Fail(err)
	}
	var peerOpts *tritium.ClientOptions
	if *peer != "" {
		p := opts
		p.Address = *peer
		peerOpts = &p
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rep, err := load.Run(ctx, opts, peerOpts, o)
	if err != nil {
		cli.Fail(err)
	}
	fmt.Print(rep)
}

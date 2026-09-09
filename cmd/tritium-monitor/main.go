// Command tritium-monitor is a terminal dashboard for a cluster.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/we-be/tritium/internal/cli"
	"github.com/we-be/tritium/internal/monitor"
)

func main() {
	node := cli.Flags(flag.CommandLine, "nodes", "localhost:8080,localhost:8081,localhost:8082", "comma-separated node addresses; the first that answers is used")
	summary := flag.Bool("summary", false, "one line per node")
	interval := flag.Duration("interval", 2*time.Second, "refresh interval")
	flag.Parse()
	opts, err := node.Options()
	if err != nil {
		cli.Fail(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := monitor.New(strings.Split(opts.Address, ","), opts)
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		monitor.Render(os.Stdout, m.Snapshot(), *summary, time.Now(), *interval)
		select {
		case <-ctx.Done():
			os.Stdout.WriteString("\n")
			return
		case <-ticker.C:
		}
	}
}

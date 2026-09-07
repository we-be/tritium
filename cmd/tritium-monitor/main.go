// Command tritium-monitor is a terminal dashboard for a cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/monitor"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	configPath := flag.String("config", "", "a node's dotenv file: fills -nodes, -password and -ca from it (explicit flags win)")
	nodes := flag.String("nodes", "localhost:8080,localhost:8081,localhost:8082", "comma-separated node addresses; the first that answers is used")
	password := flag.String("password", "", "AUTH password")
	useTLS := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "PEM bundle to verify nodes against (implies -tls)")
	summary := flag.Bool("summary", false, "one line per node")
	interval := flag.Duration("interval", 2*time.Second, "refresh interval")
	flag.Parse()
	if *configPath != "" {
		loc, err := config.LoadLocal(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "tritium-monitor:", err)
			os.Exit(1)
		}
		set := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
		if !set["nodes"] {
			*nodes = loc.Addr
		}
		if !set["password"] {
			*password = loc.Password
		}
		if !set["ca"] && loc.CA != "" {
			*ca = loc.CA
		}
	}

	opts := tritium.ClientOptions{Password: *password}
	if *useTLS || *ca != "" {
		var err error
		if opts.TLS, err = tritium.TLSConfig(*ca); err != nil {
			fmt.Fprintln(os.Stderr, "tritium-monitor:", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := monitor.New(strings.Split(*nodes, ","), opts)
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

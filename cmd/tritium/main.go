// Command tritium runs one node: an RPC server in front of a RESP store.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/server"
)

const joinAttempts = 30 // one per second; covers a seed node that is still starting

func main() {
	configFile := flag.String("config", "", "dotenv file to load (default: .env if present); the environment overrides it")
	flag.Parse()

	path := *configFile
	if path == "" {
		if _, err := os.Stat(".env"); err == nil {
			path = ".env"
		}
	}
	cfg, err := config.Load(path)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	srv, err := server.New(cfg)
	if err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}
	if err := srv.Start(cfg.ListenAddr); err != nil {
		slog.Error("listen", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}
	slog.Info("listening", "addr", srv.Addr(), "store", cfg.StoreAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.JoinAddr != "" {
		join(ctx, srv, cfg.JoinAddr)
	} else {
		slog.Info("seeding a new cluster")
	}

	<-ctx.Done()
	slog.Info("shutting down")
	if err := srv.Stop(); err != nil {
		slog.Error("shutdown", "err", err)
	}
}

func join(ctx context.Context, srv *server.Server, addr string) {
	for i := 1; ; i++ {
		err := srv.Join(addr)
		if err == nil {
			slog.Info("joined cluster", "via", addr)
			return
		}
		if i == joinAttempts {
			slog.Error("giving up joining; running standalone", "via", addr, "err", err)
			return
		}
		slog.Warn("join failed, retrying", "via", addr, "attempt", i, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// Command tritium runs one node: a RESP server in front of a RESP store.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/server"
)

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

	if len(cfg.Seeds()) == 0 {
		slog.Info("seeding a new cluster")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	slog.Info("shutting down")
	if err := srv.Stop(); err != nil {
		slog.Error("shutdown", "err", err)
	}
}

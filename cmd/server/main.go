package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/server"
)

func main() {
	configFile := flag.String("config", ".env", "Path to config file")
	flag.Parse()

	cfg, err := config.NewConfigFromDotenv(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 1) Listen first
	listener, err := net.Listen("tcp", cfg.RPCAddr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", cfg.RPCAddr, err)
	}
	actualAddr := listener.Addr().String()
	fmt.Printf("Listening on %s\n", actualAddr)

	// 2) Create the Server struct (without calling initCluster yet)
	srv, err := server.NewServerBare() // a constructor that does NOT call initCluster
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// 3) Now that we know the final address, init the cluster
	if err := srv.InitCluster(actualAddr, cfg.MemStoreAddr); err != nil {
		log.Fatalf("Failed to init cluster: %v", err)
	}

	// 4) If we have a join address, join the cluster
	if cfg.JoinAddr != "" {
		fmt.Printf("Joining cluster at %s\n", cfg.JoinAddr)
		if err := srv.JoinCluster(cfg.JoinAddr); err != nil {
			log.Printf("JoinCluster error: %v", err)
		} else {
			fmt.Println("Joined cluster successfully.")
		}
	} else {
		// If there's no join address, we can consider this node the "leader"
		fmt.Println("Starting as cluster leader.")
	}

	// 5) Start accepting RPC connections on the already-open listener
	if err := srv.StartWithListener(listener); err != nil {
		log.Fatalf("Failed to start RPC accept loop: %v", err)
	}

	fmt.Printf("Server is running on %s\n", srv.GetAddress())
	fmt.Printf("Using memory store at %s\n", cfg.MemStoreAddr)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}

// Command example is pkg/tritium from outside the module: OptionsFromEnv
// reads the same dotenv file the node itself reads, so a tool run next to a
// node needs no configuration of its own, and Scan walks the keyspace a page
// at a time instead of the unbounded KEYS the protocol refuses to offer.
//
// This directory is its own Go module (see go.mod's replace directive) so it
// can sit in the tree without pulling anything into the root module, which
// has no dependencies on purpose.
//
//	go run ./cmd/tritium &                  # a node on localhost:8080, from the repo root
//	go run ./examples/go -config ../../.env # or wherever the node's dotenv file is
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	configPath := flag.String("config", ".env", "a node's dotenv file")
	flag.Parse()

	opts, err := tritium.OptionsFromEnv(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	client, err := tritium.NewClient(&opts)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("example:hello", []byte("world"), nil); err != nil {
		log.Fatal(err)
	}

	// SCAN's cursor is opaque: start at 0, keep calling with what comes back
	// until that's 0 again. Never handed the same key twice, unlike KEYS.
	var cursor uint64
	for {
		keys, next, err := client.Scan(cursor, "example:*", 100)
		if err != nil {
			log.Fatal(err)
		}
		for _, k := range keys {
			fmt.Println(k)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

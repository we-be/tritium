package tritium_test

import (
	"fmt"
	"log"

	"github.com/we-be/tritium/pkg/tritium"
)

func Example() {
	client, err := tritium.NewClient(&tritium.ClientOptions{Address: "localhost:8080"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Set("hello", []byte("world"), new(3600)); err != nil { // TTL in seconds
		log.Fatal(err)
	}
	value, err := client.Get("hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", value)
}

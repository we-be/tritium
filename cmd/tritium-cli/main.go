// Command tritium-cli talks to a node from the shell. valkey-cli and
// redis-cli work too; this one adds the cluster view.
//
//	tritium-cli [flags] get KEY
//	tritium-cli [flags] set [-ttl SECONDS] KEY VALUE
//	tritium-cli [flags] del KEY
//	tritium-cli [flags] nodes
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/we-be/tritium/pkg/storage"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "node address")
	password := flag.String("password", "", "AUTH password")
	useTLS := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "PEM bundle to verify the node against (implies -tls)")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}

	opts := tritium.ClientOptions{Address: *addr, Timeout: 5 * time.Second, Password: *password}
	if *useTLS || *ca != "" {
		var err error
		if opts.TLS, err = tritium.TLSConfig(*ca); err != nil {
			fail(err)
		}
	}
	client, err := tritium.NewClient(&opts)
	if err != nil {
		fail(err)
	}
	defer client.Close()

	if err := run(client, flag.Arg(0), flag.Args()[1:]); err != nil {
		fail(err)
	}
}

func run(client *tritium.Client, cmd string, args []string) error {
	switch cmd {
	case "get":
		if len(args) != 1 {
			return errors.New("usage: get KEY")
		}
		v, err := client.Get(args[0])
		if err != nil {
			return err
		}
		os.Stdout.Write(v)
		if len(v) == 0 || v[len(v)-1] != '\n' {
			fmt.Println()
		}
	case "set":
		fs := flag.NewFlagSet("set", flag.ContinueOnError)
		ttl := fs.Int("ttl", 0, "seconds until the key expires (0: server default)")
		if err := fs.Parse(args); err != nil || fs.NArg() != 2 {
			return errors.New("usage: set [-ttl SECONDS] KEY VALUE")
		}
		var ttlArg *int
		if *ttl > 0 {
			ttlArg = ttl
		}
		return client.Set(fs.Arg(0), []byte(fs.Arg(1)), ttlArg)
	case "del":
		if len(args) != 1 {
			return errors.New("usage: del KEY")
		}
		ok, err := client.Delete(args[0])
		if err != nil {
			return err
		}
		fmt.Println(map[bool]string{true: "deleted", false: "not found"}[ok])
	case "nodes":
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		printNodes(nodes)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func printNodes(nodes map[string]storage.NodeInfo) {
	rows := slices.SortedFunc(func(yield func(storage.NodeInfo) bool) {
		for _, n := range nodes {
			if !yield(n) {
				return
			}
		}
	}, func(a, b storage.NodeInfo) int { return strings.Compare(a.Addr, b.Addr) })

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ADDRESS\tSTORE\tSTATE\tSEED\tCONNS\tLAST SEEN")
	for _, n := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%v\t%d\t%s\n",
			n.Addr, n.StoreAddr, n.State, n.IsLeader, n.Stats.ActiveConnections, time.Since(n.LastSeen).Round(time.Second))
	}
	w.Flush()
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tritium-cli [flags] get KEY | set [-ttl SECONDS] KEY VALUE | del KEY | nodes")
	flag.PrintDefaults()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tritium-cli:", err)
	os.Exit(1)
}

// Command tritium-cli talks to a node from the shell. valkey-cli and
// redis-cli work too; this one adds the cluster view and client-side
// encryption.
//
//	tritium-cli [flags] get KEY
//	tritium-cli [flags] set [-ttl SECONDS] KEY VALUE
//	tritium-cli [flags] del KEY
//	tritium-cli [flags] scan [PATTERN]
//	tritium-cli [flags] nodes
//	tritium-cli [flags] events [-since 1h] [-node NAME]
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/pkg/storage"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	var configTLS bool // the node the -config file describes serves TLS
	configPath := flag.String("config", "", "a node's dotenv file: fills -addr, -password and -ca from it (explicit flags win)")
	addr := flag.String("addr", "localhost:8080", "node address")
	password := flag.String("password", os.Getenv("TRITIUM_PASSWORD"), "AUTH password (default $TRITIUM_PASSWORD, which keeps it off the command line)")
	user := flag.String("user", os.Getenv("TRITIUM_USER"), "AUTH as this user instead of the default one (default $TRITIUM_USER)")
	useTLS := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "PEM bundle to verify the node against (implies -tls)")
	key := flag.String("key", os.Getenv("TRITIUM_KEY"), "32-byte encryption key as hex or base64; values are sealed client-side (default $TRITIUM_KEY)")
	flag.Usage = usage
	flag.Parse()
	if *configPath != "" {
		loc, err := config.LoadLocal(*configPath)
		if err != nil {
			fail(err)
		}
		configTLS = loc.TLS
		set := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
		if !set["addr"] {
			*addr = loc.Addr
		}
		if !set["password"] {
			*password = loc.Password
		}
		if !set["user"] && *user == "" {
			*user = loc.User
		}
		if !set["ca"] && loc.CA != "" {
			*ca = loc.CA
		}
	}
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}

	opts := tritium.ClientOptions{Address: *addr, Timeout: 5 * time.Second, User: *user, Password: *password}
	if *key != "" {
		var err error
		if opts.Key, err = tritium.ParseKey(*key); err != nil {
			fail(err)
		}
	}
	if *useTLS || *ca != "" || configTLS {
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
		typ, err := client.Type(args[0])
		if err != nil {
			return err
		}
		if typ == "zset" {
			return printZSet(client, args[0])
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
	case "scan":
		if len(args) > 1 {
			return errors.New("usage: scan [PATTERN]")
		}
		pattern := "*"
		if len(args) == 1 {
			pattern = args[0]
		}
		return scanKeys(client, pattern)
	case "nodes":
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		printNodes(nodes)
	case "info":
		v, err := client.Do(append([]string{"INFO"}, args...)...)
		if err != nil {
			return err
		}
		fmt.Print(bulk(v))
	case "events":
		fs := flag.NewFlagSet("events", flag.ContinueOnError)
		since := fs.Duration("since", 24*time.Hour, "how far back to look")
		node := fs.String("node", "", "only a node whose id contains this")
		if err := fs.Parse(args); err != nil {
			return err
		}
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(nodes))
		for id := range nodes {
			ids = append(ids, id)
		}
		events, err := client.Events(ids, *since)
		if err != nil {
			return err
		}
		printEvents(events, *node)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

// scanKeys walks every SCAN page for pattern and prints each key with its
// type and TTL — what KEYS would show, without the O(n) footgun.
// printZSet lists a sorted set (a mubs board, the fleet index) as one
// "score<TAB>member" line each, lowest score first, since GET refuses the type.
func printZSet(client *tritium.Client, key string) error {
	v, err := client.Do("ZRANGEBYSCORE", key, "-inf", "+inf", "WITHSCORES")
	if err != nil {
		return err
	}
	items, _ := v.([]any)
	for i := 0; i+1 < len(items); i += 2 {
		fmt.Printf("%s\t%s\n", bulk(items[i+1]), bulk(items[i]))
	}
	return nil
}

func bulk(v any) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}

func scanKeys(client *tritium.Client, pattern string) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tTYPE\tTTL")
	var cursor uint64
	for {
		keys, next, err := client.Scan(cursor, pattern, 200)
		if err != nil {
			return err
		}
		for _, k := range keys {
			typ, err := client.Type(k)
			if err != nil {
				return err
			}
			ttl, err := client.Do("TTL", k)
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "%s\t%s\t%v\n", k, typ, ttl)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return w.Flush()
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
	fmt.Fprintln(w, "ADDRESS\tSTATE\tVERSION\tSEEDS\tREPLICAS\tCONNS\tLAST SEEN")
	for _, n := range rows {
		replicas := strconv.Itoa(n.Stats.Replicas)
		if n.Stats.Held > 0 {
			replicas += fmt.Sprintf(" (%d held)", n.Stats.Held)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
			n.Addr, n.State, n.Version, strings.Join(n.Seeds, ","), replicas, n.Stats.ActiveConnections, time.Since(n.LastSeen).Round(time.Second))
	}
	w.Flush()
}

// printEvents prints one line per event, oldest first, filtered to nodes
// whose ID contains node when it is set.
func printEvents(events []storage.Event, node string) {
	for _, e := range events {
		if node != "" && !strings.Contains(e.Node, node) {
			continue
		}
		line := fmt.Sprintf("%s  %-8s node=%s", time.UnixMilli(e.At).Format(time.RFC3339), e.Event, e.Node)
		if e.Peer != "" {
			line += " peer=" + e.Peer
		}
		if e.Keys > 0 {
			line += fmt.Sprintf(" keys=%d", e.Keys)
		}
		if e.Took > 0 {
			line += " took=" + (time.Duration(e.Took) * time.Millisecond).String()
		}
		fmt.Println(line)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tritium-cli [flags] get KEY | set [-ttl SECONDS] KEY VALUE | del KEY | scan [PATTERN] | nodes | info [SECTION] | events [-since 1h] [-node NAME]")
	flag.PrintDefaults()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tritium-cli:", err)
	os.Exit(1)
}

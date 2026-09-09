// Command tritium-cli talks to a node from the shell. valkey-cli and
// redis-cli work too; this one adds the cluster view and client-side
// encryption.
//
//	tritium-cli [flags] get KEY
//	tritium-cli [flags] set [-ttl SECONDS] KEY VALUE
//	tritium-cli [flags] del KEY
//	tritium-cli [flags] scan [PATTERN]
//	tritium-cli [flags] where KEY
//	tritium-cli [flags] prefixes
//	tritium-cli [flags] nodes
//	tritium-cli [flags] events [-since 1h] [-node NAME]
package main

import (
	"cmp"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/we-be/tritium/internal/cli"
	"github.com/we-be/tritium/pkg/storage"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	node := cli.Flags(flag.CommandLine, "addr", "localhost:8080", "node address")
	key := flag.String("key", os.Getenv("TRITIUM_KEY"), "32-byte encryption key as hex or base64; values are sealed client-side (default $TRITIUM_KEY)")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}
	opts, err := node.Options()
	if err != nil {
		cli.Fail(err)
	}
	if *key != "" {
		if opts.Key, err = tritium.ParseKey(*key); err != nil {
			cli.Fail(err)
		}
	}
	client, err := tritium.NewClient(&opts)
	if err != nil {
		cli.Fail(err)
	}
	defer client.Close()

	if err := run(client, opts, flag.Arg(0), flag.Args()[1:]); err != nil {
		cli.Fail(err)
	}
}

func run(client *tritium.Client, opts tritium.ClientOptions, cmd string, args []string) error {
	switch cmd {
	case "where":
		if len(args) != 1 {
			return errors.New("usage: where KEY")
		}
		return where(client, opts, args[0])
	case "prefixes":
		return prefixes(os.Stdout, client, opts)
	case "get":
		if len(args) != 1 {
			return errors.New("usage: get KEY")
		}
		return get(client, args[0])
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
	case "clients":
		v, err := client.Do("CLIENT", "LIST")
		if err != nil {
			return err
		}
		fmt.Print(bulk(v))
	case "events":
		return events(client, args)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

// get prints a string as stored, with a newline unless it ends in one, or
// a sorted set one member per line.
func get(client *tritium.Client, key string) error {
	typ, err := client.Type(key)
	if err != nil {
		return err
	}
	if typ == "zset" {
		return printZSet(client, key)
	}
	v, err := client.Get(key)
	if err != nil {
		return err
	}
	os.Stdout.Write(v)
	if len(v) == 0 || v[len(v)-1] != '\n' {
		fmt.Println()
	}
	return nil
}

// events prints the fleet's log from this node's store: every node's
// entries since -since, oldest first, narrowed to one node by -node.
func events(client *tritium.Client, args []string) error {
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
	events, err := client.Events(slices.Collect(maps.Keys(nodes)), *since)
	if err != nil {
		return err
	}
	printEvents(events, *node)
	return nil
}

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

// scanKeys walks every SCAN page for pattern and prints each key with its
// type and TTL — what KEYS would show, without the O(n) footgun.
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
	fmt.Fprintln(w, "ADDRESS\tSTATE\tVERSION\tWEIGHT\tSEEDS\tREPLICAS\tKEYS\tMEM\tWRITES\tCONNS\tLAST SEEN")
	for _, n := range rows {
		replicas := strconv.Itoa(n.Stats.Replicas)
		if n.Stats.Held > 0 {
			replicas += fmt.Sprintf(" (%d held)", n.Stats.Held)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\t%d\t%s\t%d\t%d\t%s\n",
			n.Addr, n.State, n.Version, n.Weight(), strings.Join(n.Seeds, ","), replicas, n.Stats.Keys, mem(n.Stats.Memory), n.Stats.Writes, n.Stats.ActiveConnections, time.Since(n.LastSeen).Round(time.Second))
	}
	w.Flush()
}

// where asks every healthy node in the view for the key — its type, its
// TTL and a digest of what it holds — so a key that differs between nodes,
// or is missing from one, shows. Each node is reached with the same
// credentials as the first, so a node with its own password says so.
func where(client *tritium.Client, opts tritium.ClientOptions, key string) error {
	rows, err := view(client)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tTYPE\tTTL\tHOLDS")
	for _, n := range rows {
		if n.State != storage.NodeStateHealthy {
			fmt.Fprintf(w, "%s\t%s\t\t\n", n.Addr, n.State)
			continue
		}
		o := opts
		o.Address = n.Addr
		c, err := tritium.NewClient(&o)
		if err != nil {
			fmt.Fprintf(w, "%s\tunreachable\t\t%s\n", n.Addr, err)
			continue
		}
		typ, ttl, holds := lookup(c, key)
		c.Close()
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", n.Addr, typ, ttl, holds)
	}
	return w.Flush()
}

// view is the cluster as this node sees it, by address.
func view(client *tritium.Client) ([]storage.NodeInfo, error) {
	nodes, err := client.Nodes()
	if err != nil {
		return nil, err
	}
	return slices.SortedFunc(maps.Values(nodes), func(a, b storage.NodeInfo) int { return strings.Compare(a.Addr, b.Addr) }), nil
}

// prefixes counts every node's keys by prefix and type: the keyspace
// census. A prefix is what a right names, so the rows are what the
// keyspace would partition into as surfaces; a bare key is a row of its
// own, since a right names it exactly. Two SCAN walks per node, one per
// type, so a node is never asked about keys one at a time.
func prefixes(w io.Writer, client *tritium.Client, opts tritium.ClientOptions) error {
	rows, err := view(client)
	if err != nil {
		return err
	}
	type cell struct{ prefix, typ string }
	counts := map[cell][]int{}
	failed := make([]error, len(rows))
	for i, n := range rows {
		if n.State != storage.NodeStateHealthy {
			failed[i] = errors.New(string(n.State))
			continue
		}
		o := opts
		o.Address = n.Addr
		c, err := tritium.NewClient(&o)
		if err != nil {
			failed[i] = err
			continue
		}
		for _, typ := range []string{"string", "zset"} {
			err = keysOfType(c, typ, func(key string) {
				k := cell{prefixOf(key), typ}
				if counts[k] == nil {
					counts[k] = make([]int, len(rows))
				}
				counts[k][i]++
			})
			if err != nil {
				failed[i] = err
				break
			}
		}
		c.Close()
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprint(tw, "PREFIX\tTYPE")
	for _, n := range rows {
		fmt.Fprint(tw, "\t", n.Addr)
	}
	fmt.Fprintln(tw)
	cells := slices.SortedFunc(maps.Keys(counts), func(a, b cell) int {
		return cmp.Or(strings.Compare(a.prefix, b.prefix), strings.Compare(a.typ, b.typ))
	})
	total := make([]int, len(rows))
	for _, k := range cells {
		fmt.Fprintf(tw, "%s\t%s", k.prefix, k.typ)
		for i, n := range counts[k] {
			fmt.Fprint(tw, "\t", count(n, failed[i]))
			total[i] += n
		}
		fmt.Fprintln(tw)
	}
	fmt.Fprint(tw, "TOTAL\t")
	for i, n := range total {
		fmt.Fprint(tw, "\t", count(n, failed[i]))
	}
	fmt.Fprintln(tw)
	if err := tw.Flush(); err != nil {
		return err
	}
	for i, err := range failed {
		if err != nil {
			fmt.Fprintf(w, "%s: %v\n", rows[i].Addr, err)
		}
	}
	return nil
}

// count renders one census cell: a node that could not be walked shows "?"
// rather than a number that would read as zero keys.
func count(n int, failed error) string {
	if failed != nil {
		return "?"
	}
	return strconv.Itoa(n)
}

// prefixOf is the part of a key a prefix right would name: up to and
// including its first colon, or the whole key when it has none.
func prefixOf(key string) string {
	if i := strings.IndexByte(key, ':'); i >= 0 {
		return key[:i+1]
	}
	return key
}

// keysOfType walks every SCAN page of one type and hands each key to fn.
func keysOfType(c *tritium.Client, typ string, fn func(string)) error {
	cursor := "0"
	for {
		v, err := c.Do("SCAN", cursor, "COUNT", "200", "TYPE", typ)
		if err != nil {
			return err
		}
		page, _ := v.([]any)
		if len(page) != 2 {
			return fmt.Errorf("SCAN: unexpected reply %T", v)
		}
		keys, _ := page[1].([]any)
		for _, k := range keys {
			fn(bulk(k))
		}
		if cursor = bulk(page[0]); cursor == "0" {
			return nil
		}
	}
}

// lookup is one node's answer about a key: its type, its TTL and what it
// holds — the first bytes of a digest of a string's stored value, sealed or
// not, so two nodes that hold the same bytes print the same; a sorted set's
// member count.
func lookup(c *tritium.Client, key string) (typ, ttl, holds string) {
	t, err := c.Type(key)
	if err != nil {
		return "error", "", err.Error()
	}
	if t == "none" {
		return "none", "", ""
	}
	if v, err := c.Do("TTL", key); err == nil {
		if n, ok := v.(int64); ok && n >= 0 {
			ttl = (time.Duration(n) * time.Second).String()
		}
	}
	switch t {
	case "string":
		v, err := c.Do("GET", key)
		if b, ok := v.([]byte); err == nil && ok {
			sum := sha256.Sum256(b)
			holds = fmt.Sprintf("%d bytes, %x", len(b), sum[:4])
		}
	case "zset":
		if v, err := c.Do("ZCARD", key); err == nil {
			holds = fmt.Sprintf("%v members", v)
		}
	}
	return t, ttl, holds
}

// mem renders bytes the way a glance wants them.
func mem(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0fK", float64(n)/(1<<10))
	}
	return strconv.FormatInt(n, 10)
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
	fmt.Fprintln(os.Stderr, "usage: tritium-cli [flags] get KEY | set [-ttl SECONDS] KEY VALUE | del KEY | scan [PATTERN] | where KEY | prefixes | nodes | info [SECTION] | clients | events [-since 1h] [-node NAME]")
	flag.PrintDefaults()
}

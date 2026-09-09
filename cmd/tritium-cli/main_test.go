package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/internal/server"
	"github.com/we-be/tritium/pkg/tritium"
)

// The census counts a node's keys by prefix and type, and a bare key is a
// row of its own, since a right names it exactly.
func TestPrefixes(t *testing.T) {
	srv, err := server.New(config.Config{StoreAddr: resptest.Addr(t), PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })
	opts := tritium.ClientOptions{Address: srv.Addr()}
	c, err := tritium.NewClient(&opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, k := range []string{"census:a", "census:b", "censusnode:x"} {
		if err := c.Set(k, []byte("v"), nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"census:z", "censusbare"} {
		if _, err := c.Do("ZADD", k, "1", "m"); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	if err := prefixes(&out, c, opts); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, line := range strings.Split(out.String(), "\n")[1:] {
		if f := strings.Fields(line); len(f) == 3 {
			got[f[0]+" "+f[1]] = f[2]
		}
	}
	for _, want := range []string{"census: string 2", "census: zset 1", "censusnode: string 1", "censusbare zset 1"} {
		k, n, _ := strings.Cut(want, " ")
		k, n = k+" "+strings.Fields(n)[0], strings.Fields(n)[1]
		if got[k] != n {
			t.Fatalf("%s: got %q, want %s\n%s", k, got[k], n, out.String())
		}
	}
}

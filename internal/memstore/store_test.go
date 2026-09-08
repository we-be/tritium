package memstore

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/resp"
)

// do runs one command on s and decodes the reply.
func do(t *testing.T, s *Store, args ...string) any {
	t.Helper()
	r := resp.NewReader(strings.NewReader(string(s.exec(nil, args))))
	v, err := r.ReadValue()
	if err != nil {
		return err
	}
	return v
}

// TestScanWalksEveryKeyOnce: a full walk with MATCH sees each matching key exactly once.
func TestScanWalksEveryKeyOnce(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	for i := range 3000 {
		do(t, s, "SET", fmt.Sprintf("k:%d", i), "v")
	}
	do(t, s, "ZADD", "k:z", "1", "a")
	seen := map[string]int{}
	cursor, pages := "0", 0
	for {
		v := do(t, s, "SCAN", cursor, "MATCH", "k:1*", "COUNT", "100").([]any)
		for _, k := range v[1].([]any) {
			seen[string(k.([]byte))]++
		}
		pages++
		if cursor = string(v[0].([]byte)); cursor == "0" {
			break
		}
	}
	if len(seen) != 1111 || pages < 2 { // k:1, k:10-19, k:100-199, k:1000-1999
		t.Fatalf("saw %d keys in %d pages, want 1111 in several", len(seen), pages)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("%s seen %d times", k, n)
		}
	}
	if v := do(t, s, "SCAN", "0", "TYPE", "zset", "COUNT", "5000").([]any)[1].([]any); len(v) != 1 {
		t.Fatalf("TYPE zset returned %d keys", len(v))
	}
}

// TestExpiryReturnsMemory: an expired key is gone from every command and its bytes are freed.
func TestExpiryReturnsMemory(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	now := time.Now()
	s.now = func() time.Time { return now }
	do(t, s, "SET", "a", "value", "EX", "10")
	do(t, s, "ZADD", "z", "1", "m")
	do(t, s, "EXPIRE", "z", "5")
	if s.used == 0 || do(t, s, "TTL", "a") != int64(10) {
		t.Fatalf("used %d, ttl %v", s.used, do(t, s, "TTL", "a"))
	}
	now = now.Add(11 * time.Second)
	s.mu.Lock()
	s.sweep(100)
	s.mu.Unlock()
	if s.used != 0 || len(s.kv) != 0 || do(t, s, "DBSIZE") != int64(0) || do(t, s, "GET", "a") != nil {
		t.Fatalf("after expiry: used %d, %d keys", s.used, len(s.kv))
	}
}

// TestMaxMemoryEvictsSoonestExpiring: past the limit the key expiring first goes; without an expiring key the write is refused.
func TestMaxMemoryEvictsSoonestExpiring(t *testing.T) {
	s := New(Options{MaxMemory: 3 * (keyOverhead + 6)})
	defer s.Close()
	do(t, s, "SET", "aaaaa", "1", "EX", "100")
	do(t, s, "SET", "bbbbb", "1", "EX", "10")
	do(t, s, "SET", "ccccc", "1", "EX", "50")
	if v := do(t, s, "SET", "ddddd", "1"); v != "OK" {
		t.Fatalf("SET under the limit: %v", v)
	}
	if do(t, s, "GET", "bbbbb") != nil || do(t, s, "GET", "aaaaa") == nil {
		t.Fatal("the soonest-expiring key should have gone first")
	}
	do(t, s, "SET", "eeeee", "1") // evicts ccccc, then aaaaa on the next
	do(t, s, "SET", "fffff", "1")
	if v := do(t, s, "SET", "ggggg", "1"); v == "OK" {
		t.Fatal("nothing expiring is left: the write must be refused")
	}
}

// TestPipeBatchDoesNotDeadlock: a pipelined batch larger than any buffer is written whole before a reply is read.
func TestPipeBatchDoesNotDeadlock(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	ln := Listen()
	defer ln.Close()
	go s.Serve(ln)
	c, err := ln.Dial()
	if err != nil {
		t.Fatal(err)
	}
	var buf []byte
	const n = 2000
	for i := range n {
		buf = append(buf, resp.NewCommand("SET", fmt.Sprintf("key-%d", i), strings.Repeat("x", 1000))...)
	}
	if _, err := c.Write(buf); err != nil {
		t.Fatal(err)
	}
	r := resp.NewReader(c)
	for range n {
		if err := r.ReadOK(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestMatch: the glob's forms, including a * that crosses a slash.
func TestMatch(t *testing.T) {
	for _, tc := range []struct {
		p, s string
		want bool
	}{
		{"*", "", true}, {"k:*", "k:1/2", true}, {"k:?", "k:12", false}, {"[a-c]x", "bx", true},
		{"[^a-c]x", "bx", false}, {"\\*", "*", true}, {"\\*", "a", false}, {"a*b*c", "aXXbYYc", true}, {"a*b*c", "aXXbYY", false},
		{"*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b", strings.Repeat("a", 200), false}, // exponential in a recursive matcher
		{"k:*:[0-9]", "k:x/y:7", true}, {"k:*", "", false}, {"", "", true},
	} {
		start := time.Now()
		if got := match(tc.p, tc.s); got != tc.want {
			t.Errorf("match(%q, %q) = %v", tc.p, tc.s, got)
		}
		if time.Since(start) > 50*time.Millisecond {
			t.Errorf("match(%q, …) took %v", tc.p, time.Since(start))
		}
	}
}

// TestZRemRangeByRank: a cap of "everything but the last N" removes nothing while under N, then the oldest.
func TestZRemRangeByRank(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	do(t, s, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	if n := do(t, s, "ZREMRANGEBYRANK", "z", "0", "-5"); n != int64(0) {
		t.Fatalf("removed %v under the cap", n)
	}
	if n := do(t, s, "ZREMRANGEBYRANK", "z", "0", "-3"); n != int64(1) || do(t, s, "ZCARD", "z") != int64(2) {
		t.Fatalf("removed %v, want the oldest one", n)
	}
}

// TestStampedWritesSettle: the newer stamp wins whichever order writes arrive, a delete keeps an older write from returning, and a set overrides an older delete.
func TestStampedWritesSettle(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	do(t, s, "STAMPED", "20", "SET", "k", "new", "EX", "60")
	do(t, s, "STAMPED", "10", "SET", "k", "old", "EX", "60")
	if v := do(t, s, "GET", "k"); string(v.([]byte)) != "new" || do(t, s, "STAMPOF", "k") != int64(20) {
		t.Fatalf("older write landed: %s", v)
	}
	do(t, s, "STAMPED", "30", "DEL", "k")
	do(t, s, "STAMPED", "25", "SET", "k", "late", "EX", "60")
	if do(t, s, "GET", "k") != nil || do(t, s, "STAMPOF", "k") != int64(30) {
		t.Fatal("a write older than the delete came back")
	}
	do(t, s, "STAMPED", "40", "SET", "k", "again", "EX", "60")
	if v := do(t, s, "GET", "k"); string(v.([]byte)) != "again" || len(s.tomb) != 0 {
		t.Fatalf("a newer write after the delete: %v, %d tombstones", v, len(s.tomb))
	}
	do(t, s, "STAMPED", "5", "ZADD", "z", "1", "a") // sorted sets are not stamped
	if do(t, s, "ZCARD", "z") != int64(1) {
		t.Fatal("a stamped sorted-set write was refused")
	}
}

// TestScanPageIsBounded: COUNT is a hint up to a limit, never the whole keyspace.
func TestScanPageIsBounded(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	for i := range 30000 {
		do(t, s, "SET", "big:"+strconv.Itoa(i), "v")
	}
	page := do(t, s, "SCAN", "0", "COUNT", "2000000000").([]any)
	if n := len(page[1].([]any)); n >= 30000 || string(page[0].([]byte)) == "0" {
		t.Fatalf("one page held %d keys and cursor %s", n, page[0])
	}
}

// TestGetWrongType: GET and GETDEL on a sorted set are WRONGTYPE as in Redis, not a missing key; MGET still answers nil for it.
func TestGetWrongType(t *testing.T) {
	s := New(Options{})
	defer s.Close()
	do(t, s, "ZADD", "z", "1", "a")
	if err, ok := do(t, s, "GET", "z").(error); !ok || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("GET on a sorted set: %v", err)
	}
	if _, ok := do(t, s, "GETDEL", "z").(error); !ok || do(t, s, "ZCARD", "z") != int64(1) {
		t.Fatal("GETDEL on a sorted set was not refused")
	}
	if v := do(t, s, "MGET", "z").([]any); v[0] != nil {
		t.Fatalf("MGET on a sorted set: %v", v)
	}
}

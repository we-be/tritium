package storage_test

import (
	"errors"
	"fmt"
	"github.com/we-be/tritium/internal/resp"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

func TestStoreCommands(t *testing.T) {
	s, err := storage.NewStore(resptest.Addr(t), 2, "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Set("storage:k", []byte("v"), 60); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get("storage:k"); err != nil || string(v) != "v" {
		t.Fatalf("get: %q, %v", v, err)
	}
	if _, err := s.Get("storage:missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing key: got %v, want ErrNotFound", err)
	}
	// An empty value is a value, not a missing key.
	if err := s.Set("storage:empty", nil, 60); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get("storage:empty"); err != nil || v == nil {
		t.Fatalf("empty value: %#v, %v", v, err)
	}
	if n, err := s.Exists("storage:k", "storage:empty", "storage:missing"); err != nil || n != 2 {
		t.Fatalf("exists: %d, %v", n, err)
	}
	if ttl, err := s.TTL("storage:k"); err != nil || ttl <= 0 || ttl > 60 {
		t.Fatalf("ttl: %d, %v", ttl, err)
	}
	if n, err := s.Delete("storage:k", "storage:empty", "storage:missing"); err != nil || n != 2 {
		t.Fatalf("delete: %d, %v", n, err)
	}
	if _, err := s.Get("storage:k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("after delete: got %v, want ErrNotFound", err)
	}
}

func TestStoreReplicates(t *testing.T) {
	primary, err := storage.NewStore(resptest.Addr(t), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	replica, err := storage.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()

	if err := primary.AddReplica(replicaAddr); err != nil {
		t.Fatal(err)
	}
	if err := primary.AddReplica(replicaAddr); err != nil || len(primary.Replicas()) != 1 {
		t.Fatalf("AddReplica is not idempotent: %v, %v", primary.Replicas(), err)
	}
	if err := primary.Set("storage:rep", []byte("v"), 60); err != nil {
		t.Fatal(err)
	}
	if v, err := replica.Get("storage:rep"); err != nil || string(v) != "v" {
		t.Fatalf("replica did not receive write: %q, %v", v, err)
	}
	if !primary.RemoveReplica(replicaAddr) {
		t.Fatal("RemoveReplica did not find the replica")
	}
	if _, err := primary.Delete("storage:rep"); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Get("storage:rep"); err != nil && !sameServer(t) {
		t.Fatalf("delete reached a removed replica: %v", err)
	}
}

// With TRITIUM_RESP_ADDR set every Addr call is the same real server, so the
// replica can't be told apart from the primary.
func sameServer(t *testing.T) bool {
	t.Helper()
	return resptest.Addr(t) == resptest.Addr(t)
}

// A replica the transport cannot reach is held — later writes no longer wait
// on it — and Repair replays exactly what it missed: current values and
// deletions, after which writes flow again.
func TestHeldReplicaIsRepaired(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot miss a write")
	}
	primary, err := storage.NewStore(resptest.Addr(t), 2, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	replica, err := storage.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	var broken atomic.Bool
	primary.SetReplicaTransport(storage.Transport{Dial: func(addr string) (net.Conn, error) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return flaky{c, &broken}, nil
	}, Timeout: 500 * time.Millisecond})
	if err := primary.AddReplica(replicaAddr); err != nil {
		t.Fatal(err)
	}
	set := func(k, v string) {
		t.Helper()
		if err := primary.Set(k, []byte(v), 60); err != nil {
			t.Fatal(err)
		}
	}
	set("held:a", "1")
	set("held:b", "1")
	if v, _ := replica.Get("held:b"); string(v) != "1" {
		t.Fatalf("replica did not receive the write: %q", v)
	}

	broken.Store(true)
	start := time.Now()
	set("held:a", "2")
	if _, err := primary.Delete("held:b"); err != nil {
		t.Fatal(err)
	}
	set("held:c", "3")
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("writes waited %v on a held replica", d)
	}
	if v, _ := replica.Get("held:a"); string(v) != "1" {
		t.Fatalf("a write reached the broken replica: %q", v)
	}
	if primary.Repair() != 0 {
		t.Fatal("repaired a replica that is still unreachable")
	}

	broken.Store(false)
	if n := primary.Repair(); n != 3 {
		t.Fatalf("replayed %d keys, want 3", n)
	}
	if v, _ := replica.Get("held:a"); string(v) != "2" {
		t.Fatalf("held:a on the replica = %q after repair", v)
	}
	if _, err := replica.Get("held:b"); err == nil {
		t.Fatal("a delete missed by the replica was not replayed")
	}
	if v, _ := replica.Get("held:c"); string(v) != "3" {
		t.Fatalf("held:c on the replica = %q after repair", v)
	}
	set("held:d", "4")
	if v, _ := replica.Get("held:d"); string(v) != "4" {
		t.Fatalf("the repaired replica is not receiving writes: %q", v)
	}
}

// flaky is a connection whose writes fail while broken is set, as to a peer
// that stopped answering.
type flaky struct {
	net.Conn
	broken *atomic.Bool
}

func (f flaky) Write(b []byte) (int, error) {
	if f.broken.Load() {
		f.Conn.Close()
		return 0, errors.New("flaky: broken")
	}
	return f.Conn.Write(b)
}

// A sync copies whole SCAN pages — strings and sorted sets with their TTLs —
// and an overwrite rebuilds a sorted set exactly.
func TestSyncCopiesPages(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot be synced to itself")
	}
	primary, err := storage.NewStore(resptest.Addr(t), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	replica, err := storage.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	for i := range 450 { // more than two SCAN pages
		if err := primary.Set(fmt.Sprintf("page:%d", i), []byte("v"), 60); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Mutate(resp.NewCommand("ZADD", "page:z", "1", "a", "2", "b"), resp.NewCommand("EXPIRE", "page:z", "60")); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Mutate(resp.NewCommand("ZADD", "page:z", "9", "stale"), resp.NewCommand("EXPIRE", "page:z", "60")); err != nil {
		t.Fatal(err)
	}
	n, err := primary.Sync(replicaAddr, true)
	if err != nil || n != 451 {
		t.Fatalf("Sync copied %d keys, %v; want 451", n, err)
	}
	if got, _ := replica.Exists("page:0", "page:449"); got != 2 {
		t.Fatalf("replica has %d of the page keys", got)
	}
	if ttl, _ := replica.TTL("page:449"); ttl <= 0 || ttl > 60 {
		t.Fatalf("copied key lost its TTL: %d", ttl)
	}
	if v, _ := replica.Query("ZRANGEBYSCORE", "page:z", "-inf", "+inf"); fmt.Sprint(v) != "[[97] [98]]" { // a, b — stale is gone
		t.Fatalf("sorted set after an overwrite sync: %v", v)
	}
}

// Under asynchronous replication a write returns once the primary has it;
// the replica gets every write in order soon after, and a replica that
// stops answering is held and repaired like any other.
func TestAsyncReplication(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot lag itself")
	}
	primary, err := storage.NewStore(resptest.Addr(t), 2, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	replica, err := storage.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	var broken atomic.Bool
	primary.SetReplicaTransport(storage.Transport{Dial: func(addr string) (net.Conn, error) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return flaky{c, &broken}, nil
	}, Timeout: 500 * time.Millisecond})
	primary.SetAsync(64)
	if err := primary.AddReplica(replicaAddr); err != nil {
		t.Fatal(err)
	}
	for i := range 300 { // one key rewritten: the replica must end on the last value
		if err := primary.Set("async:k", []byte(strconv.Itoa(i)), 60); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, _ := replica.Get("async:k"); string(v) == "299" {
			break
		}
		if time.Now().After(deadline) {
			v, _ := replica.Get("async:k")
			t.Fatalf("replica has %q after 5 s, want 299", v)
		}
		time.Sleep(10 * time.Millisecond)
		primary.Repair() // a slow box fills the queue: the replica is held, and the health tick's repair is what brings it up to date
	}

	broken.Store(true)
	if err := primary.Set("async:held", []byte("x"), 60); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the writer meets the broken link and holds the replica
	broken.Store(false)
	if n := primary.Repair(); n == 0 {
		t.Fatal("nothing repaired after the link came back")
	}
	if v, _ := replica.Get("async:held"); string(v) != "x" {
		t.Fatalf("held write not repaired: %q", v)
	}
}

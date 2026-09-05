package storage_test

import (
	"errors"
	"testing"

	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

func TestStoreSetGetDelete(t *testing.T) {
	s, err := storage.NewStore(resptest.Addr(t), 2)
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
	if ok, err := s.Delete("storage:k"); err != nil || !ok {
		t.Fatalf("delete: %v, %v", ok, err)
	}
	if ok, _ := s.Delete("storage:k"); ok {
		t.Fatal("second delete reported a key")
	}
	if _, err := s.Get("storage:k"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("after delete: got %v, want ErrNotFound", err)
	}
}

func TestStoreReplicates(t *testing.T) {
	primary, err := storage.NewStore(resptest.Addr(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	replica, err := storage.NewStore(replicaAddr, 1)
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

package replica_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/we-be/tritium/internal/resp"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/memstore"
	"github.com/we-be/tritium/internal/replica"
	"github.com/we-be/tritium/internal/resptest"
	"github.com/we-be/tritium/pkg/storage"
)

func TestStoreCommands(t *testing.T) {
	s, err := replica.NewStore(resptest.Addr(t), 2, "")
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
	primary, err := replica.NewStore(resptest.Addr(t), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	rep, err := replica.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()

	if err := primary.AddReplica(replicaAddr); err != nil {
		t.Fatal(err)
	}
	if err := primary.AddReplica(replicaAddr); err != nil || len(primary.Replicas()) != 1 {
		t.Fatalf("AddReplica is not idempotent: %v, %v", primary.Replicas(), err)
	}
	if err := primary.Set("storage:rep", []byte("v"), 60); err != nil {
		t.Fatal(err)
	}
	if v, err := rep.Get("storage:rep"); err != nil || string(v) != "v" {
		t.Fatalf("replica did not receive write: %q, %v", v, err)
	}
	if !primary.RemoveReplica(replicaAddr) {
		t.Fatal("RemoveReplica did not find the replica")
	}
	if _, err := primary.Delete("storage:rep"); err != nil {
		t.Fatal(err)
	}
	if _, err := rep.Get("storage:rep"); err != nil && !sameServer(t) {
		t.Fatalf("delete reached a removed replica: %v", err)
	}
}

// With TRITIUM_RESP_ADDR set every Addr call is the same real server, so the
// rep can't be told apart from the primary.
func sameServer(t *testing.T) bool {
	t.Helper()
	return resptest.Shared()
}

// A rep the transport cannot reach is held — later writes no longer wait
// on it — and Repair replays exactly what it missed: current values and
// deletions, after which writes flow again.
func TestHeldReplicaIsRepaired(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot miss a write")
	}
	primary, err := replica.NewStore(resptest.Addr(t), 2, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	rep, err := replica.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	var broken atomic.Bool
	primary.SetReplicaTransport(replica.Transport{Dial: func(addr string) (net.Conn, error) {
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
	if v, _ := rep.Get("held:b"); string(v) != "1" {
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
	if v, _ := rep.Get("held:a"); string(v) != "1" {
		t.Fatalf("a write reached the broken replica: %q", v)
	}
	if primary.Repair() != 0 {
		t.Fatal("repaired a replica that is still unreachable")
	}

	broken.Store(false)
	if n := primary.Repair(); n != 3 {
		t.Fatalf("replayed %d keys, want 3", n)
	}
	if v, _ := rep.Get("held:a"); string(v) != "2" {
		t.Fatalf("held:a on the replica = %q after repair", v)
	}
	if _, err := rep.Get("held:b"); err == nil {
		t.Fatal("a delete missed by the replica was not replayed")
	}
	if v, _ := rep.Get("held:c"); string(v) != "3" {
		t.Fatalf("held:c on the replica = %q after repair", v)
	}
	set("held:d", "4")
	if v, _ := rep.Get("held:d"); string(v) != "4" {
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
	primary, err := replica.NewStore(resptest.Addr(t), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	rep, err := replica.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	for i := range 450 { // more than two SCAN pages
		if err := primary.Set(fmt.Sprintf("page:%d", i), []byte("v"), 60); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Mutate(resp.NewCommand("ZADD", "page:z", "1", "a", "2", "b"), resp.NewCommand("EXPIRE", "page:z", "60")); err != nil {
		t.Fatal(err)
	}
	if _, err := rep.Mutate(resp.NewCommand("ZADD", "page:z", "9", "stale"), resp.NewCommand("EXPIRE", "page:z", "60")); err != nil {
		t.Fatal(err)
	}
	n, err := primary.Sync(replicaAddr, true)
	if err != nil || n != 451 {
		t.Fatalf("Sync copied %d keys, %v; want 451", n, err)
	}
	if got, _ := rep.Exists("page:0", "page:449"); got != 2 {
		t.Fatalf("replica has %d of the page keys", got)
	}
	if ttl, _ := rep.TTL("page:449"); ttl <= 0 || ttl > 60 {
		t.Fatalf("copied key lost its TTL: %d", ttl)
	}
	if v, _ := rep.Query("ZRANGEBYSCORE", "page:z", "-inf", "+inf"); fmt.Sprint(v) != "[[97] [98]]" { // a, b — stale is gone
		t.Fatalf("sorted set after an overwrite sync: %v", v)
	}
}

// Under asynchronous replication a write returns once the primary has it;
// the rep gets every write in order soon after, and a rep that
// stops answering is held and repaired like any other.
func TestAsyncReplication(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot lag itself")
	}
	primary, err := replica.NewStore(resptest.Addr(t), 2, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	replicaAddr := resptest.Addr(t)
	rep, err := replica.NewStore(replicaAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	var broken atomic.Bool
	primary.SetReplicaTransport(replica.Transport{Dial: func(addr string) (net.Conn, error) {
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
	for i := range 300 { // one key rewritten: the rep must end on the last value
		if err := primary.Set("async:k", []byte(strconv.Itoa(i)), 60); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, _ := rep.Get("async:k"); string(v) == "299" {
			break
		}
		if time.Now().After(deadline) {
			v, _ := rep.Get("async:k")
			t.Fatalf("replica has %q after 5 s, want 299", v)
		}
		time.Sleep(10 * time.Millisecond)
		primary.Repair() // a slow box fills the queue: the rep is held, and the health tick's repair is what brings it up to date
	}

	broken.Store(true)
	if err := primary.Set("async:held", []byte("x"), 60); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the writer meets the broken link and holds the rep
	broken.Store(false)
	if n := primary.Repair(); n == 0 {
		t.Fatal("nothing repaired after the link came back")
	}
	if v, _ := rep.Get("async:held"); string(v) != "x" {
		t.Fatalf("held write not repaired: %q", v)
	}
}

// Two stores holding different writes of one key both end with the later
// one after syncing either way.
func TestSyncMergesByStamp(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot disagree with itself")
	}
	var n uint64
	next := func() uint64 { n++; return n }
	aAddr, bAddr := resptest.Addr(t), resptest.Addr(t)
	a, err := replica.NewStore(aAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := replica.NewStore(bAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a.SetStamper(next, true)
	b.SetStamper(next, true)
	if err := a.Set("merge:k", []byte("first"), 60); err != nil {
		t.Fatal(err)
	}
	if err := b.Set("merge:k", []byte("later"), 60); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Sync(bAddr, true); err != nil {
		t.Fatal(err)
	}
	if v, _ := b.Get("merge:k"); string(v) != "later" {
		t.Fatalf("an older write overwrote the newer one on sync: %q", v)
	}
	if _, err := b.Sync(aAddr, false); err != nil {
		t.Fatal(err)
	}
	if v, _ := a.Get("merge:k"); string(v) != "later" {
		t.Fatalf("the newer write did not reach the store holding the older one: %q", v)
	}
}

// A TLS store round-trips; a plain dial to the same listener is refused
// instead of exchanging AUTH and values in the clear.
func TestStoreTLS(t *testing.T) {
	cert, pool := selfSignedCert(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tln := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})
	st := memstore.New(memstore.Options{Version: "test"})
	go st.Serve(tln)
	t.Cleanup(func() { tln.Close(); st.Close() })

	s, err := replica.NewStoreTLS(ln.Addr().String(), 1, "s3cret", &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Set("tls:k", []byte("v"), 60); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get("tls:k"); err != nil || string(v) != "v" {
		t.Fatalf("get: %q, %v", v, err)
	}

	if _, err := replica.NewStore(ln.Addr().String(), 1, "s3cret"); err == nil {
		t.Fatal("a plain dial to a TLS store was accepted")
	}
}

// selfSignedCert is a certificate that is its own CA, valid for 127.0.0.1.
func selfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tritium-test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

// settles waits until want reads back from store at key — "" for a key that
// must be gone — since a scoped replica is fed from a queue and a write is
// not there the instant the primary answers.
func settles(t *testing.T, store *replica.Store, key, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		v, err := store.Get(key)
		switch {
		case want == "" && errors.Is(err, storage.ErrNotFound):
			return
		case want != "" && err == nil && string(v) == want:
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s is %q (%v) after 2 s, want %q", key, v, err, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A replica held to rights is sent only what those rights name: a write
// outside them never leaves, a DEL naming keys on both sides is cut down to
// the ones it may hold, and neither counts as a write the replica missed.
// Increment 5 of docs/trust-plan.md.
func TestFanOutHonoursReplicaRights(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot be told apart from the primary")
	}
	for _, stamps := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "stamped"}[stamps], func(t *testing.T) {
			primary, err := replica.NewStore(resptest.Addr(t), 1, "")
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			repAddr := resptest.Addr(t)
			rep, err := replica.NewStore(repAddr, 1, "")
			if err != nil {
				t.Fatal(err)
			}
			defer rep.Close()
			if stamps {
				var n atomic.Uint64
				primary.SetStamper(func() uint64 { return n.Add(1) }, true)
			}

			pub := &config.Rights{Read: []string{"pub:"}, Write: []string{"pub:"}}
			primary.SetReplicaRights(func(addr string) *config.Rights {
				if addr == repAddr {
					return pub
				}
				return nil
			})

			// What the primary already holds is copied on attach, and that
			// copy reaches no further than the fan-out does.
			if err := primary.Set("pub:old", []byte("v"), 60); err != nil {
				t.Fatal(err)
			}
			if err := primary.Set("fleet:old", []byte("v"), 60); err != nil {
				t.Fatal(err)
			}
			if err := primary.AddReplica(repAddr); err != nil {
				t.Fatal(err)
			}
			if n, err := primary.Sync(repAddr, true); err != nil || n != 1 {
				t.Fatalf("resync: %d keys, %v; want 1", n, err)
			}
			if v, err := rep.Get("pub:old"); err != nil || string(v) != "v" {
				t.Fatalf("the resync skipped a key the replica may hold: %q, %v", v, err)
			}
			if _, err := rep.Get("fleet:old"); !errors.Is(err, storage.ErrNotFound) {
				t.Fatal("the resync copied a key outside the replica's rights")
			}

			if err := primary.Set("pub:k", []byte("v"), 60); err != nil {
				t.Fatal(err)
			}
			if err := primary.Set("fleet:k", []byte("v"), 60); err != nil {
				t.Fatal(err)
			}
			settles(t, rep, "pub:k", "v") // the fan-out that carried fleet:k, if any, went out before this one
			if _, err := rep.Get("fleet:k"); !errors.Is(err, storage.ErrNotFound) {
				t.Fatal("a key outside the replica's rights was fanned out to it")
			}

			// The replica has a fleet: key of its own — its store is shared
			// with nobody, but a DEL that reached it would still remove one.
			if err := rep.Set("fleet:mine", []byte("v"), 60); err != nil {
				t.Fatal(err)
			}
			if _, err := primary.Delete("pub:k", "fleet:mine"); err != nil {
				t.Fatal(err)
			}
			settles(t, rep, "pub:k", "")
			if v, err := rep.Get("fleet:mine"); err != nil || string(v) != "v" {
				t.Fatalf("a DEL reached past the replica's rights: %q, %v", v, err)
			}

			// Two fan-outs went out short; nothing was held, since a key the
			// replica may not hold is not a key it missed.
			if n := primary.Withheld()[repAddr]; n != 2 {
				t.Fatalf("withheld from %s: %d, want 2", repAddr, n)
			}
			if held := primary.Held(); len(held) != 0 {
				t.Fatalf("a scoped replica was held: %v", held)
			}
		})
	}
}

// slow is a connection whose writes take a beat, as to a peer on a link
// worse than the fleet's own.
type slow struct {
	net.Conn
	delay time.Duration
}

func (s slow) Write(b []byte) (int, error) {
	time.Sleep(s.delay)
	return s.Conn.Write(b)
}

// A replica held to rights is fed from a queue from the moment it attaches,
// so a fleet write never waits on it: an outsider joining for a surface
// cannot slow the fleet down, however bad its link. The unscoped replica on
// the same link is the control — that one a write does wait for.
// Increment 7 of docs/trust-plan.md.
func TestScopedReplicaIsNeverWaitedOn(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot lag itself")
	}
	const delay = 500 * time.Millisecond
	for _, scoped := range []bool{true, false} {
		t.Run(map[bool]string{true: "scoped", false: "fleet"}[scoped], func(t *testing.T) {
			primary, err := replica.NewStore(resptest.Addr(t), 1, "")
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			repAddr := resptest.Addr(t)
			rep, err := replica.NewStore(repAddr, 1, "")
			if err != nil {
				t.Fatal(err)
			}
			defer rep.Close()
			primary.SetReplicaTransport(replica.Transport{Dial: func(addr string) (net.Conn, error) {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					return nil, err
				}
				return slow{c, delay}, nil
			}, Timeout: 5 * time.Second})
			if scoped {
				primary.SetReplicaRights(func(string) *config.Rights {
					return &config.Rights{Read: []string{"pub:"}, Write: []string{"pub:"}}
				})
			}
			if err := primary.AddReplica(repAddr); err != nil {
				t.Fatal(err)
			}

			if queued := slices.Contains(primary.Queued(), repAddr); queued != scoped {
				t.Fatalf("queued: %v, want %v", queued, scoped)
			}
			start := time.Now()
			if err := primary.Set("pub:k", []byte("v"), 60); err != nil {
				t.Fatal(err)
			}
			took := time.Since(start)
			if scoped && took > delay/2 {
				t.Fatalf("a write waited %v on a scoped replica: the fleet pays for an outsider's link", took)
			}
			if !scoped && took < delay {
				t.Fatalf("a write took %v, less than the link costs: the control is not waiting", took)
			}
			// Queued is not dropped: it arrives, just not on the write's time.
			settles(t, rep, "pub:k", "v")
		})
	}
}

// asked records every command a store is sent, proxying them on to a real
// one, so a test can say what was asked for and not only what came back.
type asked struct {
	ln   net.Listener
	up   string
	mu   sync.Mutex
	cmds [][]string
}

func record(tb testing.TB, upstream string) *asked {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	a := &asked{ln: ln, up: upstream}
	go a.serve()
	tb.Cleanup(func() { ln.Close() })
	return a
}

func (a *asked) addr() string { return a.ln.Addr().String() }

func (a *asked) serve() {
	for {
		c, err := a.ln.Accept()
		if err != nil {
			return
		}
		go a.proxy(c)
	}
}

func (a *asked) proxy(c net.Conn) {
	defer c.Close()
	up, err := net.Dial("tcp", a.up)
	if err != nil {
		return
	}
	defer up.Close()
	pr, pw := io.Pipe()
	go func() {
		r := resp.NewReader(pr)
		for {
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			a.mu.Lock()
			a.cmds = append(a.cmds, args)
			a.mu.Unlock()
		}
	}()
	go func() { io.Copy(c, up); c.Close() }()
	io.Copy(up, io.TeeReader(c, pw))
	pw.Close()
}

// commands is what has been asked so far, whose first word is name.
func (a *asked) commands(name string) [][]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out [][]string
	for _, c := range a.cmds {
		if len(c) > 0 && strings.EqualFold(c[0], name) {
			out = append(out, slices.Clone(c))
		}
	}
	return out
}

// A resync to a replica held to rights reads what that replica may hold and
// nothing else: the walk is one SCAN per right, so a key outside the surface
// is never even asked about, where filtering a full walk would have paid for
// the whole keyspace to copy a corner of it. Increment 8 of
// docs/trust-plan.md.
func TestResyncWalksOnlyTheReplicasRights(t *testing.T) {
	if sameServer(t) {
		t.Skip("a shared store cannot be told apart from the primary")
	}
	store := record(t, resptest.Addr(t))
	primary, err := replica.NewStore(store.addr(), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	repAddr := resptest.Addr(t)
	rep, err := replica.NewStore(repAddr, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()

	// A small surface in a keyspace mostly out of its reach.
	for i := range 200 {
		if err := primary.Set("fleet:"+strconv.Itoa(i), []byte("v"), 60); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"pub:a", "pub:b", "news"} {
		if err := primary.Set(k, []byte("v"), 60); err != nil {
			t.Fatal(err)
		}
	}
	primary.SetReplicaRights(func(addr string) *config.Rights {
		if addr == repAddr {
			return &config.Rights{Read: []string{"pub:", "news"}, Write: []string{"pub:"}}
		}
		return nil
	})

	before := len(store.commands("SCAN"))
	n, err := primary.Sync(repAddr, true)
	if err != nil || n != 3 {
		t.Fatalf("resync: %d keys, %v; want 3", n, err)
	}
	if v, err := rep.Get("pub:a"); err != nil || string(v) != "v" {
		t.Fatalf("the resync skipped a key the replica may hold: %q, %v", v, err)
	}
	if v, err := rep.Get("news"); err != nil || string(v) != "v" {
		t.Fatalf("the resync skipped an exactly-named right: %q, %v", v, err)
	}

	// Every walk named a right; none asked for the keyspace.
	scans := store.commands("SCAN")[before:]
	if len(scans) != 2 {
		t.Fatalf("%d SCANs for two rights: %q", len(scans), scans)
	}
	var matched []string
	for _, c := range scans {
		i := slices.IndexFunc(c, func(a string) bool { return strings.EqualFold(a, "MATCH") })
		if i < 0 || i+1 >= len(c) {
			t.Fatalf("a resync for a scoped replica walked the keyspace: %q", c)
		}
		matched = append(matched, c[i+1])
	}
	slices.Sort(matched)
	if want := []string{"news", "pub:*"}; !slices.Equal(matched, want) {
		t.Fatalf("walked %q; want %q", matched, want)
	}

	// And no key outside the surface was so much as read.
	for _, name := range []string{"TYPE", "TTL", "GET", "STAMPOF"} {
		for _, c := range store.commands(name) {
			if len(c) > 1 && strings.HasPrefix(c[1], "fleet:") {
				t.Fatalf("the resync read a key the replica may not hold: %q", c)
			}
		}
	}
}

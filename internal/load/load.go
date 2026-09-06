// Package load drives a node with a mix of commands at a rate and reports
// what it saw: throughput, latency percentiles per command, and, with a
// second node, how long a write takes to show up there.
package load

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/we-be/tritium/pkg/tritium"
)

// Options shape one run.
type Options struct {
	Rate     int           // operations per second across all connections; 0 = as fast as they go
	Duration time.Duration // how long to run
	Conns    int           // concurrent connections, one client each
	Keys     int           // size of the key space
	Size     int           // value size in bytes
	TTL      int           // seconds each written key lives
	Mix      [3]int        // relative weights of SET, GET, ZADD
	LagEvery time.Duration // with a peer: how often a timestamped write is made to measure replication lag
}

// Sample is one command kind's latencies.
type Sample struct {
	N, Errors          int
	P50, P95, P99, Max time.Duration
}

// Report is what a run produced.
type Report struct {
	Elapsed        time.Duration
	Ops            int
	Errors         int
	Set, Get, ZAdd Sample
	Lag            Sample // replication lag to the peer, when one was given
	LagMissed      int    // writes never seen on the peer within the wait
}

func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d ops in %s: %.0f ops/s, %d errors\n", r.Ops, r.Elapsed.Round(time.Millisecond), float64(r.Ops)/r.Elapsed.Seconds(), r.Errors)
	for _, s := range []struct {
		name string
		s    Sample
	}{{"SET", r.Set}, {"GET", r.Get}, {"ZADD", r.ZAdd}} {
		if s.s.N > 0 {
			fmt.Fprintf(&b, "  %-5s n=%-7d p50=%-9s p95=%-9s p99=%-9s max=%s\n", s.name, s.s.N, s.s.P50, s.s.P95, s.s.P99, s.s.Max)
		}
	}
	if r.Lag.N > 0 || r.LagMissed > 0 {
		fmt.Fprintf(&b, "  replication lag n=%-4d p50=%-9s p99=%-9s max=%-9s missed=%d\n", r.Lag.N, r.Lag.P50, r.Lag.P99, r.Lag.Max, r.LagMissed)
	}
	return b.String()
}

// Run drives the node at opts until o.Duration passes or ctx ends. With
// peer set, timestamped writes to the node are watched for on the peer.
func Run(ctx context.Context, opts tritium.ClientOptions, peer *tritium.ClientOptions, o Options) (Report, error) {
	if o.Conns <= 0 {
		o.Conns = 1
	}
	if o.Keys <= 0 {
		o.Keys = 1000
	}
	if o.Mix == [3]int{} {
		o.Mix = [3]int{50, 45, 5}
	}
	ctx, cancel := context.WithTimeout(ctx, o.Duration)
	defer cancel()

	var tick <-chan time.Time
	if o.Rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(o.Rate))
		defer t.Stop()
		tick = t.C
	}
	var r [4]byte
	rand.Read(r[:])
	prefix := "load:" + hex.EncodeToString(r[:]) + ":"
	value := strings.Repeat("x", max(o.Size, 1))

	var mu sync.Mutex
	var set, get, zadd, lag []time.Duration
	var errs, lagMissed int
	record := func(kind int, d time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil && !errors.Is(err, tritium.ErrNotFound) {
			errs++
			return
		}
		switch kind {
		case 0:
			set = append(set, d)
		case 1:
			get = append(get, d)
		case 2:
			zadd = append(zadd, d)
		}
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := range o.Conns {
		c, err := tritium.NewClient(&opts)
		if err != nil {
			cancel()
			wg.Wait()
			return Report{}, err
		}
		wg.Go(func() {
			defer c.Close()
			n := i
			for {
				if tick != nil {
					select {
					case <-ctx.Done():
						return
					case <-tick:
					}
				} else if ctx.Err() != nil {
					return
				}
				n += o.Conns
				key := prefix + strconv.Itoa(n%o.Keys)
				kind := pick(o.Mix, n)
				t0 := time.Now()
				var err error
				switch kind {
				case 0:
					err = c.Set(key, []byte(value), &o.TTL)
				case 1:
					_, err = c.Get(key)
				case 2:
					_, err = c.Do("ZADD", prefix+"z", strconv.Itoa(n), key)
				}
				record(kind, time.Since(t0), err)
			}
		})
	}
	if peer != nil {
		wg.Go(func() {
			w, err := tritium.NewClient(&opts)
			if err != nil {
				return
			}
			defer w.Close()
			p, err := tritium.NewClient(peer)
			if err != nil {
				return
			}
			defer p.Close()
			every := o.LagEvery
			if every <= 0 {
				every = 200 * time.Millisecond
			}
			for i := 0; ctx.Err() == nil; i++ {
				key := prefix + "lag:" + strconv.Itoa(i)
				t0 := time.Now()
				if err := w.Set(key, []byte(strconv.FormatInt(t0.UnixNano(), 10)), &o.TTL); err != nil {
					mu.Lock()
					errs++
					mu.Unlock()
					continue
				}
				seen := false
				for time.Since(t0) < 2*time.Second && ctx.Err() == nil {
					if _, err := p.Get(key); err == nil {
						seen = true
						break
					}
					time.Sleep(time.Millisecond)
				}
				mu.Lock()
				if seen {
					lag = append(lag, time.Since(t0))
				} else if ctx.Err() == nil {
					lagMissed++
				}
				mu.Unlock()
				select {
				case <-ctx.Done():
				case <-time.After(every):
				}
			}
		})
	}
	wg.Wait()
	rep := Report{Elapsed: time.Since(start), Errors: errs, Set: sample(set), Get: sample(get), ZAdd: sample(zadd), Lag: sample(lag), LagMissed: lagMissed}
	rep.Ops = len(set) + len(get) + len(zadd)
	return rep, nil
}

// pick chooses SET, GET or ZADD for the n-th operation by the mix weights.
func pick(mix [3]int, n int) int {
	total := mix[0] + mix[1] + mix[2]
	if total <= 0 {
		return 0
	}
	x := (n * 7919) % total // spread, not clustered
	switch {
	case x < mix[0]:
		return 0
	case x < mix[0]+mix[1]:
		return 1
	default:
		return 2
	}
}

func sample(d []time.Duration) Sample {
	if len(d) == 0 {
		return Sample{}
	}
	slices.Sort(d)
	at := func(p float64) time.Duration { return d[min(len(d)-1, int(float64(len(d))*p))] }
	return Sample{N: len(d), P50: at(.5), P95: at(.95), P99: at(.99), Max: d[len(d)-1]}
}

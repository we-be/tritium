package server

import (
	"sync"
	"time"
)

// guesses counts refused AUTHs per client address, across connections: five
// per connection was the old limit, and reconnecting reset it. An address
// that keeps failing is shut out for a while, and the shutting-out is in
// the log, so a run of guesses is visible from anywhere.
type guesses struct {
	mu   sync.Mutex
	by   map[string]*guessing
	last time.Time // when stale entries were last dropped
}

type guessing struct {
	fails int
	since time.Time // the window's start
	until time.Time // shut out until then; zero when not
}

var (
	guessWindow  = 10 * time.Minute
	guessLimit   = 20
	guessLockout = 10 * time.Minute
)

// note records one refusal and reports whether it started a lockout.
func (g *guesses) note(host string) bool {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.by == nil {
		g.by = map[string]*guessing{}
	}
	if now.Sub(g.last) > guessWindow {
		for h, e := range g.by {
			if now.Sub(e.since) > guessWindow && now.After(e.until) {
				delete(g.by, h)
			}
		}
		g.last = now
	}
	e := g.by[host]
	if e == nil || now.Sub(e.since) > guessWindow {
		e = &guessing{since: now}
		g.by[host] = e
	}
	e.fails++
	if e.fails == guessLimit {
		e.until = now.Add(guessLockout)
		return true
	}
	return false
}

func (g *guesses) blocked(host string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.by[host]
	return e != nil && time.Now().Before(e.until)
}

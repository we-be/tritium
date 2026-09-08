package server

import (
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/we-be/tritium/internal/replica"
	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/pkg/storage"
)

// eventsTTL and eventsCap bound this node's own event log: a day of
// history, and a hard cap so a peer flapping attach/detach all day cannot
// grow the replicated set without bound. Variables so tests can shrink them.
var (
	eventsTTL = 24 * time.Hour
	eventsCap = 500
)

// eventLog buffers this node's cluster events and writes them through the
// store on its own goroutine. Attach, detach, hold and repair all fire from
// inside cluster or store locks — merge, checkHealth, pool.hold under its
// own mutex — and a write there, which fans out to every peer including
// perhaps the one just being attached or detached, would stall or deadlock
// the caller. emit only ever queues; the goroutine does the actual write
// with none of those locks held.
type eventLog struct {
	nodeID string
	store  *replica.Store
	ch     chan storage.Event
	done   chan struct{}
}

// newEventLog starts the writer goroutine for nodeID's log.
func newEventLog(nodeID string, store *replica.Store) *eventLog {
	el := &eventLog{nodeID: nodeID, store: store, ch: make(chan storage.Event, 64), done: make(chan struct{})}
	go el.run()
	return el
}

// emit queues one event. Events are rare, but a full buffer means they are
// arriving faster than they can be written — the caller must never block on
// it, so this one is dropped instead.
func (el *eventLog) emit(kind, peer string, keys int, took time.Duration) {
	ev := storage.Event{At: time.Now().UnixMilli(), Node: el.nodeID, Event: kind, Peer: peer, Keys: keys}
	if took > 0 {
		ev.Took = took.Milliseconds()
	}
	select {
	case el.ch <- ev:
	default:
		slog.Warn("cluster: event log buffer full, dropping", "event", kind, "peer", peer)
	}
}

func (el *eventLog) run() {
	for {
		select {
		case <-el.done:
			return
		case ev := <-el.ch:
			el.write(ev)
		}
	}
}

// write appends ev through the store's normal replicated path, so it lands
// on every peer like any other key, then trims the log by age and by count
// in the same round trip: one write, at most one extra key touched.
func (el *eventLog) write(ev storage.Event) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	key := storage.EventsKeyPrefix + el.nodeID
	cutoff := strconv.FormatInt(time.Now().Add(-eventsTTL).UnixMilli(), 10)
	cmds := []resp.Command{
		resp.NewCommand("ZADD", key, strconv.FormatInt(ev.At, 10), string(b)),
		resp.NewCommand("ZREMRANGEBYSCORE", key, "-inf", "("+cutoff),
		resp.NewCommand("ZREMRANGEBYRANK", key, "0", strconv.Itoa(-(eventsCap + 1))),
		resp.NewCommand("EXPIRE", key, strconv.Itoa(int(eventsTTL.Seconds()))),
	}
	if _, err := el.store.Mutate(cmds...); err != nil {
		slog.Debug("cluster: event write failed", "event", ev.Event, "err", err)
	}
}

func (el *eventLog) stop() {
	close(el.done)
}

// eventsKept is how many entries this node's own event log currently holds,
// for INFO tritium.
func (s *Server) eventsKept(id string) int64 {
	v, err := s.store.Query("ZCARD", storage.EventsKeyPrefix+id)
	if err != nil {
		return 0
	}
	n, _ := v.(int64)
	return n
}

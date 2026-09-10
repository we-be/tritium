package replica

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/internal/resp"
)

// Resync and repair: copying what a peer's store lacks after an attach,
// and replaying what a held replica missed once it answers again.

// Repair replays, on every replica held after a failed write, the keys it
// missed meanwhile — our current copy of each, or its deletion — and
// releases it once nothing is missing. Meant for a periodic tick: a replica
// that still does not answer stays held for the next one. Returns the keys
// replayed.
func (s *Store) Repair() int {
	s.mu.RLock()
	replicas := slices.Clone(s.replicas)
	hook := s.repairHook
	s.mu.RUnlock()
	total := 0
	for _, r := range replicas {
		if !r.isHeld() {
			continue
		}
		n, err := s.repair(r)
		total += n
		if err != nil {
			slog.Debug("replica repair failed, still held", "addr", r.addr, "keys", n, "err", err)
			continue
		}
		slog.Info("replica repaired", "addr", r.addr, "keys", n)
		if hook != nil && n > 0 {
			hook(r.addr, n)
		}
	}
	return total
}

func (s *Store) repair(p *pool) (int, error) {
	if p.queue != nil {
		p.flush() // whatever was queued before the hold is noted now, never sent after the replay
	}
	p.mu.Lock()
	spilled := p.spilled
	p.mu.Unlock()
	if spilled {
		n, err := s.Sync(p.addr, true)
		if err != nil {
			return n, err
		}
		p.mu.Lock()
		p.held, p.missed, p.spilled = false, nil, false
		p.mu.Unlock()
		return n, nil
	}
	n := 0
	for range 5 { // writes noted during a round are replayed by the next; a few rounds, then the next tick
		p.mu.Lock()
		keys := slices.Collect(maps.Keys(p.missed))
		p.mu.Unlock()
		for batch := range slices.Chunk(p.scopeKeys(keys), 200) {
			cmds, _, err := s.copyCommands(batch, true, true)
			if err != nil {
				return n, err
			}
			if len(cmds) > 0 {
				if _, err := p.doAll(cmds); err != nil {
					var se *resp.ServerError
					if !errors.As(err, &se) {
						return n, err
					}
				}
			}
			n += len(batch)
		}
		if p.forget(keys) {
			return n, nil
		}
	}
	return n, nil
}

// Sync copies every key on the primary to the store at addr — a peer back
// from an outage missed every write made meanwhile, and a newcomer holds
// nothing. With overwrite the primary's copy wins (the peer was down, so ours
// is the newer one); without it only keys the peer lacks are filled, so a
// node that just started never clobbers what the survivors hold. Best
// effort, a SCAN page at a time — three pipelined round trips per page, not
// per key — returning the keys copied and the first error.
func (s *Store) Sync(addr string, overwrite bool) (int, error) {
	s.mu.RLock()
	via, rights := s.via, s.rights
	s.mu.RUnlock()
	dst, err := newPool(addr, 1, via)
	if err != nil {
		return 0, err
	}
	defer dst.close()
	if rights != nil { // a copy to a peer reaches no further than its rights, like a fan-out
		dst.rights = func() *config.Rights { return rights(addr) }
	}
	n, cursor := 0, "0"
	var first error
	for {
		v, err := s.primary.do(resp.NewCommand("SCAN", cursor, "COUNT", "200"))
		if err != nil {
			return n, err
		}
		page, ok := v.([]any)
		if !ok || len(page) != 2 {
			return n, fmt.Errorf("unexpected SCAN reply %T", v)
		}
		next, _ := page[0].([]byte)
		raw, _ := page[1].([]any)
		keys := make([]string, 0, len(raw))
		for _, k := range raw {
			key, _ := k.([]byte)
			keys = append(keys, string(key))
		}
		cmds, copied, err := s.copyCommands(dst.scopeKeys(keys), overwrite, false)
		if err != nil {
			return n, err
		}
		if len(cmds) > 0 {
			if _, err := dst.doAll(cmds); err != nil && first == nil {
				first = err
			}
		}
		n += copied
		cursor = string(next)
		if cursor == "0" {
			return n, first
		}
	}
}

// copyCommands is what recreates keys elsewhere with their remaining TTL,
// read in two pipelined round trips: every key's type and TTL, then every
// value. Keys that are gone or of a kind tritium does not write are
// skipped — or, with replay, a gone key becomes a DEL, since a replay is
// of writes the other side missed and one of them may have been the
// delete. With overwrite the copy is exact: a sorted set is rebuilt from
// scratch so members removed here go there too. Returns the commands and
// how many keys they cover.
func (s *Store) copyCommands(keys []string, overwrite, replay bool) ([]resp.Command, int, error) {
	if len(keys) == 0 {
		return nil, 0, nil
	}
	_, stamps := s.Stamps()
	per := 2
	if stamps {
		per = 3
	}
	probe := make([]resp.Command, 0, per*len(keys))
	for _, k := range keys {
		probe = append(probe, resp.NewCommand("TYPE", k), resp.NewCommand("TTL", k))
		if stamps {
			probe = append(probe, resp.NewCommand("STAMPOF", k))
		}
	}
	replies, err := s.primary.doAll(probe)
	if err != nil {
		return nil, 0, err
	}
	type want struct {
		key, typ string
		ttl      int64
		stamp    uint64
	}
	var wants []want
	var reads []resp.Command
	var out []resp.Command
	for i, k := range keys {
		typ, _ := replies[per*i].(string)
		ttl, _ := replies[per*i+1].(int64)
		var stamp uint64
		if stamps {
			n, _ := replies[per*i+2].(int64)
			stamp = uint64(n)
		}
		switch {
		case typ == "none" && replay:
			// a delete the other side missed: with its stamp, so it also beats an older write that reaches there later
			if stamp > 0 {
				out = append(out, resp.NewCommand("STAMPED", strconv.FormatUint(stamp, 10), "DEL", k))
			} else {
				out = append(out, resp.NewCommand("DEL", k))
			}
		case ttl <= 0: // gone, or a key without an expiry: not ours
		case typ == "string":
			wants = append(wants, want{k, typ, ttl, stamp})
			reads = append(reads, resp.NewCommand("GET", k))
		case typ == "zset":
			wants = append(wants, want{k, typ, ttl, 0})
			reads = append(reads, resp.NewCommand("ZRANGEBYSCORE", k, "-inf", "+inf", "WITHSCORES"))
		}
	}
	copied := len(out)
	if len(reads) == 0 {
		return out, copied, nil
	}
	values, err := s.primary.doAll(reads)
	if err != nil {
		return nil, 0, err
	}
	for i, w := range wants {
		exp := strconv.FormatInt(w.ttl, 10)
		switch w.typ {
		case "string":
			val, ok := values[i].([]byte)
			if !ok {
				continue // expired between the two reads
			}
			args := []string{"SET", w.key, string(val), "EX", exp}
			switch {
			case w.stamp > 0: // the stamp decides there, whichever side wrote last
				args = append([]string{"STAMPED", strconv.FormatUint(w.stamp, 10)}, args...)
			case !overwrite:
				args = append(args, "NX")
			}
			out = append(out, resp.NewCommand(args...))
		case "zset":
			pairs, _ := values[i].([]any)
			if len(pairs) < 2 {
				continue
			}
			args := []string{"ZADD", w.key}
			if !overwrite {
				args = append(args, "NX")
			}
			for j := 0; j+1 < len(pairs); j += 2 {
				member, _ := pairs[j].([]byte)
				score, _ := pairs[j+1].([]byte)
				args = append(args, string(score), string(member))
			}
			expire := []string{"EXPIRE", w.key, exp}
			if overwrite {
				out = append(out, resp.NewCommand("DEL", w.key))
			} else {
				expire = append(expire, "GT")
			}
			out = append(out, resp.NewCommand(args...), resp.NewCommand(expire...))
		}
		copied++
	}
	return out, copied, nil
}

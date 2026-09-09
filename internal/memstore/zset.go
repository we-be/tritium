package memstore

import (
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/we-be/tritium/internal/resp"
)

// Sorted sets: members kept sorted by score, ranged by score bounds.

func (s *Store) zadd(b []byte, args []string) []byte {
	nx := len(args) > 2 && strings.EqualFold(args[2], "NX")
	if nx {
		args = append(args[:2], args[3:]...)
	}
	if len(args) < 4 || len(args)%2 != 0 {
		return errArgs(b, "ZADD")
	}
	k := args[1]
	e := s.live(k)
	if e != nil && e.zset == nil {
		return wrongType(b)
	}
	scores := make([]float64, 0, (len(args)-2)/2)
	var need int64
	for i := 2; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return resp.AppendError(b, "ERR value is not a valid float")
		}
		scores = append(scores, score)
		if e == nil {
			need += memberOverhead + int64(len(args[i+1]))
		} else if _, ok := e.zset[args[i+1]]; !ok {
			need += memberOverhead + int64(len(args[i+1]))
		}
	}
	if e == nil {
		need += keyOverhead + int64(len(k))
	}
	if !s.room(need) {
		return errOOM(b)
	}
	if e = s.kv[k]; e == nil { // room may have evicted it
		e = &entry{zset: map[string]float64{}, size: keyOverhead + int64(len(k))}
		s.put(k, e)
	}
	var added int64
	for i := 3; i < len(args); i += 2 {
		m := args[i]
		if _, ok := e.zset[m]; !ok {
			added++
			grow := memberOverhead + int64(len(m))
			e.size += grow
			s.used += grow
		} else if nx {
			continue
		}
		e.zset[m] = scores[(i-3)/2]
	}
	return resp.AppendInt(b, added)
}

// zrangebyscore handles ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count].
func (s *Store) zrangebyscore(b []byte, args []string) []byte {
	lo, loEx, err1 := parseBound(args[2])
	hi, hiEx, err2 := parseBound(args[3])
	if err1 != nil || err2 != nil {
		return resp.AppendError(b, "ERR min or max is not a float")
	}
	withScores, offset, count := false, 0, -1
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return resp.AppendError(b, "ERR syntax error")
			}
			offset, _ = strconv.Atoi(args[i+1])
			count, _ = strconv.Atoi(args[i+2])
			i += 2
		default:
			return resp.AppendError(b, "ERR syntax error")
		}
	}
	members := s.zrange(args[1], lo, loEx, hi, hiEx)
	members = members[min(offset, len(members)):]
	if count >= 0 && count < len(members) {
		members = members[:count]
	}
	n := len(members)
	if withScores {
		n *= 2
	}
	b = resp.AppendArray(b, n)
	e := s.live(args[1])
	for _, m := range members {
		b = resp.AppendBulkString(b, m)
		if withScores {
			b = resp.AppendBulkString(b, strconv.FormatFloat(e.zset[m], 'f', -1, 64))
		}
	}
	return b
}

func (s *Store) zrem(b []byte, args []string) []byte {
	var n int64
	if e := s.live(args[1]); e != nil && e.zset != nil {
		for _, m := range args[2:] {
			if _, ok := e.zset[m]; ok {
				s.zdel(e, m)
				n++
			}
		}
		s.dropEmpty(args[1], e)
	}
	return resp.AppendInt(b, n)
}

func (s *Store) zremrangebyscore(b []byte, args []string) []byte {
	lo, loEx, err1 := parseBound(args[2])
	hi, hiEx, err2 := parseBound(args[3])
	if err1 != nil || err2 != nil {
		return resp.AppendError(b, "ERR min or max is not a float")
	}
	members := s.zrange(args[1], lo, loEx, hi, hiEx)
	if e := s.live(args[1]); e != nil {
		for _, m := range members {
			s.zdel(e, m)
		}
		s.dropEmpty(args[1], e)
	}
	return resp.AppendInt(b, int64(len(members)))
}

// zremrangebyrank removes members by position in score order; a negative
// rank counts from the end, as on a real server.
func (s *Store) zremrangebyrank(b []byte, args []string) []byte {
	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		return resp.AppendError(b, "ERR value is not an integer or out of range")
	}
	e := s.live(args[1])
	if e == nil || e.zset == nil {
		return resp.AppendInt(b, 0)
	}
	members := sorted(e)
	n := len(members)
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start = max(start, 0) // a stop still negative means the range is empty
	if start > stop || start >= n {
		return resp.AppendInt(b, 0)
	}
	stop = min(stop, n-1)
	for _, m := range members[start : stop+1] {
		s.zdel(e, m)
	}
	s.dropEmpty(args[1], e)
	return resp.AppendInt(b, int64(stop-start+1))
}

func (s *Store) zcard(b []byte, args []string) []byte {
	if e := s.live(args[1]); e != nil && e.zset != nil {
		return resp.AppendInt(b, int64(len(e.zset)))
	}
	return resp.AppendInt(b, 0)
}

func (s *Store) zdel(e *entry, m string) {
	shrink := memberOverhead + int64(len(m))
	delete(e.zset, m)
	e.size -= shrink
	s.used -= shrink
}

// dropEmpty removes a sorted set its last member left, as a real server does.
func (s *Store) dropEmpty(k string, e *entry) {
	if len(e.zset) == 0 {
		s.remove(k, e)
	}
}

// zrange lists a sorted set's members with scores in [lo, hi], ordered by
// score then member.
func (s *Store) zrange(k string, lo float64, loEx bool, hi float64, hiEx bool) []string {
	e := s.live(k)
	if e == nil || e.zset == nil {
		return nil
	}
	var members []string
	for m, sc := range e.zset {
		if sc < lo || sc > hi || (loEx && sc == lo) || (hiEx && sc == hi) {
			continue
		}
		members = append(members, m)
	}
	sortMembers(e, members)
	return members
}

func sorted(e *entry) []string {
	members := make([]string, 0, len(e.zset))
	for m := range e.zset {
		members = append(members, m)
	}
	sortMembers(e, members)
	return members
}

func sortMembers(e *entry, members []string) {
	slices.SortFunc(members, func(a, b string) int {
		if e.zset[a] != e.zset[b] {
			if e.zset[a] < e.zset[b] {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	})
}

// parseBound reads a ZRANGEBYSCORE bound: a float, -inf, +inf, or "(" for exclusive.
func parseBound(s string) (float64, bool, error) {
	exclusive := strings.HasPrefix(s, "(")
	s = strings.TrimPrefix(s, "(")
	switch s {
	case "-inf":
		return math.Inf(-1), exclusive, nil
	case "+inf", "inf":
		return math.Inf(1), exclusive, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, exclusive, err
}

package memstore

import (
	"strconv"
	"strings"

	"github.com/we-be/tritium/internal/resp"
)

// SCAN: bucketed cursors that never hand a key out twice, and glob matching.

// scanPageMax bounds one SCAN page, whatever COUNT asks: a page is built under
// the store's lock, and KEYS is unsupported for the same reason.
const scanPageMax = 10000

// scan walks the buckets from the cursor until it has COUNT keys, and hands
// back the next bucket as the cursor; 0 once the last bucket is done.
func (s *Store) scan(b []byte, args []string) []byte {
	cursor, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return resp.AppendError(b, "ERR invalid cursor")
	}
	pattern, typ, count := "", "", 10
	for i := 2; i+1 < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "TYPE":
			typ = strings.ToLower(args[i+1])
		case "COUNT":
			if count, err = strconv.Atoi(args[i+1]); err != nil || count < 1 {
				return resp.AppendError(b, "ERR value is not an integer or out of range")
			}
			count = min(count, scanPageMax) // a hint, not a way to ask for the whole keyspace under one lock
		default:
			return resp.AppendError(b, "ERR syntax error")
		}
	}
	var keys []any
	i := cursor
	for ; i < buckets && len(keys) < count; i++ {
		for k := range s.bucket[i] {
			e := s.live(k)
			if e == nil || (pattern != "" && !match(pattern, k)) || (typ != "" && typeOf(e) != typ) {
				continue
			}
			keys = append(keys, []byte(k))
		}
	}
	if i >= buckets {
		i = 0
	}
	return resp.AppendValue(b, []any{[]byte(strconv.FormatUint(i, 10)), keys})
}

// match is the server's glob: * ? [set] [^set] [a-z] and \ escapes. It
// walks both strings once and backtracks only to the last *, so a pattern
// built to make a recursive matcher branch at every star (the CVE-2022-36021
// shape against Redis) costs O(len(pattern) * len(key)) here, not more.
func match(p, s string) bool {
	if !strings.ContainsAny(p, `*?[\`) {
		return p == s
	}
	pi, si := 0, 0
	starP, starS := -1, 0 // where the last * was, and where s stood then
	for si < len(s) {
		if pi < len(p) {
			switch p[pi] {
			case '*':
				starP, starS = pi, si
				pi++
				continue
			case '?':
				pi++
				si++
				continue
			case '[':
				if end := strings.IndexByte(p[pi+1:], ']'); end >= 0 {
					set := p[pi+1 : pi+1+end]
					neg := strings.HasPrefix(set, "^")
					if inSet(strings.TrimPrefix(set, "^"), s[si]) != neg {
						pi += end + 2
						si++
						continue
					}
					break // no match here: fall back to the last star
				}
				if s[si] == '[' {
					pi++
					si++
					continue
				}
			case '\\':
				if pi+1 < len(p) && p[pi+1] == s[si] {
					pi += 2
					si++
					continue
				}
			default:
				if p[pi] == s[si] {
					pi++
					si++
					continue
				}
			}
		}
		if starP < 0 {
			return false
		}
		starS++ // let the last * swallow one more byte and try again from there
		pi, si = starP+1, starS
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

func inSet(set string, c byte) bool {
	for i := 0; i < len(set); i++ {
		switch {
		case set[i] == '\\' && i+1 < len(set):
			i++
			if set[i] == c {
				return true
			}
		case i+2 < len(set) && set[i+1] == '-':
			if set[i] <= c && c <= set[i+2] {
				return true
			}
			i += 2
		case set[i] == c:
			return true
		}
	}
	return false
}

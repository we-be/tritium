package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadDotenv parses KEY=value lines. Blank lines and # comments are skipped,
// an unquoted value ends at " #", and a value opened with a quote may span
// lines until the closing quote.
func ReadDotenv(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	vals := map[string]string{}
	sc := bufio.NewScanner(f)
	var key, quoted string
	var quote byte
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())

		if quote != 0 {
			quoted += "\n" + line
			if strings.HasSuffix(line, string(quote)) {
				vals[key] = strings.TrimSuffix(quoted, string(quote))
				quote = 0
			}
			continue
		}
		if line == "" || line[0] == '#' {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: expected KEY=value", path, n)
		}
		key, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if len(v) > 0 && (v[0] == '"' || v[0] == '\'') {
			if end := strings.IndexByte(v[1:], v[0]); end >= 0 {
				vals[key] = v[1 : 1+end] // anything after the closing quote is a comment
			} else {
				quote, quoted = v[0], v[1:]
			}
			continue
		}
		v, _, _ = strings.Cut(v, " #")
		vals[key] = strings.TrimSpace(v)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if quote != 0 {
		return nil, fmt.Errorf("%s: unterminated quoted value for %s", path, key)
	}
	return vals, nil
}

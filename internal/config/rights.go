package config

import (
	"errors"
	"fmt"
	"strings"
)

// SurfacePrefix marks a named rights set in a dotenv file:
// SURFACE_<name>=<rights>. In the process environment the same entry is
// TRITIUM_SURFACE_<name>, for the reason USER_ has TRITIUM_USER_. A surface
// is not a USERS_FILE line: every name in that file is a user's, and a
// surface has no password to put on the left of the ":".
const (
	SurfacePrefix    = "SURFACE_"
	EnvSurfacePrefix = "TRITIUM_SURFACE_"
)

// Rights are the keys a principal may reach, and the whole policy language:
// a user today, a peer once a peer is a principal too. A spec is
// ";"-separated clauses of <r|w|rw>:<key-or-prefix>[,...]; an entry ending in
// ":" or "/" covers every key under it and any other names one key exactly,
// so "fleet" grants that key alone and "sig:" everything below it. A clause
// may instead be @<surface>, which stands for what that surface names.
type Rights struct {
	Read  []string // key prefixes this principal may read
	Write []string // key prefixes this principal may write
}

// surfaces collects every SURFACE_<name> entry. Load parses them before
// users, because a user may hold one by name.
func surfaces(vals map[string]string) (map[string]Rights, error) {
	raw := entries(map[string]string{}, vals, SurfacePrefix, EnvSurfacePrefix)
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]Rights, len(raw))
	for name, spec := range raw {
		if !plainName(name) {
			return nil, fmt.Errorf("%s%s: a surface's name is letters, digits, '_' and '-'", SurfacePrefix, name)
		}
		// A surface names key prefixes and never another surface, so a name is
		// one hop from the keys it grants and no chain has to be followed to
		// see what a credential can reach.
		r, err := parseRights(spec, func(ref string) (Rights, error) {
			return Rights{}, fmt.Errorf("@%s: a surface names key prefixes, never another surface", ref)
		})
		if err != nil {
			return nil, fmt.Errorf("%s%s: %w", SurfacePrefix, name, err)
		}
		out[name] = r
	}
	return out, nil
}

// ParseRights reads one rights spec, expanding an @<surface> clause to what
// that surface names. Inline clauses and named ones mix in any order, so a
// credential can hold a surface plus a key of its own: "@public;w:node:mine".
func ParseRights(spec string, surfaces map[string]Rights) (Rights, error) {
	return parseRights(spec, func(name string) (Rights, error) {
		s, ok := surfaces[name]
		if !ok {
			return Rights{}, fmt.Errorf("@%s: no %s%s is configured", name, SurfacePrefix, name)
		}
		return s, nil
	})
}

// parseRights reads the clauses, handing each @<surface> to expand — which is
// what decides whether a name resolves at all, since a surface's own spec may
// not hold one.
func parseRights(spec string, expand func(name string) (Rights, error)) (Rights, error) {
	var r Rights
	for clause := range strings.SplitSeq(spec, ";") {
		if clause = strings.TrimSpace(clause); clause == "" {
			continue
		}
		if name, ok := strings.CutPrefix(clause, "@"); ok {
			s, err := expand(name)
			if err != nil {
				return Rights{}, err
			}
			r.Read = append(r.Read, s.Read...)
			r.Write = append(r.Write, s.Write...)
			continue
		}
		perm, list, ok := strings.Cut(clause, ":")
		if !ok {
			return Rights{}, fmt.Errorf("%q is not <r|w|rw>:<prefix>[,...] or @<surface>", clause)
		}
		var prefixes []string
		for p := range strings.SplitSeq(list, ",") {
			if p = strings.TrimSpace(p); p != "" {
				prefixes = append(prefixes, p)
			}
		}
		switch strings.ToLower(strings.TrimSpace(perm)) {
		case "r":
			r.Read = append(r.Read, prefixes...)
		case "w":
			r.Write = append(r.Write, prefixes...)
		case "rw", "wr":
			r.Read = append(r.Read, prefixes...)
			r.Write = append(r.Write, prefixes...)
		default:
			return Rights{}, fmt.Errorf("%q is not r, w or rw", perm)
		}
	}
	if len(r.Read) == 0 && len(r.Write) == 0 {
		return Rights{}, errors.New("no rights, so it could reach nothing")
	}
	return r, nil
}

// plainName holds a surface's name to what a variable name carries, so
// SURFACE_<name> and @<name> can only ever mean the same string.
func plainName(s string) bool {
	return s != "" && !strings.ContainsFunc(s, func(r rune) bool {
		return !(r == '_' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
	})
}

// May reports whether key is one of rights: named exactly, or under a right
// that ends in ":" or "/".
func May(rights []string, key string) bool {
	for _, r := range rights {
		if key == r || (strings.HasSuffix(r, ":") || strings.HasSuffix(r, "/")) && strings.HasPrefix(key, r) {
			return true
		}
	}
	return false
}

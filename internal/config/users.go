package config

import (
	"fmt"
	"maps"
	"os"
	"strings"
)

// UserPrefix marks a client credential in the environment or a dotenv file:
// USER_<name>=<password>:<rights>. UsersFileVar names a file holding the
// same entries one per line, with the bare name on the left of the "=".
const (
	UserPrefix   = "USER_"
	UsersFileVar = "USERS_FILE"
)

// User is a client that gets less than AUTH_PASSWORD grants: it authenticates
// as AUTH <name> <password> and may only touch keys under the prefixes it
// names. Peer commands are never one of them.
//
//	USER_gateway=s3cret:rw:node:gateway,sig:gateway;r:board:,fleet,node:,id:
//
// rights is ";"-separated clauses of <r|w|rw>:<prefix>[,<prefix>...]. A
// prefix is matched against the whole key, so "fleet" grants that one key
// and "sig:" everything under it. A password may not contain ":".
type User struct {
	Name     string
	Password string
	Read     []string // key prefixes this user may read
	Write    []string // key prefixes this user may write
}

// users collects every USER_<name> entry: UsersFileVar first, then the
// dotenv file, then the process environment, each overriding the last.
func users(vals map[string]string, file string) (map[string]User, error) {
	raw := map[string]string{}
	if file != "" {
		fv, err := ReadDotenv(file)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", UsersFileVar, err)
		}
		maps.Copy(raw, fv)
	}
	for k, v := range vals {
		if name, ok := strings.CutPrefix(k, UserPrefix); ok {
			raw[name] = v
		}
	}
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		if name, ok := strings.CutPrefix(k, UserPrefix); ok {
			raw[name] = v
		}
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]User, len(raw))
	for name, spec := range raw {
		u, err := ParseUser(name, spec)
		if err != nil {
			return nil, err
		}
		out[name] = u
	}
	return out, nil
}

// ParseUser reads one <password>:<rights> entry.
func ParseUser(name, spec string) (User, error) {
	if name == "" || name == "default" || name == "peer" {
		return User{}, fmt.Errorf("%s%s: default and peer are the node's own credentials", UserPrefix, name)
	}
	password, rights, ok := strings.Cut(spec, ":")
	if !ok || password == "" {
		return User{}, fmt.Errorf("%s%s: expected <password>:<rights>, and a password may not contain ':'", UserPrefix, name)
	}
	u := User{Name: name, Password: password}
	for clause := range strings.SplitSeq(rights, ";") {
		if clause = strings.TrimSpace(clause); clause == "" {
			continue
		}
		perm, list, ok := strings.Cut(clause, ":")
		if !ok {
			return User{}, fmt.Errorf("%s%s: %q is not <r|w|rw>:<prefix>[,...]", UserPrefix, name, clause)
		}
		var prefixes []string
		for p := range strings.SplitSeq(list, ",") {
			if p = strings.TrimSpace(p); p != "" {
				prefixes = append(prefixes, p)
			}
		}
		switch strings.ToLower(strings.TrimSpace(perm)) {
		case "r":
			u.Read = append(u.Read, prefixes...)
		case "w":
			u.Write = append(u.Write, prefixes...)
		case "rw", "wr":
			u.Read = append(u.Read, prefixes...)
			u.Write = append(u.Write, prefixes...)
		default:
			return User{}, fmt.Errorf("%s%s: %q is not r, w or rw", UserPrefix, name, perm)
		}
	}
	if len(u.Read) == 0 && len(u.Write) == 0 {
		return User{}, fmt.Errorf("%s%s: no rights, so it could reach nothing", UserPrefix, name)
	}
	return u, nil
}

// May reports whether key is under one of prefixes.
func May(prefixes []string, key string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

package config

import (
	"fmt"
	"maps"
	"os"
	"strings"
)

// UserPrefix marks a client credential in a dotenv file:
// USER_<name>=<password>:<rights>. In the process environment the same entry
// is TRITIUM_USER_<name>, since USER_ID and friends live there too.
// UsersFileVar names a file holding the entries one per line, with the bare
// name on the left of the "=".
const (
	UserPrefix    = "USER_"
	EnvUserPrefix = "TRITIUM_USER_"
	UsersFileVar  = "USERS_FILE"
)

// PeerPrefix marks a node's credential: PEER_<name>=<password>:<rights>, a
// peer that authenticates by name and is held to what its rights name —
// what it may write through this node, and, in time, what it is sent. The
// built-in "peer" holds everything. TRITIUM_PEER_<name> in the process
// environment; not a USERS_FILE line, since every name there is a user's.
const (
	PeerPrefix    = "PEER_"
	EnvPeerPrefix = "TRITIUM_PEER_"
)

// User is a client that gets less than AUTH_PASSWORD grants: it authenticates
// as AUTH <name> <password> and may only touch keys its Rights name. Peer
// commands are never among them.
//
//	USER_gateway=s3cret:rw:node:gateway,sig:gateway;r:board:,fleet,node:,id:
//	USER_bob=s3cret:@public
//
// A password may not contain ":", since that is where the rights start.
type User struct {
	Name     string
	Password string
	Rights   // what it may read and write, inline or by surface name
}

// users collects every USER_<name> entry: UsersFileVar first, then the
// dotenv file, then the process environment, each overriding the last.
func users(vals map[string]string, file string, surfaces map[string]Rights) (map[string]User, error) {
	raw := map[string]string{}
	if file != "" {
		fv, err := ReadDotenv(file)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", UsersFileVar, err)
		}
		warnIfShared(file)
		maps.Copy(raw, fv)
	}
	entries(raw, vals, UserPrefix, EnvUserPrefix)
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]User, len(raw))
	for name, spec := range raw {
		u, err := ParseUser(name, spec, surfaces)
		if err != nil {
			return nil, err
		}
		out[name] = u
	}
	return out, nil
}

// entries collects one prefix's <name>=<spec> entries into raw: the dotenv
// file first, then the process environment, which overrides it. USER_ and
// SURFACE_ are gathered the same way, and both take the TRITIUM_ prefix in
// the environment so that nothing already living there is read as one.
func entries(raw, vals map[string]string, prefix, envPrefix string) map[string]string {
	for k, v := range vals {
		if name, ok := strings.CutPrefix(k, prefix); ok {
			raw[name] = v
		}
	}
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		if name, ok := strings.CutPrefix(k, envPrefix); ok {
			raw[name] = v
		}
	}
	return raw
}

// peers collects every PEER_<name> entry: nodes held to rights, one
// credential each, never sharing a name with a user since AUTH <name> must
// say which it is.
func peers(vals map[string]string, surfaces map[string]Rights, users map[string]User) (map[string]User, error) {
	raw := entries(map[string]string{}, vals, PeerPrefix, EnvPeerPrefix)
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]User, len(raw))
	for name, spec := range raw {
		if _, taken := users[name]; taken {
			return nil, fmt.Errorf("%s%s and %s%s: one name is one credential", PeerPrefix, name, UserPrefix, name)
		}
		p, err := credential(PeerPrefix, name, spec, surfaces)
		if err != nil {
			return nil, err
		}
		out[name] = p
	}
	return out, nil
}

// ParseUser reads one <password>:<rights> entry, drawing on the surfaces an
// @<name> clause in its rights may hold.
func ParseUser(name, spec string, surfaces map[string]Rights) (User, error) {
	return credential(UserPrefix, name, spec, surfaces)
}

// credential is what a USER_ and a PEER_ entry have in common: a name that
// is not the node's own, a password, and rights.
func credential(prefix, name, spec string, surfaces map[string]Rights) (User, error) {
	if name == "" || name == "default" || name == "peer" {
		return User{}, fmt.Errorf("%s%s: default and peer are the node's own credentials", prefix, name)
	}
	password, spec, ok := strings.Cut(spec, ":")
	if !ok || password == "" {
		return User{}, fmt.Errorf("%s%s: expected <password>:<rights>, and a password may not contain ':'", prefix, name)
	}
	rights, err := ParseRights(spec, surfaces)
	if err != nil {
		return User{}, fmt.Errorf("%s%s: %w", prefix, name, err)
	}
	return User{Name: name, Password: password, Rights: rights}, nil
}

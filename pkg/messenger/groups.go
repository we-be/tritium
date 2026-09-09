package messenger

// A group is a named roster — member names and a version — signed by
// whoever created it, published at grp:<name> the same first-come way a
// name claims id:<name>. There are no group keys: sending to a group is
// just Client.SendAll to every member (and so to every one of their
// devices) over the ordinary pairwise sessions, with the group's name
// folded into the plaintext so a receiving Client can tell which group a
// message belongs to and show it as such. Only the creator's signature can
// change the roster; anyone can read it, same as a bundle.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
)

const groupLabel = "tritium-messenger-v1 group"

// What a group roster is refused for.
var (
	ErrBadGroup   = errors.New("messenger: group roster is malformed or its signature is invalid")
	ErrNotCreator = errors.New("messenger: only the group's creator can change its roster")
)

// Group is a roster of member names, signed by its creator.
type Group struct {
	Name    string   `json:"name"`
	Creator string   `json:"creator"` // the name whose identity signs this roster
	Version int      `json:"version"` // orders publications; one older than already seen is refused
	Members []string `json:"members"` // names; each member's devices are reached through its own roster
	Sig     []byte   `json:"sig"`     // the creator's signature over the rest
}

func (g Group) signed() []byte {
	out := field([]byte(groupLabel), []byte(g.Name))
	out = field(out, []byte(g.Creator))
	out = binary.BigEndian.AppendUint64(out, uint64(g.Version))
	out = binary.BigEndian.AppendUint32(out, uint32(len(g.Members)))
	for _, m := range g.Members {
		out = field(out, []byte(m))
	}
	return out
}

// Verify checks the roster was signed, unmodified, by creator's identity key.
func (g Group) Verify(creator Bundle) error {
	if g.Creator != creator.Name || !validVersion(g.Version) {
		return ErrBadGroup
	}
	if !ed25519.Verify(ed25519.PublicKey(creator.Signing), g.signed(), g.Sig) {
		return ErrBadGroup
	}
	return nil
}

// CreateGroup claims name as a group, first come like a user name, with this
// client's identity as creator.
func (c *Client) CreateGroup(name string, members []string) (Group, error) {
	g := Group{Name: name, Creator: c.id.Name, Version: 1, Members: members}
	g.Sig = ed25519.Sign(c.id.signing, g.signed())
	data, err := json.Marshal(g)
	if err != nil {
		return Group{}, err
	}
	v, err := c.t.Do("SET", "grp:"+name, string(data), "EX", strconv.Itoa(bundleTTL), "NX")
	if err != nil {
		return Group{}, err
	}
	if v == nil {
		return Group{}, ErrNameTaken
	}
	return g, nil
}

// LookupGroup fetches a group's roster and verifies it against its creator's
// published identity.
func (c *Client) LookupGroup(name string) (Group, error) {
	v, err := c.t.Do("GET", "grp:"+name)
	if err != nil {
		return Group{}, err
	}
	raw, ok := v.([]byte)
	if !ok {
		return Group{}, fmt.Errorf("messenger: no group named %q", name)
	}
	var g Group
	if err := json.Unmarshal(raw, &g); err != nil || g.Name != name {
		return Group{}, ErrBadGroup
	}
	creator, err := c.Lookup(g.Creator)
	if err != nil {
		return Group{}, err
	}
	if err := g.Verify(creator); err != nil {
		return Group{}, err
	}
	if !c.current("grp:"+name, g.Version) {
		return Group{}, ErrStaleRoster
	}
	return g, nil
}

// AddMember and RemoveMember re-sign the roster with member added or
// removed; only the group's creator can, since only its Client holds the
// signing key the roster verifies against.
func (c *Client) AddMember(group, member string) (Group, error) {
	return c.editGroup(group, func(g *Group) {
		if !slices.Contains(g.Members, member) {
			g.Members = append(g.Members, member)
		}
	})
}

// RemoveMember is AddMember's inverse.
func (c *Client) RemoveMember(group, member string) (Group, error) {
	return c.editGroup(group, func(g *Group) {
		g.Members = slices.DeleteFunc(g.Members, func(m string) bool { return m == member })
	})
}

func (c *Client) editGroup(name string, edit func(*Group)) (Group, error) {
	g, err := c.LookupGroup(name)
	if err != nil {
		return Group{}, err
	}
	if g.Creator != c.id.Name {
		return Group{}, ErrNotCreator
	}
	edit(&g)
	g.Version = max(g.Version, c.rosters["grp:"+name]) + 1
	c.rosters["grp:"+name] = g.Version
	g.Sig = ed25519.Sign(c.id.signing, g.signed())
	data, err := json.Marshal(g)
	if err != nil {
		return Group{}, err
	}
	if _, err := c.t.Do("SET", "grp:"+name, string(data), "EX", strconv.Itoa(bundleTTL)); err != nil {
		return Group{}, err
	}
	return g, nil
}

// wrapGroup and unwrapGroup fold a group name into a plaintext body so it
// travels inside the same encryption as the message itself — the server
// never sees it, matching the messenger's sealed-sender design. The magic
// prefix is checked against the group's signed roster before it is ever
// trusted (see (*Client).attributeGroup): an ordinary peer cannot make a
// plain message look like it came from a group they are not a member of.
var groupMagic = []byte("tritium-messenger-group-v1:")

func wrapGroup(group string, body []byte) []byte {
	out := append([]byte{}, groupMagic...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(group)))
	out = append(out, group...)
	return append(out, body...)
}

func unwrapGroup(pt []byte) (group string, body []byte, ok bool) {
	n := len(groupMagic)
	if len(pt) < n+2 || !bytes.HasPrefix(pt, groupMagic) {
		return "", pt, false
	}
	nameLen := int(binary.BigEndian.Uint16(pt[n : n+2]))
	if len(pt) < n+2+nameLen {
		return "", pt, false
	}
	return string(pt[n+2 : n+2+nameLen]), pt[n+2+nameLen:], true
}

// SendGroup sends body to every member of a group (and each of their
// devices), tagging the plaintext with the group's name. It is a pairwise
// send per recipient over their own session — there is no group key — so a
// member added later cannot read what was sent before, and one removed
// keeps whatever it already has. The sender is skipped even if listed as a
// member, since messaging yourself is a no-op.
func (c *Client) SendGroup(name string, body []byte) error {
	g, err := c.LookupGroup(name)
	if err != nil {
		return err
	}
	tagged := wrapGroup(name, body)
	var errs []error
	for _, member := range g.Members {
		if member == c.id.Name {
			continue
		}
		if err := c.SendAll(member, tagged); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", member, err))
		}
	}
	return errors.Join(errs...)
}

// attributeGroup promotes a group tag on m.Body to m.Group, but only once
// the named group's signed roster confirms the sender belongs to it: the tag
// is a hint from the sender, never the source of truth, so a forged one just
// fails to attribute rather than spoofing a group the sender isn't in.
func (c *Client) attributeGroup(m Message) Message {
	name, body, ok := unwrapGroup(m.Body)
	if !ok {
		return m
	}
	g, err := c.LookupGroup(name)
	if err != nil || !m.Verified || (m.From.Name != g.Creator && !slices.Contains(g.Members, m.From.Name)) {
		return m // the creator counts as belonging even when not listed among Members; an unverified name counts for nothing
	}
	m.Group, m.Body = name, body
	return m
}

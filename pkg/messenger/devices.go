package messenger

// A name may hold several devices. The name's original identity keeps
// publishing at id:<name> exactly as before; a device is a second, ordinary
// Identity of its own — its own signing and agreement keys, its own
// rotating prekey, its own sessions and mailboxes — published under
// id:<name>/<device> the same way any identity publishes itself. What makes
// it a device rather than a stranger with a similar name is a certificate:
// the primary identity signs the device's long-term keys into a roster at
// devices:<name>, so fan-out only ever trusts a device the name holder
// actually vouched for. There is no shared state between a name's devices;
// a message delivered to one is not visible on another.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

const deviceRosterLabel = "tritium-messenger-v1 device-roster"

var (
	ErrBadDeviceRoster = errors.New("messenger: device roster is malformed or its signature is invalid")
	ErrNotDevice       = errors.New("messenger: bundle's name or keys do not match the device being authorized")
	ErrStaleRoster     = errors.New("messenger: roster is older than one already seen")
)

// current records a roster version and reports whether it is at least the
// newest this client has seen under key. A roster is a signed value anyone
// can write back to the store, so without this an old copy — one that still
// lists a device or member since removed — could be replayed by whoever kept it.
func (c *Client) current(key string, version int) bool {
	if version < c.rosters[key] {
		return false
	}
	c.rosters[key] = version
	return true
}

// DeviceCert is one device's long-term keys as certified by the name's
// primary identity. The device's own bundle (published separately, and
// rotating its own prekey over time) must carry exactly these keys to be
// trusted; the cert never expires them, so it does not need to be reissued
// when the device's prekey rotates.
type DeviceCert struct {
	Device    string `json:"device"`    // label; the device publishes its bundle at id:<name>/<device>
	Signing   []byte `json:"signing"`   // the device's Ed25519 public key
	Agreement []byte `json:"agreement"` // the device's X25519 public key
}

func (c DeviceCert) signed(name string) []byte {
	var out []byte
	for _, f := range [][]byte{[]byte(name), []byte(c.Device), c.Signing, c.Agreement} {
		out = field(out, f)
	}
	return out
}

// field appends f behind its length, so two fields can never be read as
// one no matter what bytes they hold.
func field(out, f []byte) []byte {
	out = binary.BigEndian.AppendUint32(out, uint32(len(f)))
	return append(out, f...)
}

// maxRosterVersion bounds a version so it cannot be pushed past what any
// later, honest roster could beat; int64, since int is 32 bits on a Pi Zero.
const maxRosterVersion int64 = 1 << 31

func validVersion(v int) bool { return v >= 0 && int64(v) < maxRosterVersion }

// DeviceRoster is every device certified under one name, signed as a unit so
// a change is atomic and Version orders successive publications.
type DeviceRoster struct {
	Name    string       `json:"name"`
	Version int          `json:"version"`
	Devices []DeviceCert `json:"devices"`
	Sig     []byte       `json:"sig"` // the primary identity's signature over Name, Version and every cert
}

func (r DeviceRoster) signed() []byte {
	out := field([]byte(deviceRosterLabel), []byte(r.Name))
	out = binary.BigEndian.AppendUint64(out, uint64(r.Version))
	out = binary.BigEndian.AppendUint32(out, uint32(len(r.Devices)))
	for _, d := range r.Devices {
		out = field(out, d.signed(r.Name))
	}
	return out
}

// Verify checks the roster was signed, unmodified, by primary's identity key.
func (r DeviceRoster) Verify(primary Bundle) error {
	if r.Name != primary.Name || !validVersion(r.Version) {
		return ErrBadDeviceRoster
	}
	if !ed25519.Verify(ed25519.PublicKey(primary.Signing), r.signed(), r.Sig) {
		return ErrBadDeviceRoster
	}
	return nil
}

// AuthorizeDevice certifies bundle's long-term keys as a device of this
// client's name and publishes the updated roster. Only the name holder can
// call this productively: the roster verifies against the primary identity's
// signing key, which only they hold. bundle is the device's own published
// bundle (fetch it with Lookup(name+"/"+device) after the device runs its
// own init), so a device cannot attach itself — its holder must already
// have it and choose to vouch for it.
func (c *Client) AuthorizeDevice(device string, bundle Bundle) (DeviceRoster, error) {
	if err := bundle.Verify(); err != nil {
		return DeviceRoster{}, err
	}
	if bundle.Name != c.id.Name+"/"+device {
		return DeviceRoster{}, ErrNotDevice
	}
	roster, err := c.deviceRoster()
	if err != nil {
		return DeviceRoster{}, err
	}
	cert := DeviceCert{Device: device, Signing: bundle.Signing, Agreement: bundle.Agreement}
	replaced := false
	for i, d := range roster.Devices {
		if d.Device == device {
			roster.Devices[i], replaced = cert, true
			break
		}
	}
	if !replaced {
		roster.Devices = append(roster.Devices, cert)
	}
	roster.Version = max(roster.Version, c.rosters["devices:"+c.id.Name]) + 1 // past anything a receiver has seen, even if the store holds an older copy
	c.rosters["devices:"+c.id.Name] = roster.Version
	roster.Sig = ed25519.Sign(c.id.signing, roster.signed())
	data, err := json.Marshal(roster)
	if err != nil {
		return DeviceRoster{}, err
	}
	_, err = c.t.Do("SET", "devices:"+c.id.Name, string(data), "EX", strconv.Itoa(bundleTTL))
	return roster, err
}

func (c *Client) deviceRoster() (DeviceRoster, error) {
	v, err := c.t.Do("GET", "devices:"+c.id.Name)
	if err != nil {
		return DeviceRoster{}, err
	}
	raw, ok := v.([]byte)
	if !ok {
		return DeviceRoster{Name: c.id.Name}, nil
	}
	var r DeviceRoster
	if err := json.Unmarshal(raw, &r); err != nil || r.Name != c.id.Name {
		return DeviceRoster{Name: c.id.Name}, nil // no roster yet, or one from a stale key: start fresh
	}
	return r, nil
}

// LookupAll returns name's primary bundle followed by every device whose
// certificate the primary has signed. A bundle published at a device's key
// that was never certified, or whose keys don't match its certificate — a
// squatted or stale key, since names are first come per key — is left out
// rather than trusted.
func (c *Client) LookupAll(name string) ([]Bundle, error) {
	primary, err := c.Lookup(name)
	if err != nil {
		return nil, err
	}
	out := []Bundle{primary}
	v, err := c.t.Do("GET", "devices:"+name)
	if err != nil {
		return nil, err
	}
	raw, ok := v.([]byte)
	if !ok {
		return out, nil // no devices registered
	}
	var roster DeviceRoster
	if err := json.Unmarshal(raw, &roster); err != nil || roster.Verify(primary) != nil || !c.current("devices:"+name, roster.Version) {
		return out, nil // malformed, not signed by this name's identity, or older than one seen: the primary still works
	}
	for _, cert := range roster.Devices {
		b, err := c.Lookup(name + "/" + cert.Device)
		if err != nil {
			continue // not published, or its bundle expired
		}
		if !bytes.Equal(b.Signing, cert.Signing) || !bytes.Equal(b.Agreement, cert.Agreement) {
			continue // the key holds a different bundle than the one certified
		}
		out = append(out, b)
	}
	return out, nil
}

// Devices returns every device verified under name, the primary excluded.
func (c *Client) Devices(name string) ([]Bundle, error) {
	all, err := c.LookupAll(name)
	if err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, nil
	}
	return all[1:], nil
}

// SendAll delivers body to name's primary identity and every device verified
// under it, each over its own pairwise session. A device that has no session
// yet gets its own hello; one that never answers just never gets the
// message read, same as any offline peer. Devices do not share state, so a
// message sent this way is not visible on a device that was not sent it.
func (c *Client) SendAll(name string, body []byte) error {
	bundles, err := c.LookupAll(name)
	if err != nil {
		return err
	}
	var errs []error
	for _, b := range bundles {
		if err := c.Send(b, body); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b.Name, err))
		}
	}
	return errors.Join(errs...)
}

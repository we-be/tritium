package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	env := "# comment\nSECURE_STORE_ADDRESS='store:6379' # inline\nRPC_ADDRESS=:9000 # old name still honored\nAUTH_PASSWORD=\"multi\nline\"\nMAX_SERVER_CONNECTIONS=2\nTLS_CLIENT_AUTH=true\nTLS_CA=ca.pem\n"
	if err := os.WriteFile(path, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LISTEN_ADDRESS", ":9001") // environment wins over the file

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	one := 1
	want := Config{StoreAddr: "store:6379", ListenAddr: ":9001", Password: "multi\nline", PoolSize: 2, MaxClients: DefaultMaxClients, Ownership: true, Electronegativity: &one, TLSClientAuth: true, TLSCA: "ca.pem"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	for _, bad := range []string{"MAX_SERVER_CONNECTIONS=lots\n", "TLS_CERT=only.pem\n", "TLS_CLIENT_AUTH=true\n", "SECURE_STORE_TLS=true\n"} {
		if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatalf("%q was accepted", bad)
		}
	}
}

func TestLoadLocal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.env")
	os.WriteFile(path, []byte("LISTEN_ADDRESS=:9090\nAUTH_PASSWORD=a\nSECURE_STORE_PASSWORD=s\nTLS_CA=/ca.pem\n"), 0o600)
	loc, err := LoadLocal(path)
	if err != nil || loc != (Local{Addr: "127.0.0.1:9090", Password: "a", StorePassword: "s", CA: "/ca.pem"}) {
		t.Fatalf("LoadLocal = %+v, %v", loc, err)
	}
}

// A dotenv file written with shell habits loads as meant, or fails loudly.
func TestDotenvExportAndQuotes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	os.WriteFile(path, []byte("export AUTH_PASSWORD=secret\nPEER_PASSWORD='with space'\n"), 0o600)
	vals, err := ReadDotenv(path)
	if err != nil || vals["AUTH_PASSWORD"] != "secret" || vals["PEER_PASSWORD"] != "with space" {
		t.Fatalf("%v, %v", vals, err)
	}
	os.WriteFile(path, []byte("AUTH_PASSWORD='don't-stop'\n"), 0o600)
	if _, err := ReadDotenv(path); err == nil {
		t.Fatal("a quote inside a quoted value loaded as a shorter password")
	}
}

// USER_ in the process environment is not a credential; TRITIUM_USER_ is.
func TestUsersComeFromTheRightPlace(t *testing.T) {
	t.Setenv("USER_ID", "1000")
	t.Setenv("TRITIUM_USER_svc", "pw:r:a:")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Users["ID"]; ok || cfg.Users["svc"].Password != "pw" {
		t.Fatalf("users: %v", cfg.Users)
	}
}

// A surface held by name grants exactly what writing its clauses inline would.
func TestSurfaceGrantsWhatItNames(t *testing.T) {
	const rights = "rw:hello:,mbx:,msg:;w:id:;r:id:,devices:,grp:"
	dir := t.TempDir()
	named, inline := filepath.Join(dir, "named.env"), filepath.Join(dir, "inline.env")
	os.WriteFile(named, []byte("SURFACE_public="+rights+"\nUSER_bob=pw:@public\n"), 0o600)
	os.WriteFile(inline, []byte("USER_bob=pw:"+rights+"\n"), 0o600)

	a, err := Load(named)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Load(inline)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Users["bob"].Rights, b.Users["bob"].Rights) {
		t.Fatalf("@public = %+v, inline = %+v", a.Users["bob"].Rights, b.Users["bob"].Rights)
	}
}

// A credential naming a surface that is not configured must not start as a
// credential with fewer rights than meant; the error says which name failed.
func TestUnconfiguredSurfaceIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	os.WriteFile(path, []byte("USER_bob=pw:@public\n"), 0o600)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "public") {
		t.Fatalf("err = %v", err)
	}
}

// A surface is one hop from a name to key prefixes: it may not hold another.
func TestSurfacesDoNotNest(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	os.WriteFile(path, []byte("SURFACE_base=r:board:\nSURFACE_more=@base;w:sig:\n"), 0o600)
	if _, err := Load(path); err == nil {
		t.Fatal("a surface naming a surface loaded")
	}
}

// A right names one key exactly unless it ends in a separator.
func TestMayIsExactWithoutASeparator(t *testing.T) {
	rights := []string{"fleet", "sig:", "node/"}
	for key, want := range map[string]bool{"fleet": true, "fleet-master-key": false, "sig:x:1": true, "sig": false, "node/a": true, "nodex": false} {
		if May(rights, key) != want {
			t.Errorf("May(%q) = %v", key, !want)
		}
	}
}

// A size takes a K, M or G suffix, with or without a B, and nothing else.
func TestParseBytes(t *testing.T) {
	for raw, want := range map[string]int64{"268435456": 268435456, "256M": 256 << 20, "1G": 1 << 30, "512K": 512 << 10, "64MB": 64 << 20, " 2k ": 2 << 10} {
		if got, err := ParseBytes(raw); err != nil || got != want {
			t.Fatalf("ParseBytes(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, bad := range []string{"", "-1", "1T", "lots"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Fatalf("ParseBytes(%q) was accepted", bad)
		}
	}
}

// A name is one credential: a PEER_ and a USER_ of the same name would
// leave AUTH <name> to guess which, so the config is refused.
func TestPeerAndUserNamesAreDistinct(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	os.WriteFile(path, []byte("USER_x=pw:r:a:\nPEER_x=pw:rw:b:\n"), 0o600)
	if _, err := Load(path); err == nil {
		t.Fatal("a peer and a user shared a name")
	}
	os.WriteFile(path, []byte("SURFACE_public=rw:pub:\nPEER_x=pw:@public\n"), 0o600)
	cfg, err := Load(path)
	if err != nil || cfg.Peers["x"].Write[0] != "pub:" {
		t.Fatalf("PEER_x: %+v, %v", cfg.Peers, err)
	}
}

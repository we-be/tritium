package config

import (
	"os"
	"path/filepath"
	"reflect"
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
	want := Config{StoreAddr: "store:6379", ListenAddr: ":9001", Password: "multi\nline", PoolSize: 2, MaxClients: DefaultMaxClients, Ownership: true, TLSClientAuth: true, TLSCA: "ca.pem"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	for _, bad := range []string{"MAX_SERVER_CONNECTIONS=lots\n", "TLS_CERT=only.pem\n", "TLS_CLIENT_AUTH=true\n"} {
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

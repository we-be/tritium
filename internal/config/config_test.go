package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	env := "# comment\nSECURE_STORE_ADDRESS='store:6379' # inline\nRPC_ADDRESS=:9000 # inline\nJOIN_ADDRESS=\"multi\nline\"\nMAX_SERVER_CONNECTIONS=2\n"
	if err := os.WriteFile(path, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RPC_ADDRESS", ":9001") // environment wins over the file

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := Config{StoreAddr: "store:6379", RPCAddr: ":9001", JoinAddr: "multi\nline", PoolSize: 2}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	if err := os.WriteFile(path, []byte("MAX_SERVER_CONNECTIONS=lots\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("non-integer pool size was accepted")
	}
}

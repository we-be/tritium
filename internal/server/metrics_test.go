package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/we-be/tritium/internal/config"
)

// A scraper gets the node's figures in the text format it expects, and
// nothing else on that port answers at all.
func TestMetricsEndpoint(t *testing.T) {
	s := startNode(t, config.Config{MetricsAddr: "127.0.0.1:0"})
	base := "http://" + s.MetricsAddr()

	res, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); ct != metricsContentType {
		t.Fatalf("content type %q, want %q", ct, metricsContentType)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tritium_node_info", "tritium_cluster_nodes", "tritium_writes_total", "tritium_connections_active", "tritium_store_keys"} {
		if !strings.Contains(string(body), "# TYPE "+name+" ") {
			t.Errorf("%s missing from:\n%s", name, body)
		}
	}

	other, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	other.Body.Close()
	if other.StatusCode != http.StatusNotFound {
		t.Fatalf("GET / = %d, want 404", other.StatusCode)
	}
}

package resp_test

import (
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
)

func TestCommandEncoding(t *testing.T) {
	got := resp.NewCommand("SET", "k", "v")
	if want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"; string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// One reply per RESP type, plus the empty-vs-null bulk distinction.
func TestReadValue(t *testing.T) {
	in := "+OK\r\n:42\r\n$5\r\nhello\r\n$0\r\n\r\n$-1\r\n*2\r\n$1\r\na\r\n:1\r\n*-1\r\n-ERR boom\r\n"
	r := resp.NewReader(strings.NewReader(in))
	want := []any{"OK", int64(42), []byte("hello"), []byte{}, nil, []any{[]byte("a"), int64(1)}, nil}
	for i, w := range want {
		got, err := r.ReadValue()
		if err != nil || !reflect.DeepEqual(got, w) {
			t.Fatalf("value %d: got %#v, %v; want %#v", i, got, err, w)
		}
	}
	var se *resp.ServerError
	if _, err := r.ReadValue(); !errors.As(err, &se) || se.Msg != "ERR boom" {
		t.Fatalf("error reply: got %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	conn, err := net.Dial("tcp", resptest.Addr(t))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r := resp.NewReader(conn)

	bin := "\x00\x01\xff"
	steps := []struct {
		cmd  resp.Command
		want any
	}{
		{resp.NewCommand("PING"), "PONG"},
		{resp.NewCommand("SET", "resp:k", bin), "OK"},
		{resp.NewCommand("GET", "resp:k"), []byte(bin)},
		{resp.NewCommand("MGET", "resp:k", "resp:missing"), []any{[]byte(bin), nil}},
		{resp.NewCommand("DEL", "resp:k"), int64(1)},
		{resp.NewCommand("GET", "resp:k"), nil},
	}
	for i, s := range steps {
		got, err := s.cmd.Do(conn, r)
		if err != nil || !reflect.DeepEqual(got, s.want) {
			t.Fatalf("step %d: got %#v, %v; want %#v", i, got, err, s.want)
		}
	}
}

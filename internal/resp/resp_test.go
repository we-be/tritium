package resp_test

import (
	"bytes"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/we-be/tritium/internal/resp"
	"github.com/we-be/tritium/internal/resptest"
)

func TestCommandEncoding(t *testing.T) {
	cmd := resp.NewCommand("SET", "k", "v")
	if want := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"; string(cmd) != want {
		t.Fatalf("got %q, want %q", cmd, want)
	}
	args, err := resp.NewReader(bytes.NewReader(cmd)).ReadCommand()
	if err != nil || !reflect.DeepEqual(args, []string{"SET", "k", "v"}) {
		t.Fatalf("ReadCommand: %q, %v", args, err)
	}
	if _, err := resp.NewReader(bytes.NewReader([]byte("+PING\r\n"))).ReadCommand(); !errors.Is(err, resp.ErrInvalidCommand) {
		t.Fatalf("non-array command: got %v", err)
	}
}

// Every reply type written with the Append helpers reads back as the
// matching Go value, including empty-vs-null bulk strings.
func TestReadValue(t *testing.T) {
	var in []byte
	in = resp.AppendSimpleString(in, "OK")
	in = resp.AppendInt(in, 42)
	in = resp.AppendBulk(in, []byte("hello"))
	in = resp.AppendBulk(in, []byte{})
	in = resp.AppendNull(in)
	in = resp.AppendArray(in, 2)
	in = resp.AppendBulkString(in, "a")
	in = resp.AppendInt(in, 1)
	in = append(in, "*-1\r\n"...)
	in = resp.AppendError(in, "ERR boom")

	r := resp.NewReader(bytes.NewReader(in))
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

func TestPrefix(t *testing.T) {
	got := resp.Prefix(resp.NewCommand("SETEX", "k", "60", "v"), "TRITIUM.REPLICATE")
	if string(got) != string(resp.NewCommand("TRITIUM.REPLICATE", "SETEX", "k", "60", "v")) {
		t.Fatalf("Prefix = %q", got)
	}
}

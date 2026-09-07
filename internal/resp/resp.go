// Package resp encodes commands and decodes replies in the Redis Serialization
// Protocol (RESP2), which Valkey, Redis, Garnet and friends all speak.
package resp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	// ErrInvalidType is returned when a value starts with an unknown type prefix.
	ErrInvalidType = errors.New("resp: invalid type prefix")
	// ErrInvalidCommand is returned when a command is not an array of bulk strings.
	ErrInvalidCommand = errors.New("resp: command must be an array of bulk strings")
)

// Type prefixes.
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// ServerError is an error reply ("-ERR ...") from the server. The connection
// is still healthy after one; only transport errors poison it.
type ServerError struct {
	Msg string
}

func (e *ServerError) Error() string { return "resp: " + e.Msg }

// Command is an encoded RESP command: an array of bulk strings.
type Command []byte

// NewCommand encodes args as a RESP array of bulk strings.
func NewCommand(args ...string) Command {
	n := 1 + len(strconv.Itoa(len(args))) + 2 // *<len>\r\n
	for _, a := range args {
		n += 1 + len(strconv.Itoa(len(a))) + 2 + len(a) + 2 // $<len>\r\n<data>\r\n
	}
	cmd := make(Command, 0, n)
	cmd = append(strconv.AppendInt(append(cmd, '*'), int64(len(args)), 10), '\r', '\n')
	for _, a := range args {
		cmd = append(strconv.AppendInt(append(cmd, '$'), int64(len(a)), 10), '\r', '\n')
		cmd = append(append(cmd, a...), '\r', '\n')
	}
	return cmd
}

// Prefix returns cmd with one more argument in front: "*N" becomes "*N+1"
// and the argument is inserted, so a serialized command can be wrapped in
// another (TRITIUM.REPLICATE <cmd>) without decoding it.
func Prefix(cmd Command, arg string) Command {
	nl := bytes.IndexByte(cmd, '\n')
	if nl < 2 || cmd[0] != '*' {
		return cmd
	}
	n, err := strconv.Atoi(string(cmd[1 : nl-1]))
	if err != nil {
		return cmd
	}
	out := make(Command, 0, len(cmd)+len(arg)+16)
	out = fmt.Appendf(out, "*%d\r\n$%d\r\n%s\r\n", n+1, len(arg), arg)
	return append(out, cmd[nl+1:]...)
}

// WriteTo writes the command with a single Write so it can never interleave
// with another writer on the same connection.
func (c Command) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(c)
	return int64(n), err
}

// Do writes the command to w and reads one reply from r.
func (c Command) Do(w io.Writer, r *Reader) (any, error) {
	if _, err := c.WriteTo(w); err != nil {
		return nil, fmt.Errorf("resp: write: %w", err)
	}
	return r.ReadValue()
}

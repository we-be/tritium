// Package resp encodes commands and decodes replies in the Redis Serialization
// Protocol (RESP2), which Valkey, Redis, Garnet and friends all speak.
package resp

import (
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ErrInvalidType is returned when a reply starts with an unknown type prefix.
var ErrInvalidType = errors.New("resp: invalid type prefix")

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
	cmd = fmt.Appendf(cmd, "*%d\r\n", len(args))
	for _, a := range args {
		cmd = fmt.Appendf(cmd, "$%d\r\n%s\r\n", len(a), a)
	}
	return cmd
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

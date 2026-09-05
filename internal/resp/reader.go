package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// maxBulkLen matches proto-max-bulk-len's default; anything larger is a
// corrupt stream, not data, and must not be allocated.
const maxBulkLen = 512 << 20

// Reader decodes RESP replies. Values decode as string (simple string), int64
// (integer), []byte (bulk string), []any (array) or untyped nil (null bulk
// string or null array). Error replies come back as a *ServerError.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadValue reads one reply of any type.
func (r *Reader) ReadValue() (any, error) {
	typ, err := r.r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("resp: read type: %w", err)
	}
	switch typ {
	case SimpleString:
		return r.readLineString()
	case Error:
		msg, err := r.readLineString()
		if err != nil {
			return nil, err
		}
		return nil, &ServerError{Msg: msg}
	case Integer:
		return r.readInteger()
	case BulkString:
		b, err := r.readBulk()
		if b == nil || err != nil {
			return nil, err // untyped nil for a null bulk string, not []byte(nil)
		}
		return b, nil
	case Array:
		a, err := r.readArray()
		if a == nil || err != nil {
			return nil, err
		}
		return a, nil
	default:
		return nil, fmt.Errorf("%w %q", ErrInvalidType, typ)
	}
}

// ReadCommand reads a client command: an array of bulk strings.
func (r *Reader) ReadCommand() ([]string, error) {
	v, err := r.ReadValue()
	if err != nil {
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, ErrInvalidCommand
	}
	args := make([]string, len(arr))
	for i, e := range arr {
		b, ok := e.([]byte)
		if !ok {
			return nil, ErrInvalidCommand
		}
		args[i] = string(b)
	}
	return args, nil
}

// ReadOK expects a "+OK" reply.
func (r *Reader) ReadOK() error {
	v, err := r.ReadValue()
	if err != nil {
		return err
	}
	if v != "OK" {
		return fmt.Errorf("resp: expected OK, got %v", v)
	}
	return nil
}

// ReadBulk expects a bulk string reply; a null bulk string returns nil, nil.
func (r *Reader) ReadBulk() ([]byte, error) {
	v, err := r.ReadValue()
	if err != nil {
		return nil, err
	}
	switch b := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return b, nil
	default:
		return nil, fmt.Errorf("resp: expected bulk string, got %T", v)
	}
}

// ReadInt expects an integer reply.
func (r *Reader) ReadInt() (int64, error) {
	v, err := r.ReadValue()
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("resp: expected integer, got %T", v)
	}
	return n, nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("resp: invalid line ending")
	}
	return line[:len(line)-2], nil
}

func (r *Reader) readLineString() (string, error) {
	line, err := r.readLine()
	return string(line), err
}

func (r *Reader) readInteger() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(line), 10, 64)
}

func (r *Reader) readBulk() ([]byte, error) {
	n, err := r.readInteger()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if n > maxBulkLen {
		return nil, fmt.Errorf("resp: bulk string of %d bytes exceeds limit", n)
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, errors.New("resp: invalid bulk string terminator")
	}
	return buf[:n:n], nil
}

func (r *Reader) readArray() ([]any, error) {
	n, err := r.readInteger()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	arr := make([]any, n)
	for i := range arr {
		if arr[i], err = r.ReadValue(); err != nil {
			return nil, err
		}
	}
	return arr, nil
}

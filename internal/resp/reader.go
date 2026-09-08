package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Limits on what one value may declare. Bulk matches proto-max-bulk-len's
// default. Elements and depth bound what a stream can make the node allocate
// or recurse into before a byte of payload arrives: a command is one flat
// array, and no reply nests past three.
const (
	maxBulkLen  = 512 << 20
	maxArrayLen = 1 << 20
	maxDepth    = 32
)

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
	return r.read(0)
}

func (r *Reader) read(depth int) (any, error) {
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
		a, err := r.readArray(depth)
		if a == nil || err != nil {
			return nil, err
		}
		return a, nil
	default:
		return nil, fmt.Errorf("%w %q", ErrInvalidType, typ)
	}
}

// Peek blocks until the next value's first byte is buffered, without
// consuming it, so a caller can wait for a command with no read deadline and
// then bound only the read that follows.
func (r *Reader) Peek() error {
	_, err := r.r.Peek(1)
	return err
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
	buf, err := r.readN(int(n) + 2)
	if err != nil {
		return nil, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, errors.New("resp: invalid bulk string terminator")
	}
	return buf[:n:n], nil
}

// readN reads exactly n bytes, growing the buffer as they arrive once past
// the first MiB, so a declared length costs the sender bytes before it costs
// the node memory.
func (r *Reader) readN(n int) ([]byte, error) {
	const chunk = 1 << 20
	if n <= chunk {
		buf := make([]byte, n)
		_, err := io.ReadFull(r.r, buf)
		return buf, err
	}
	var b bytes.Buffer
	b.Grow(chunk)
	if _, err := io.CopyN(&b, r.r, int64(n)); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (r *Reader) readArray(depth int) ([]any, error) {
	if depth >= maxDepth {
		return nil, fmt.Errorf("resp: array nested deeper than %d", maxDepth)
	}
	n, err := r.readInteger()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if n > maxArrayLen {
		return nil, fmt.Errorf("resp: array of %d elements exceeds limit", n)
	}
	arr := make([]any, 0, min(n, 64)) // grown as elements arrive: the length is the sender's claim
	for range n {
		v, err := r.read(depth + 1)
		if err != nil {
			return nil, err
		}
		arr = append(arr, v)
	}
	return arr, nil
}

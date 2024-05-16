package storage

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// RESP data types
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// Reader is a RESP reader
type Reader struct {
	r *bufio.Reader
}

// NewReader creates a new RESP reader
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadValue reads a RESP value
func (r *Reader) ReadValue() (interface{}, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case SimpleString:
		return r.readSimpleString()
	case Error:
		return nil, r.readError()
	case Integer:
		return r.readInteger()
	case BulkString:
		return r.readBulkString()
	case Array:
		return r.readArray()
	default:
		return nil, errors.New("invalid RESP data type")
	}
}

func (r *Reader) readSimpleString() (string, error) {
	return r.readLine()
}

func (r *Reader) readError() error {
	err, _ := r.readLine()
	return errors.New(err)
}

func (r *Reader) readInteger() (int64, error) {
	str, err := r.readLine()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(str, 10, 64)
}

func (r *Reader) readBulkString() ([]byte, error) {
	lenStr, err := r.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.ParseInt(lenStr, 10, 64)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	buf := make([]byte, length+2)
	_, err = io.ReadFull(r.r, buf)
	if err != nil {
		return nil, err
	}
	return buf[:length], nil
}

func (r *Reader) readArray() ([]interface{}, error) {
	lenStr, err := r.readLine()
	if err != nil {
		return nil, err
	}
	length, err := strconv.ParseInt(lenStr, 10, 64)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	array := make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		value, err := r.ReadValue()
		if err != nil {
			return nil, err
		}
		array[i] = value
	}
	return array, nil
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line[:len(line)-2], nil
}
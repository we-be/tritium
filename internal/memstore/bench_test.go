package memstore

import (
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/we-be/tritium/internal/resp"
)

func BenchmarkSet(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	args := []string{"SET", "bench:k", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "EX", "60"}
	var out []byte
	for b.Loop() {
		out = s.exec(out[:0], args)
	}
}

func BenchmarkStampedSet(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	args := []string{"STAMPED", "", "SET", "bench:k", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "EX", "60"}
	var out []byte
	n := uint64(0)
	for b.Loop() {
		n++
		args[1] = strconv.FormatUint(n, 10)
		out = s.exec(out[:0], args)
	}
}

func BenchmarkGet(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	s.exec(nil, []string{"SET", "bench:k", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "EX", "60"})
	args := []string{"GET", "bench:k"}
	var out []byte
	for b.Loop() {
		out = s.exec(out[:0], args)
	}
}

func BenchmarkZAdd(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	args := []string{"ZADD", "bench:z", "0", ""}
	var out []byte
	i := 0
	for b.Loop() {
		i++
		args[2], args[3] = strconv.Itoa(i), "m"+strconv.Itoa(i%1000)
		out = s.exec(out[:0], args)
	}
}

// One SCAN page of 1000 over 100k keys.
func BenchmarkScanPage(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	for i := range 100000 {
		s.exec(nil, []string{"SET", "scan:" + strconv.Itoa(i), "v", "EX", "600"})
	}
	cursor := "0"
	var out []byte
	for b.Loop() {
		out = s.exec(out[:0], []string{"SCAN", cursor, "COUNT", "1000"})
		v, _ := resp.NewReader(bytesReader(out)).ReadValue()
		cursor = string(v.([]any)[0].([]byte))
	}
}

// A SET round trip over the in-process pipe, and the same over loopback TCP.
func BenchmarkPipeRoundTrip(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	ln := Listen()
	defer ln.Close()
	go s.Serve(ln)
	c, err := ln.Dial()
	if err != nil {
		b.Fatal(err)
	}
	roundTrips(b, c)
}

func BenchmarkTCPRoundTrip(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	go s.Serve(ln)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	roundTrips(b, c)
}

func roundTrips(b *testing.B, c net.Conn) {
	b.Helper()
	cmd := resp.NewCommand("SET", "bench:k", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "EX", "60")
	r := resp.NewReader(c)
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := r.ReadOK(); err != nil {
			b.Fatal(err)
		}
	}
}

// Pipelined: 100 SETs per write, then 100 replies.
func BenchmarkPipePipelined100(b *testing.B) {
	s := New(Options{})
	defer s.Close()
	ln := Listen()
	defer ln.Close()
	go s.Serve(ln)
	c, err := ln.Dial()
	if err != nil {
		b.Fatal(err)
	}
	var buf []byte
	for i := range 100 {
		buf = append(buf, resp.NewCommand("SET", fmt.Sprintf("bench:%d", i), "0123456789abcdef0123456789abcdef", "EX", "60")...)
	}
	r := resp.NewReader(c)
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.Write(buf); err != nil {
			b.Fatal(err)
		}
		for range 100 {
			if err := r.ReadOK(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func bytesReader(b []byte) *sliceReader { return &sliceReader{b: b} }

type sliceReader struct{ b []byte }

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

package memstore

import (
	"io"
	"net"
	"sync"
	"time"
)

// Listener hands out connections that never leave the process: the node
// reaches the store it embeds the way it reaches an external one, over RESP,
// so the pool, the resync and every command work unchanged, with no port to
// bind and nothing for another process to connect to.
type Listener struct {
	accept chan net.Conn
	done   chan struct{}
	once   sync.Once
}

func Listen() *Listener {
	return &Listener{accept: make(chan net.Conn), done: make(chan struct{})}
}

// Dial opens a connection whose other end Accept returns.
func (l *Listener) Dial() (net.Conn, error) {
	client, server := pipe()
	select {
	case l.accept <- server:
		return client, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *Listener) Addr() net.Addr { return addr{} }

type addr struct{}

func (addr) Network() string { return "embedded" }
func (addr) String() string  { return "embedded" }

// pipe is a connected pair of in-memory conns. Unlike net.Pipe, a write
// never waits for the reader: the pool writes a whole pipelined batch before
// it reads a reply, and the store answers each command as it reads it, so a
// synchronous pipe would deadlock on the first batch larger than a buffer.
func pipe() (net.Conn, net.Conn) {
	a, b := newBuffer(), newBuffer()
	return &end{r: a, w: b}, &end{r: b, w: a}
}

type end struct {
	r, w *buffer
}

func (e *end) Read(p []byte) (int, error)  { return e.r.read(p) }
func (e *end) Write(p []byte) (int, error) { return e.w.write(p) }

func (e *end) Close() error {
	e.w.close()
	e.r.close()
	return nil
}

func (e *end) LocalAddr() net.Addr              { return addr{} }
func (e *end) RemoteAddr() net.Addr             { return addr{} }
func (e *end) SetDeadline(time.Time) error      { return nil } // in-process: nothing to time out
func (e *end) SetReadDeadline(time.Time) error  { return nil }
func (e *end) SetWriteDeadline(time.Time) error { return nil }

// buffer is one direction: bytes written wait here until read, without bound.
type buffer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	data   []byte
	off    int
	closed bool
}

func newBuffer() *buffer {
	b := &buffer{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *buffer) write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	b.data = append(b.data, p...)
	b.cond.Signal()
	return len(p), nil
}

func (b *buffer) read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.off == len(b.data) && !b.closed {
		b.cond.Wait()
	}
	if b.off == len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	if b.off == len(b.data) {
		b.data, b.off = b.data[:0], 0 // drained: reuse the array rather than grow it forever
	}
	return n, nil
}

func (b *buffer) close() {
	b.mu.Lock()
	b.closed = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

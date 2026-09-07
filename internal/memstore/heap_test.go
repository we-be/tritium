package memstore

import (
	"runtime"
	"strconv"
	"testing"
)

// TestHeapPerKey prints what a key really costs on the heap against what
// the store charges it, at two value sizes; run with -v.
func TestHeapPerKey(t *testing.T) {
	for _, size := range []int{64, 1024} {
		s := New(Options{})
		val := string(make([]byte, size))
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		const n = 200000
		for i := range n {
			s.exec(nil, []string{"SET", "mem:" + strconv.Itoa(i), val, "EX", "600"})
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		live := int64(after.HeapAlloc - before.HeapAlloc)
		t.Logf("size=%dB: live heap %d B/key, charged %d B/key (%.1fx)", size, live/n, s.used/n, float64(live)/float64(s.used))
		s.Close()
	}
}

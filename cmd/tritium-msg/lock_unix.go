//go:build unix

package main

import (
	"os"
	"path/filepath"
	"syscall"
)

// lockDir holds an exclusive lock on the state directory until the returned
// function is called, so two processes never rewrite state.json over each
// other: a ratchet step lost that way is a message the peer can never read.
func lockDir(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}

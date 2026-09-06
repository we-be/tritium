//go:build !unix

package main

func lockDir(dir string) (func(), error) { return func() {}, nil }

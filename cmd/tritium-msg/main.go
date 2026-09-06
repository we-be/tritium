// Command tritium-msg is a shell client for the messenger.
//
//	tritium-msg [flags] init NAME        create and publish an identity
//	tritium-msg [flags] me               show your name and fingerprint
//	tritium-msg [flags] lookup NAME      show a peer's fingerprint
//	tritium-msg [flags] send NAME TEXT   send a message
//	tritium-msg [flags] recv [-watch]    print new messages
//
// Identity and session state live in the -state directory, readable only by
// you. Guard it like a private key, because it is one.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/we-be/tritium/pkg/messenger"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	home, _ := os.UserHomeDir()
	addr := flag.String("addr", "localhost:8080", "node address")
	password := flag.String("password", "", "AUTH password")
	useTLS := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "PEM bundle to verify the node against (implies -tls)")
	dir := flag.String("state", filepath.Join(home, ".tritium-msg"), "directory holding identity and session state")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}

	opts := tritium.ClientOptions{Address: *addr, Timeout: 5 * time.Second, Password: *password}
	if *useTLS || *ca != "" {
		var err error
		if opts.TLS, err = tritium.TLSConfig(*ca); err != nil {
			fail(err)
		}
	}
	conn, err := tritium.NewClient(&opts)
	if err != nil {
		fail(err)
	}
	defer conn.Close()

	if err := run(conn, *dir, flag.Arg(0), flag.Args()[1:]); err != nil {
		fail(err)
	}
}

func run(conn *tritium.Client, dir, cmd string, args []string) error {
	idFile, stateFile := filepath.Join(dir, "identity.json"), filepath.Join(dir, "state.json")

	if cmd == "init" {
		if len(args) != 1 {
			return errors.New("usage: init NAME")
		}
		if _, err := os.Stat(idFile); err == nil {
			return fmt.Errorf("%s already exists; remove it to start over", idFile)
		}
		id, err := messenger.NewIdentity(args[0])
		if err != nil {
			return err
		}
		if err := messenger.New(conn, id).Publish(); err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := writeJSON(idFile, id); err != nil {
			return err
		}
		fmt.Printf("%s  %s\n", id.Name, id.Fingerprint())
		return nil
	}

	var id messenger.Identity
	if err := readJSON(idFile, &id); err != nil {
		return fmt.Errorf("no identity yet; run: tritium-msg init NAME (%w)", err)
	}
	client := messenger.New(conn, &id)
	if data, err := os.ReadFile(stateFile); err == nil {
		if err := client.Restore(data); err != nil {
			return fmt.Errorf("%s: %w", stateFile, err)
		}
	}
	save := func() error {
		st, err := client.State()
		if err != nil {
			return err
		}
		return os.WriteFile(stateFile, st, 0o600)
	}

	switch cmd {
	case "me":
		fmt.Printf("%s  %s\n", id.Name, id.Fingerprint())
	case "lookup":
		if len(args) != 1 {
			return errors.New("usage: lookup NAME")
		}
		b, err := client.Lookup(args[0])
		if err != nil {
			return err
		}
		fmt.Printf("%s  %s\n", b.Name, b.Fingerprint())
	case "send":
		if len(args) < 2 {
			return errors.New("usage: send NAME TEXT")
		}
		if err := client.Publish(); err != nil {
			return err
		}
		peer, err := client.Lookup(args[0])
		if err != nil {
			return err
		}
		if err := client.Send(peer, []byte(strings.Join(args[1:], " "))); err != nil {
			return err
		}
		return save()
	case "recv":
		fs := flag.NewFlagSet("recv", flag.ContinueOnError)
		watch := fs.Bool("watch", false, "keep polling until interrupted")
		every := fs.Duration("every", 2*time.Second, "poll interval with -watch")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if err := client.Publish(); err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		for {
			msgs, err := client.Receive()
			if err != nil {
				return err
			}
			for _, m := range msgs {
				fmt.Printf("[%s] %s (%s): %s\n", m.Time.Local().Format("15:04:05"), m.From.Name, m.From.Fingerprint()[:11], m.Body)
			}
			if err := save(); err != nil {
				return err
			}
			if !*watch {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(*every):
			}
		}
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

func writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tritium-msg [flags] init NAME | me | lookup NAME | send NAME TEXT | recv [-watch]")
	flag.PrintDefaults()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tritium-msg:", err)
	os.Exit(1)
}

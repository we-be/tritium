// Command tritium-msg is a shell client for the messenger.
//
//	tritium-msg [flags] init NAME        create and publish an identity
//	tritium-msg [flags] me               show your name and fingerprint
//	tritium-msg [flags] lookup NAME      show a peer's fingerprint
//	tritium-msg [flags] send NAME TEXT   send a message
//	tritium-msg [flags] recv [-watch]    print new messages
//	tritium-msg [flags] ask [-fp FP] NAME [TEXT]
//	                                     send from a throwaway identity, print the reply
//	tritium-msg [flags] serve [-name NAME]
//	                                     JSON lines: incoming on stdout, replies on stdin
//	tritium-msg [flags] device authorize DEVICE FINGERPRINT
//	                                     certify a device published as NAME/DEVICE onto NAME; the fingerprint is read on the device itself
//	tritium-msg [flags] device list      devices certified onto NAME
//	tritium-msg [flags] group create/add/remove/send/list NAME ...
//	                                     a roster this identity created (add/remove), or belongs to (send)
//
// Identity and session state live in the -state directory, readable only by
// you. Guard it like a private key, because it is one. One process at a time
// may use a state directory; the others wait on its lock. A device is a
// second identity of its own, run from its own -state directory and
// certified onto a name with `device authorize` run from the name's own
// state; init it the same way as any identity, with NAME/DEVICE as its name.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/we-be/tritium/internal/config"
	"github.com/we-be/tritium/pkg/messenger"
	"github.com/we-be/tritium/pkg/tritium"
)

func main() {
	home, _ := os.UserHomeDir()
	configPath := flag.String("config", "", "a node's dotenv file: fills -addr, -password and -ca from it (explicit flags win)")
	addr := flag.String("addr", "localhost:8080", "node address")
	password := flag.String("password", "", "AUTH password")
	useTLS := flag.Bool("tls", false, "connect with TLS")
	ca := flag.String("ca", "", "PEM bundle to verify the node against (implies -tls)")
	dir := flag.String("state", filepath.Join(home, ".tritium-msg"), "directory holding identity and session state")
	flag.Usage = usage
	flag.Parse()
	if *configPath != "" {
		loc, err := config.LoadLocal(*configPath)
		if err != nil {
			fail(err)
		}
		set := map[string]bool{}
		flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
		if !set["addr"] {
			*addr = loc.Addr
		}
		if !set["password"] {
			*password = loc.Password
		}
		if !set["ca"] && loc.CA != "" {
			*ca = loc.CA
		}
	}
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

	if cmd == "ask" {
		return ask(conn, args)
	}
	if cmd != "init" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		unlock, err := lockDir(dir)
		if err != nil {
			return err
		}
		defer unlock()
	}
	var serveFlags *serveOptions
	if cmd == "serve" {
		var err error
		if serveFlags, err = parseServe(args); err != nil {
			return err
		}
		if _, err := os.Stat(idFile); err != nil && serveFlags.name != "" {
			cmd, args = "init", []string{serveFlags.name} // first run: become NAME, then serve
			defer func() { serveFlags = nil }()
		}
	}

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
		out := os.Stdout
		if serveFlags != nil {
			out = os.Stderr // serve's stdout is the message stream
		}
		fmt.Fprintf(out, "%s  %s\n", id.Name, id.Fingerprint())
		if serveFlags == nil {
			return nil
		}
		cmd = "serve" // created for serve: carry on
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
	publish := func() error { // rotation may have changed the identity
		if err := client.Publish(); err != nil {
			return err
		}
		return writeJSON(idFile, &id)
	}

	switch cmd {
	case "serve":
		return serve(client, &id, idFile, save, serveFlags)
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
			return errors.New("usage: send NAME TEXT | send NAME -file PATH | send NAME -   (stdin)")
		}
		body, err := messageBody(args[1:])
		if err != nil {
			return err
		}
		if err := publish(); err != nil {
			return err
		}
		// fans out to every device certified under NAME, not just its primary identity
		if err := client.SendAll(args[0], body); err != nil {
			return err
		}
		return save()
	case "device":
		if err := publish(); err != nil {
			return err
		}
		return device(client, &id, args)
	case "group":
		if err := publish(); err != nil {
			return err
		}
		return group(client, args)
	case "recv":
		fs := flag.NewFlagSet("recv", flag.ContinueOnError)
		watch := fs.Bool("watch", false, "keep polling until interrupted")
		every := fs.Duration("every", 2*time.Second, "poll interval with -watch")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if err := publish(); err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		for {
			msgs, err := client.Receive()
			if err != nil {
				return err
			}
			if err := save(); err != nil { // before printing: a message is deleted only once its receipt is on disk
				return err
			}
			for _, m := range msgs {
				via := ""
				if m.Group != "" {
					via = " #" + m.Group
				}
				fmt.Printf("[%s] %s (%s)%s: %s\n", m.Time.Local().Format("15:04:05"), m.From.Name, m.From.Fingerprint()[:11], via, m.Body)
			}
			if len(msgs) > 0 {
				continue // another batch may be waiting, and this one is deleted by the next call
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

// messageBody is what `send` carries: the words on the command line, a file
// (-file PATH), or standard input (-). A secret handed to a new machine
// should come from a file or a pipe, never from the command line, where
// every process on the sender's box can read it.
func messageBody(args []string) ([]byte, error) {
	switch {
	case len(args) == 2 && args[0] == "-file":
		return os.ReadFile(args[1])
	case len(args) == 1 && args[0] == "-":
		return io.ReadAll(os.Stdin)
	}
	return []byte(strings.Join(args, " ")), nil
}

// device authorizes a device published under id.Name+"/"+DEVICE, or lists
// the devices already authorized.
func device(client *messenger.Client, id *messenger.Identity, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: device authorize DEVICE FINGERPRINT | device list")
	}
	switch args[0] {
	case "authorize":
		if len(args) != 3 {
			return errors.New("usage: device authorize DEVICE FINGERPRINT   (the fingerprint from `me` on the device)")
		}
		bundle, err := client.Lookup(id.Name + "/" + args[1])
		if err != nil {
			return err
		}
		if bundle.Fingerprint() != args[2] { // the device key is first come like any name: never certify whatever squats there
			return fmt.Errorf("%s/%s is published by %s, not the fingerprint given", id.Name, args[1], bundle.Fingerprint())
		}
		if _, err := client.AuthorizeDevice(args[1], bundle); err != nil {
			return err
		}
		fmt.Printf("authorized %s/%s  %s\n", id.Name, args[1], bundle.Fingerprint())
		return nil
	case "list":
		devices, err := client.Devices(id.Name)
		if err != nil {
			return err
		}
		for _, d := range devices {
			fmt.Printf("%s  %s\n", d.Name, d.Fingerprint())
		}
		return nil
	default:
		return fmt.Errorf("unknown device command %q", args[0])
	}
}

// group creates, edits, sends to, or lists a group's roster.
func group(client *messenger.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: group create/add/remove/send/list NAME ...")
	}
	verb, args := args[0], args[1:]
	if len(args) == 0 {
		return errors.New("usage: group " + verb + " NAME ...")
	}
	name := args[0]
	switch verb {
	case "create":
		g, err := client.CreateGroup(name, args[1:])
		if err != nil {
			return err
		}
		fmt.Printf("created #%s with %d member(s)\n", g.Name, len(g.Members))
		return nil
	case "add", "remove":
		if len(args) != 2 {
			return errors.New("usage: group " + verb + " NAME MEMBER")
		}
		var g messenger.Group
		var err error
		if verb == "add" {
			g, err = client.AddMember(name, args[1])
		} else {
			g, err = client.RemoveMember(name, args[1])
		}
		if err != nil {
			return err
		}
		fmt.Printf("#%s now has %d member(s)\n", g.Name, len(g.Members))
		return nil
	case "send":
		if len(args) < 2 {
			return errors.New("usage: group send NAME TEXT")
		}
		return client.SendGroup(name, []byte(strings.Join(args[1:], " ")))
	case "list":
		g, err := client.LookupGroup(name)
		if err != nil {
			return err
		}
		fmt.Printf("#%s  creator=%s  version=%d\n", g.Name, g.Creator, g.Version)
		for _, m := range g.Members {
			fmt.Println(" ", m)
		}
		return nil
	default:
		return fmt.Errorf("unknown group command %q", verb)
	}
}

// ask sends from a throwaway identity and prints the reply. Exit 2 on no
// reply, 3 when the peer's fingerprint is not the pinned one.
func ask(conn *tritium.Client, args []string) error {
	fs := flag.NewFlagSet("ask", flag.ContinueOnError)
	fp := fs.String("fp", "", "refuse unless the peer's fingerprint is exactly this")
	timeout := fs.Duration("timeout", 8*time.Second, "how long to wait for the reply")
	ttl := fs.Int("ttl", 60, "seconds the request and its reply live on the server")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("usage: ask [-fp FP] [-timeout D] [-ttl S] NAME [TEXT]  (TEXT from stdin when omitted)")
	}
	var body []byte
	if fs.NArg() > 1 {
		body = []byte(strings.Join(fs.Args()[1:], " "))
	} else {
		var err error
		if body, err = io.ReadAll(os.Stdin); err != nil {
			return err
		}
	}
	reply, err := messenger.Ask(conn, fs.Arg(0), *fp, body, *timeout, *ttl)
	switch {
	case errors.Is(err, messenger.ErrTimeout):
		fmt.Fprintln(os.Stderr, "tritium-msg:", err)
		os.Exit(2)
	case errors.Is(err, messenger.ErrFingerprint):
		fmt.Fprintln(os.Stderr, "tritium-msg:", err)
		os.Exit(3)
	case err != nil:
		return err
	}
	os.Stdout.Write(reply)
	return nil
}

type serveOptions struct {
	name  string
	every time.Duration
	ttl   int
	prune time.Duration
}

func parseServe(args []string) (*serveOptions, error) {
	o := &serveOptions{}
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.StringVar(&o.name, "name", "", "create and publish the identity as NAME when there is none yet")
	fs.DurationVar(&o.every, "every", 500*time.Millisecond, "poll interval")
	fs.IntVar(&o.ttl, "ttl", 60, "seconds a reply lives on the server")
	fs.DurationVar(&o.prune, "prune", time.Hour, "forget sessions idle this long (throwaway askers)")
	return o, fs.Parse(args)
}

// inbound is one received message as serve prints it: body as text, or b64
// when it is not valid UTF-8.
type inbound struct {
	From  string `json:"from"`
	FP    string `json:"fp"`
	Time  string `json:"time"`
	Body  string `json:"body,omitempty"`
	B64   []byte `json:"b64,omitempty"`
	Group string `json:"group,omitempty"`
}

// outbound is one reply as serve reads it from stdin.
type outbound struct {
	FP   string `json:"fp"`
	Body string `json:"body"`
	B64  []byte `json:"b64"`
}

// serve is the responder half of ask: it prints every message as one JSON
// line and sends back whatever JSON lines arrive on stdin. It ends when stdin
// closes (the parent went away) or on SIGTERM. The identity is republished
// hourly, which also rotates the prekey, and idle sessions are pruned.
func serve(client *messenger.Client, id *messenger.Identity, idFile string, save func() error, o *serveOptions) error {
	client.TTL = o.ttl
	var mu sync.Mutex           // the client is not goroutine-safe: one lock for it and for state.json
	republish := func() error { // Publish may rotate the prekey, so the identity is stored again
		if err := client.Publish(); err != nil {
			return err
		}
		return writeJSON(idFile, id)
	}
	if err := republish(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "serving as %s  %s\n", id.Name, id.Fingerprint())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stdinDone := make(chan error, 1)
	go func() {
		dec := json.NewDecoder(os.Stdin)
		for {
			var out outbound
			if err := dec.Decode(&out); err != nil {
				stdinDone <- err
				return
			}
			body := []byte(out.Body)
			if len(out.B64) > 0 {
				body = out.B64
			}
			mu.Lock()
			if err := client.Reply(out.FP, body); err != nil {
				fmt.Fprintln(os.Stderr, "tritium-msg: reply:", err)
			} else if err := save(); err != nil {
				fmt.Fprintln(os.Stderr, "tritium-msg: save:", err)
			}
			mu.Unlock()
		}
	}()

	enc := json.NewEncoder(os.Stdout)
	lastHousekeeping := time.Now()
	for {
		mu.Lock()
		msgs, err := client.Receive()
		if err == nil {
			err = save()
		}
		if time.Since(lastHousekeeping) > time.Hour {
			lastHousekeeping = time.Now()
			client.Prune(o.prune)
			if perr := republish(); perr != nil {
				fmt.Fprintln(os.Stderr, "tritium-msg: republish:", perr)
			}
		}
		mu.Unlock()
		if err != nil {
			fmt.Fprintln(os.Stderr, "tritium-msg: receive:", err)
		}
		for _, m := range msgs {
			in := inbound{From: m.From.Name, FP: m.From.Fingerprint(), Time: m.Time.UTC().Format(time.RFC3339Nano), Group: m.Group}
			if utf8.Valid(m.Body) {
				in.Body = string(m.Body)
			} else {
				in.B64 = m.Body
			}
			if err := enc.Encode(in); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-stdinDone:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("stdin: %w", err)
		case <-time.After(o.every):
		}
	}
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
	fmt.Fprintln(os.Stderr, "usage: tritium-msg [flags] init NAME | me | lookup NAME | send NAME TEXT | recv [-watch] | ask [-fp FP] NAME [TEXT] | serve [-name NAME] | device authorize DEVICE FINGERPRINT | device list | group create/add/remove/send/list NAME ...")
	flag.PrintDefaults()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tritium-msg:", err)
	os.Exit(1)
}

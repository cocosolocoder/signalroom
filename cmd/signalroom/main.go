package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cocosolocoder/signalroom/internal/api"
	"github.com/cocosolocoder/signalroom/internal/events"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "demo":
		if len(os.Args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: signalroom demo")
			os.Exit(2)
		}
		if err := runDemo(); err != nil {
			fmt.Fprintf(os.Stderr, "signalroom: %v\n", err)
			os.Exit(1)
		}
	case "serve":
		if err := runServe(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "signalroom: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: signalroom <command> [flags]

commands:
  demo   run the in-memory event timeline demo
  serve  start the HTTP event ingestion server

serve flags:
  --addr <addr>  listen address, e.g. :8080 or 127.0.0.1:0
  --data <dir>   data directory for persistent event storage`)
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", "", "listen address")
	data := fs.String("data", "", "data directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *addr == "" || *data == "" {
		fs.Usage()
		return errors.New("both --addr and --data are required")
	}

	store, err := events.OpenStore(*data)
	if err != nil {
		return err
	}
	defer store.Close()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	srv := &http.Server{
		Handler:           api.NewServer(store).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	fmt.Printf("signalroom listening on %s\n", ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		fmt.Println("signalroom shutting down...")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}

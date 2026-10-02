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

	"github.com/cocosolocoder/signalroom/internal/events"
	"github.com/cocosolocoder/signalroom/internal/httpapi"
	"github.com/cocosolocoder/signalroom/internal/incidents"
	"github.com/cocosolocoder/signalroom/internal/logfile"
	"github.com/cocosolocoder/signalroom/internal/metrics"
)

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "listen address")
	dataDir := fs.String("data", "", "directory for durable event data")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataDir == "" || fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: signalroom serve --addr <address> --data <directory>")
		os.Exit(2)
	}

	log, batches, err := logfile.Open(*dataDir)
	if err != nil {
		return err
	}
	defer log.Close()

	timeline := events.NewTimeline()
	for _, batch := range batches {
		for _, event := range batch {
			if err := timeline.Load(event); err != nil {
				return fmt.Errorf("recover events: %w", err)
			}
		}
	}
	timeline.SortAll()

	// Incidents are replayed after events so linked event ids resolve
	// against the already-recovered timeline.
	incidentLog, incidentRecords, err := logfile.OpenIncidentLog(*dataDir)
	if err != nil {
		return err
	}
	defer incidentLog.Close()

	registry := incidents.NewRegistry(timeline, incidentLog.AppendRecord)
	for _, record := range incidentRecords {
		if err := registry.Load(record); err != nil {
			return fmt.Errorf("recover incidents: %w", err)
		}
	}

	// Metrics are replayed after incidents; they are independent of both.
	metricLog, metricSamples, err := logfile.OpenMetricLog(*dataDir)
	if err != nil {
		return err
	}
	defer metricLog.Close()

	metricTimeline := metrics.NewTimeline()
	for _, sample := range metricSamples {
		if err := metricTimeline.Load(sample); err != nil {
			return fmt.Errorf("recover metrics: %w", err)
		}
	}
	metricTimeline.SortAll()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	server := &http.Server{
		Handler: httpapi.NewHandler(timeline, log, log,
			httpapi.WithIncidents(registry, incidentLog),
			httpapi.WithMetrics(metricTimeline, metricLog)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Serve(listener)
	}()

	// Published exactly once the socket accepts connections; clients (and
	// tests) can wait on this line instead of polling with fixed sleeps.
	fmt.Printf("signalroom: listening on %s\n", listener.Addr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-stop:
		fmt.Fprintf(os.Stderr, "signalroom: received %s, shutting down\n", sig)
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// tinyroute serves restriction-aware routes from an OSM XML snapshot or a
// normalized OSM table in a tinySQL snapshot. It never caches route answers.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/routing"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	osm := flag.String("osm", "", "OSM XML snapshot")
	dbPath := flag.String("db", "", "tinySQL snapshot containing an imported OSM table")
	table := flag.String("table", "osm", "normalized OSM table name")
	tenant := flag.String("tenant", "default", "tinySQL tenant")
	names := flag.String("profiles", "car,bicycle,foot", "comma-separated profiles")
	addr := flag.String("addr", "127.0.0.1:8081", "listen address")
	maxStates := flag.Int("max-states", 1000000, "maximum search states per request")
	flag.Parse()
	if (*osm == "") == (*dbPath == "") {
		return fmt.Errorf("provide exactly one of -osm or -db")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var data routing.Data
	var db *tinysql.DB
	if *osm != "" {
		f, err := os.Open(*osm)
		if err != nil {
			return err
		}
		data, err = routing.ReadOSM(ctx, f)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	} else {
		var err error
		db, err = tinysql.LoadFromFile(*dbPath)
		if err != nil {
			return err
		}
	}
	routers := make(map[routing.Profile]*routing.Router)
	for _, name := range strings.Split(*names, ",") {
		p := routing.Profile(strings.TrimSpace(name))
		var r *routing.Router
		var err error
		opts := routing.Options{MaxStates: *maxStates}
		if db != nil {
			r, err = routing.FromDB(ctx, db, *tenant, *table, p, opts)
		} else {
			r, err = routing.Build(ctx, data, p, opts)
		}
		if err != nil {
			return fmt.Errorf("profile %s: %w", p, err)
		}
		routers[p] = r
		log.Printf("ready: %+v", r.Stats())
	}
	server := &http.Server{Addr: *addr, Handler: routing.Handler(routers), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("routing API on %s (no answer cache)", *addr)
	err := server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Command tinysqld is the enterprise DBMS daemon entry point.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func main() {
	var (
		dataPath    = flag.String("data", "", "Durable database path or directory")
		storageMode = flag.String("storage", "disk", "Storage mode: disk, hybrid, index, wal, advanced_wal")
		tenant      = flag.String("tenant", "default", "Default tenant")
		httpAddr    = flag.String("http", "127.0.0.1:8088", "HTTP listen address; empty disables HTTP")
		authToken   = flag.String("auth", "", "Optional bearer token for API endpoints")
		reqTimeout  = flag.Duration("request-timeout", 30*time.Second, "Maximum SQL request duration")
		readTimeout = flag.Duration("http-read-timeout", 10*time.Second, "HTTP read timeout")
		writeTO     = flag.Duration("http-write-timeout", 30*time.Second, "HTTP write timeout")
		shutdownTO  = flag.Duration("shutdown-timeout", 10*time.Second, "Graceful shutdown timeout")
		check       = flag.Bool("check", false, "Open the DBMS runtime, print status, then exit")
	)
	flag.Parse()

	mode, err := tinysql.ParseStorageMode(*storageMode)
	if err != nil {
		log.Fatal(err)
	}

	inst, err := tinysql.OpenEnterprise(tinysql.StorageConfig{
		Mode: mode,
		Path: *dataPath,
	}, *tenant)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := inst.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	fmt.Printf("tinySQL DBMS initialized: mode=%s storage=%s tenant=%s\n", inst.Mode, inst.DB.StorageMode(), inst.Tenant)
	fmt.Println("job scheduler: enabled")

	if *check {
		return
	}

	daemon := newDaemon(inst, daemonConfig{
		DefaultTenant:  *tenant,
		AuthToken:      *authToken,
		RequestTimeout: *reqTimeout,
	})
	var httpSrv *http.Server
	if *httpAddr != "" {
		httpSrv = &http.Server{
			Addr:         *httpAddr,
			Handler:      daemon.routes(),
			ReadTimeout:  *readTimeout,
			WriteTimeout: *writeTO,
		}
		go func() {
			log.Printf("tinysqld HTTP listening on %s", *httpAddr)
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("http server: %v", err)
			}
		}()
	} else {
		fmt.Println("HTTP listener disabled")
	}

	fmt.Println("press Ctrl+C to stop")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	fmt.Println("shutdown requested")

	daemon.ready.Store(false)
	if httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), *shutdownTO)
		defer cancel()
		if err := httpSrv.Shutdown(ctx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}
}

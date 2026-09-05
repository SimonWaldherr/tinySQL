//go:build !js && !wasm && !baremetal

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

func commandValidate(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: tinytiles validate dataset.ttiles/")
		return 2
	}
	start := time.Now()
	manifest, err := tiles.ValidateArtifact(context.Background(), args[0])
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles validate: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "valid schema=%s tables=%d elapsed=%s\n", manifest.Schema, len(manifest.Tables), time.Since(start).Round(time.Millisecond))
	return 0
}

func commandInspect(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: tinytiles inspect dataset.ttiles/")
		return 2
	}
	manifest, err := tiles.ValidateArtifact(context.Background(), args[0])
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles inspect: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		fmt.Fprintf(stderr, "tinytiles inspect: %v\n", err)
		return 1
	}
	return 0
}

func commandTile(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("tile", flag.ContinueOnError)
	fs.SetOutput(stderr)
	memory := fs.Int64("max-memory", 64<<20, "maximum reader cache budget in bytes")
	out := fs.String("out", "", "write tile bytes to this file instead of stdout")
	if fs.Parse(args) != nil || fs.NArg() != 4 {
		fmt.Fprintln(stderr, "usage: tinytiles tile [-out file] dataset.ttiles/ z x y")
		return 2
	}
	z, err := strconv.Atoi(fs.Arg(1))
	if err == nil {
		_, err = strconv.Atoi(fs.Arg(2))
	}
	if err == nil {
		_, err = strconv.Atoi(fs.Arg(3))
	}
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles tile: z, x and y must be integers: %v\n", err)
		return 2
	}
	x, _ := strconv.Atoi(fs.Arg(2))
	y, _ := strconv.Atoi(fs.Arg(3))
	reader, err := tiles.OpenArtifact(context.Background(), fs.Arg(0), tiles.OpenOptions{MaxMemoryBytes: *memory})
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles tile: %v\n", err)
		return 1
	}
	defer reader.Close()
	tile, found, err := reader.Lookup(context.Background(), tiles.Key{Z: z, X: x, Y: y})
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles tile: %v\n", err)
		return 1
	}
	if !found {
		fmt.Fprintf(stderr, "tinytiles tile: not found: %d/%d/%d\n", z, x, y)
		return 3
	}
	if *out == "" {
		_, err = stdout.Write(tile.Data)
	} else {
		err = os.WriteFile(*out, tile.Data, 0o644)
	}
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles tile: write: %v\n", err)
		return 1
	}
	return 0
}

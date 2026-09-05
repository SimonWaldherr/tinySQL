//go:build !js && !wasm && !baremetal

// tinytiles is the command-line entry point for the standalone tinyTiles
// project. Its read commands use only tinySQL's public tiles API, so they can
// run without SQLite after an artifact has been published.
package main

import (
	"fmt"
	"io"
	"os"
)

var version = "0.1.0-dev"

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printHelp(stdout)
		return 0
	}
	switch args[0] {
	case "help", "-h", "--help":
		printHelp(stdout)
		return 0
	case "version", "--version":
		fmt.Fprintln(stdout, version)
		return 0
	case "import":
		return commandImport(args[1:], stdout, stderr)
	case "build":
		return commandBuild(args[1:], stdout, stderr)
	case "validate":
		return commandValidate(args[1:], stdout, stderr)
	case "inspect":
		return commandInspect(args[1:], stdout, stderr)
	case "tile":
		return commandTile(args[1:], stdout, stderr)
	case "benchmark":
		return commandBenchmark(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "tinytiles: unknown command %q\n", args[0])
		return 2
	}
}

func printHelp(w io.Writer) {
	fmt.Fprint(w, `tinytiles — read-optimized tile artifacts

Usage:
  tinytiles build [flags] source.osm.pbf dataset.ttiles/
  tinytiles import [flags] source.mbtiles dataset.ttiles/
  tinytiles validate dataset.ttiles/
  tinytiles inspect dataset.ttiles/
  tinytiles tile [flags] dataset.ttiles/ z x y
  tinytiles benchmark [flags] --source source.mbtiles --artifact dataset.ttiles/
  tinytiles version

Commands:
  build      run a configured PBF→MBTiles generator, then import and publish
  import     build and atomically publish a bounded read artifact
  validate   run the complete checksum, table, index and tile-digest audit
  inspect    print the published semantic artifact information as JSON
  tile       read one TMS tile; use -out to write binary data to a file
  benchmark  compare warm TMS point-lookups with SQLite

build, import and benchmark require the sqliteimport build tag. validate,
inspect and tile use only the SQLite-free artifact reader on native targets.
tinyTiles is a separate read artifact format. It does not claim to be a
standards-compliant MBTiles/SQLite replacement.
`)
}

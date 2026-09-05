//go:build !sqliteimport && !js && !wasm && !baremetal

package main

import (
	"fmt"
	"io"
)

func commandImport(_ []string, _ io.Writer, stderr io.Writer) int {
	fmt.Fprintln(stderr, "tinytiles import requires the sqliteimport build tag: go run -tags=sqliteimport ./cmd/tinytiles …")
	return 2
}

func commandBuild(_ []string, _ io.Writer, stderr io.Writer) int {
	fmt.Fprintln(stderr, "tinytiles build requires the sqliteimport build tag: go run -tags=sqliteimport ./cmd/tinytiles …")
	return 2
}

func commandBenchmark(_ []string, _ io.Writer, stderr io.Writer) int {
	fmt.Fprintln(stderr, "tinytiles benchmark requires the sqliteimport build tag: go run -tags=sqliteimport ./cmd/tinytiles …")
	return 2
}

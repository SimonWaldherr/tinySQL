package main

import (
	"fmt"
	"github.com/SimonWaldherr/tinySQL/cmd/wasm_browser/internal/referencegen"
	"os"
)

func main() {
	cwd, err := os.Getwd()
	if err == nil {
		var root string
		root, err = referencegen.FindRepoRoot(cwd)
		if err == nil {
			err = referencegen.WriteFiles(root)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Updated HTML and browser JSON function references.")
}

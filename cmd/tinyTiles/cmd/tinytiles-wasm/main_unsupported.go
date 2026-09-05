//go:build !js || !wasm

package main

import "fmt"

func main() {
	fmt.Println("tinytiles-wasm requires GOOS=js GOARCH=wasm")
}

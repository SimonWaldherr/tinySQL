//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !js && !wasm && !baremetal

package importer

func availableDiskBytes(string) int64 { return -1 }

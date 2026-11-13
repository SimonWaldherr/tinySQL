//go:build !arm64 && !amd64

package search

func vectorL2PrefixNativeKernels() []vectorL2PrefixKernel { return nil }

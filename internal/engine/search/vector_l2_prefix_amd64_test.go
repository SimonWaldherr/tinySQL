package search

func vectorL2PrefixNativeKernels() []vectorL2PrefixKernel {
	kernels := []vectorL2PrefixKernel{{"sse2", vectorL2SquaredSSE2}}
	if vectorUseAVX2 {
		kernels = append(kernels, vectorL2PrefixKernel{"avx2-fma", vectorL2SquaredAVX2})
	}
	return kernels
}

package search

func vectorL2PrefixNativeKernels() []vectorL2PrefixKernel {
	if vectorUseNEON {
		return []vectorL2PrefixKernel{{"neon", vectorL2SquaredNEON}}
	}
	return nil
}

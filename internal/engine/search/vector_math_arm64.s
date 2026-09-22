#include "textflag.h"

// The reduction kernels below share one layout. The main loop consumes 16
// float64 lanes per iteration with eight independent 2-lane accumulators
// (V16-V23), loading four registers per VLD1: with four accumulators the
// loop was bound by FMA latency rather than by load or FMA throughput. One
// optional 8-lane step handles the next remainder, the accumulators are then
// summed pairwise in vector registers (the previous kernels moved every lane
// through a general-purpose register and added them one by one), and a
// scalar loop finishes the last < 8 elements. The summation order is fixed,
// so results are deterministic; they differ from the portable loops only by
// floating-point reassociation, within the tolerance the tests check.

// vectorDotNEON computes the dot product of two equally sized float64 slices.
TEXT ·vectorDotNEON(SB), NOSPLIT, $0-56
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	VEOR V16.B16, V16.B16, V16.B16
	VEOR V17.B16, V17.B16, V17.B16
	VEOR V18.B16, V18.B16, V18.B16
	VEOR V19.B16, V19.B16, V19.B16
	VEOR V20.B16, V20.B16, V20.B16
	VEOR V21.B16, V21.B16, V21.B16
	VEOR V22.B16, V22.B16, V22.B16
	VEOR V23.B16, V23.B16, V23.B16
	CMP $16, R1
	BLT vectorDotNEON_step8

vectorDotNEON_loop16:
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VLD1.P 64(R0), [V8.D2, V9.D2, V10.D2, V11.D2]
	VLD1.P 64(R2), [V12.D2, V13.D2, V14.D2, V15.D2]
	VFMLA V0.D2, V4.D2, V16.D2
	VFMLA V1.D2, V5.D2, V17.D2
	VFMLA V2.D2, V6.D2, V18.D2
	VFMLA V3.D2, V7.D2, V19.D2
	VFMLA V8.D2, V12.D2, V20.D2
	VFMLA V9.D2, V13.D2, V21.D2
	VFMLA V10.D2, V14.D2, V22.D2
	VFMLA V11.D2, V15.D2, V23.D2
	SUB $16, R1
	CMP $16, R1
	BGE vectorDotNEON_loop16

vectorDotNEON_step8:
	CMP $8, R1
	BLT vectorDotNEON_reduce
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VFMLA V0.D2, V4.D2, V16.D2
	VFMLA V1.D2, V5.D2, V17.D2
	VFMLA V2.D2, V6.D2, V18.D2
	VFMLA V3.D2, V7.D2, V19.D2
	SUB $8, R1

vectorDotNEON_reduce:
	WORD $0x4e74d610 // fadd v16.2d, v16.2d, v20.2d
	WORD $0x4e75d631 // fadd v17.2d, v17.2d, v21.2d
	WORD $0x4e76d652 // fadd v18.2d, v18.2d, v22.2d
	WORD $0x4e77d673 // fadd v19.2d, v19.2d, v23.2d
	WORD $0x4e72d610 // fadd v16.2d, v16.2d, v18.2d
	WORD $0x4e73d631 // fadd v17.2d, v17.2d, v19.2d
	WORD $0x4e71d610 // fadd v16.2d, v16.2d, v17.2d
	WORD $0x7e70da00 // faddp d0, v16.2d

vectorDotNEON_tail:
	CBZ R1, vectorDotNEON_done
	FMOVD.P 8(R0), F1
	FMOVD.P 8(R2), F2
	FMADDD F2, F0, F1, F0
	SUB $1, R1
	B vectorDotNEON_tail

vectorDotNEON_done:
	FMOVD F0, ret+48(FP)
	RET

// vectorL2SquaredNEON computes the squared Euclidean distance of two equally
// sized float64 slices.
TEXT ·vectorL2SquaredNEON(SB), NOSPLIT, $0-56
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	VEOR V16.B16, V16.B16, V16.B16
	VEOR V17.B16, V17.B16, V17.B16
	VEOR V18.B16, V18.B16, V18.B16
	VEOR V19.B16, V19.B16, V19.B16
	VEOR V20.B16, V20.B16, V20.B16
	VEOR V21.B16, V21.B16, V21.B16
	VEOR V22.B16, V22.B16, V22.B16
	VEOR V23.B16, V23.B16, V23.B16
	CMP $16, R1
	BLT vectorL2SquaredNEON_step8

vectorL2SquaredNEON_loop16:
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VLD1.P 64(R0), [V8.D2, V9.D2, V10.D2, V11.D2]
	VLD1.P 64(R2), [V12.D2, V13.D2, V14.D2, V15.D2]
	WORD $0x4ee4d400 // fsub v0.2d, v0.2d, v4.2d
	VFMLA V0.D2, V0.D2, V16.D2
	WORD $0x4ee5d421 // fsub v1.2d, v1.2d, v5.2d
	VFMLA V1.D2, V1.D2, V17.D2
	WORD $0x4ee6d442 // fsub v2.2d, v2.2d, v6.2d
	VFMLA V2.D2, V2.D2, V18.D2
	WORD $0x4ee7d463 // fsub v3.2d, v3.2d, v7.2d
	VFMLA V3.D2, V3.D2, V19.D2
	WORD $0x4eecd508 // fsub v8.2d, v8.2d, v12.2d
	VFMLA V8.D2, V8.D2, V20.D2
	WORD $0x4eedd529 // fsub v9.2d, v9.2d, v13.2d
	VFMLA V9.D2, V9.D2, V21.D2
	WORD $0x4eeed54a // fsub v10.2d, v10.2d, v14.2d
	VFMLA V10.D2, V10.D2, V22.D2
	WORD $0x4eefd56b // fsub v11.2d, v11.2d, v15.2d
	VFMLA V11.D2, V11.D2, V23.D2
	SUB $16, R1
	CMP $16, R1
	BGE vectorL2SquaredNEON_loop16

vectorL2SquaredNEON_step8:
	CMP $8, R1
	BLT vectorL2SquaredNEON_reduce
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	WORD $0x4ee4d400 // fsub v0.2d, v0.2d, v4.2d
	VFMLA V0.D2, V0.D2, V16.D2
	WORD $0x4ee5d421 // fsub v1.2d, v1.2d, v5.2d
	VFMLA V1.D2, V1.D2, V17.D2
	WORD $0x4ee6d442 // fsub v2.2d, v2.2d, v6.2d
	VFMLA V2.D2, V2.D2, V18.D2
	WORD $0x4ee7d463 // fsub v3.2d, v3.2d, v7.2d
	VFMLA V3.D2, V3.D2, V19.D2
	SUB $8, R1

vectorL2SquaredNEON_reduce:
	WORD $0x4e74d610 // fadd v16.2d, v16.2d, v20.2d
	WORD $0x4e75d631 // fadd v17.2d, v17.2d, v21.2d
	WORD $0x4e76d652 // fadd v18.2d, v18.2d, v22.2d
	WORD $0x4e77d673 // fadd v19.2d, v19.2d, v23.2d
	WORD $0x4e72d610 // fadd v16.2d, v16.2d, v18.2d
	WORD $0x4e73d631 // fadd v17.2d, v17.2d, v19.2d
	WORD $0x4e71d610 // fadd v16.2d, v16.2d, v17.2d
	WORD $0x7e70da00 // faddp d0, v16.2d

vectorL2SquaredNEON_tail:
	CBZ R1, vectorL2SquaredNEON_done
	FMOVD.P 8(R0), F1
	FMOVD.P 8(R2), F2
	FSUBD F2, F1, F1
	FMADDD F1, F0, F1, F0
	SUB $1, R1
	B vectorL2SquaredNEON_tail

vectorL2SquaredNEON_done:
	FMOVD F0, ret+48(FP)
	RET

// vectorL1NEON computes the Manhattan distance of two equally sized vectors.
TEXT ·vectorL1NEON(SB), NOSPLIT, $0-56
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	VEOR V16.B16, V16.B16, V16.B16
	VEOR V17.B16, V17.B16, V17.B16
	VEOR V18.B16, V18.B16, V18.B16
	VEOR V19.B16, V19.B16, V19.B16
	VEOR V20.B16, V20.B16, V20.B16
	VEOR V21.B16, V21.B16, V21.B16
	VEOR V22.B16, V22.B16, V22.B16
	VEOR V23.B16, V23.B16, V23.B16
	CMP $16, R1
	BLT vectorL1NEON_step8

vectorL1NEON_loop16:
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VLD1.P 64(R0), [V8.D2, V9.D2, V10.D2, V11.D2]
	VLD1.P 64(R2), [V12.D2, V13.D2, V14.D2, V15.D2]
	WORD $0x4ee4d400 // fsub v0.2d, v0.2d, v4.2d
	WORD $0x4ee0f800 // fabs v0.2d, v0.2d
	WORD $0x4e60d610 // fadd v16.2d, v16.2d, v0.2d
	WORD $0x4ee5d421 // fsub v1.2d, v1.2d, v5.2d
	WORD $0x4ee0f821 // fabs v1.2d, v1.2d
	WORD $0x4e61d631 // fadd v17.2d, v17.2d, v1.2d
	WORD $0x4ee6d442 // fsub v2.2d, v2.2d, v6.2d
	WORD $0x4ee0f842 // fabs v2.2d, v2.2d
	WORD $0x4e62d652 // fadd v18.2d, v18.2d, v2.2d
	WORD $0x4ee7d463 // fsub v3.2d, v3.2d, v7.2d
	WORD $0x4ee0f863 // fabs v3.2d, v3.2d
	WORD $0x4e63d673 // fadd v19.2d, v19.2d, v3.2d
	WORD $0x4eecd508 // fsub v8.2d, v8.2d, v12.2d
	WORD $0x4ee0f908 // fabs v8.2d, v8.2d
	WORD $0x4e68d694 // fadd v20.2d, v20.2d, v8.2d
	WORD $0x4eedd529 // fsub v9.2d, v9.2d, v13.2d
	WORD $0x4ee0f929 // fabs v9.2d, v9.2d
	WORD $0x4e69d6b5 // fadd v21.2d, v21.2d, v9.2d
	WORD $0x4eeed54a // fsub v10.2d, v10.2d, v14.2d
	WORD $0x4ee0f94a // fabs v10.2d, v10.2d
	WORD $0x4e6ad6d6 // fadd v22.2d, v22.2d, v10.2d
	WORD $0x4eefd56b // fsub v11.2d, v11.2d, v15.2d
	WORD $0x4ee0f96b // fabs v11.2d, v11.2d
	WORD $0x4e6bd6f7 // fadd v23.2d, v23.2d, v11.2d
	SUB $16, R1
	CMP $16, R1
	BGE vectorL1NEON_loop16

vectorL1NEON_step8:
	CMP $8, R1
	BLT vectorL1NEON_reduce
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	WORD $0x4ee4d400 // fsub v0.2d, v0.2d, v4.2d
	WORD $0x4ee0f800 // fabs v0.2d, v0.2d
	WORD $0x4e60d610 // fadd v16.2d, v16.2d, v0.2d
	WORD $0x4ee5d421 // fsub v1.2d, v1.2d, v5.2d
	WORD $0x4ee0f821 // fabs v1.2d, v1.2d
	WORD $0x4e61d631 // fadd v17.2d, v17.2d, v1.2d
	WORD $0x4ee6d442 // fsub v2.2d, v2.2d, v6.2d
	WORD $0x4ee0f842 // fabs v2.2d, v2.2d
	WORD $0x4e62d652 // fadd v18.2d, v18.2d, v2.2d
	WORD $0x4ee7d463 // fsub v3.2d, v3.2d, v7.2d
	WORD $0x4ee0f863 // fabs v3.2d, v3.2d
	WORD $0x4e63d673 // fadd v19.2d, v19.2d, v3.2d
	SUB $8, R1

vectorL1NEON_reduce:
	WORD $0x4e74d610 // fadd v16.2d, v16.2d, v20.2d
	WORD $0x4e75d631 // fadd v17.2d, v17.2d, v21.2d
	WORD $0x4e76d652 // fadd v18.2d, v18.2d, v22.2d
	WORD $0x4e77d673 // fadd v19.2d, v19.2d, v23.2d
	WORD $0x4e72d610 // fadd v16.2d, v16.2d, v18.2d
	WORD $0x4e73d631 // fadd v17.2d, v17.2d, v19.2d
	WORD $0x4e71d610 // fadd v16.2d, v16.2d, v17.2d
	WORD $0x7e70da00 // faddp d0, v16.2d

vectorL1NEON_tail:
	CBZ R1, vectorL1NEON_done
	FMOVD.P 8(R0), F1
	FMOVD.P 8(R2), F2
	FSUBD F2, F1, F1
	FABSD F1, F1
	FADDD F1, F0, F0
	SUB $1, R1
	B vectorL1NEON_tail

vectorL1NEON_done:
	FMOVD F0, ret+48(FP)
	RET

// vectorAccumulateNEON adds src into dst in place. It is used while building
// centroids and during IVF k-means training. Unlike distance kernels it has no
// reduction: each loaded dst lane is immediately written back after the add.
TEXT ·vectorAccumulateNEON(SB), NOSPLIT, $0-48
	MOVD dst_base+0(FP), R0
	MOVD dst_len+8(FP), R1
	MOVD src_base+24(FP), R2

	CMP $8, R1
	BLT accumulate_tail
	MOVD R0, R3

accumulate_loop:
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4e62d421 // fadd v1.2d, v1.2d, v2.2d
	VST1.P [V1.B16], 16(R0)
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4e62d421 // fadd v1.2d, v1.2d, v2.2d
	VST1.P [V1.B16], 16(R0)
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4e62d421 // fadd v1.2d, v1.2d, v2.2d
	VST1.P [V1.B16], 16(R0)
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4e62d421 // fadd v1.2d, v1.2d, v2.2d
	VST1.P [V1.B16], 16(R0)
	SUB $8, R1
	CMP $8, R1
	BGE accumulate_loop

accumulate_tail:
	CBZ R1, accumulate_done
	FMOVD (R0), F0
	FMOVD.P 8(R2), F1
	FADDD F1, F0, F0
	FMOVD F0, (R0)
	ADD $8, R0
	SUB $1, R1
	B accumulate_tail

accumulate_done:
	RET


// vectorCosineNEON computes dot(a,b), dot(a,a), and dot(b,b) in one pass, so
// the scalar VEC_COSINE_SIMILARITY path reads both vectors once. Each result
// has eight accumulators; the two 8-lane halves of an iteration use disjoint
// halves of them, which needs all 32 vector registers.
TEXT ·vectorCosineNEON(SB), NOSPLIT, $0-72
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	VEOR V8.B16, V8.B16, V8.B16
	VEOR V9.B16, V9.B16, V9.B16
	VEOR V10.B16, V10.B16, V10.B16
	VEOR V11.B16, V11.B16, V11.B16
	VEOR V12.B16, V12.B16, V12.B16
	VEOR V13.B16, V13.B16, V13.B16
	VEOR V14.B16, V14.B16, V14.B16
	VEOR V15.B16, V15.B16, V15.B16
	VEOR V16.B16, V16.B16, V16.B16
	VEOR V17.B16, V17.B16, V17.B16
	VEOR V18.B16, V18.B16, V18.B16
	VEOR V19.B16, V19.B16, V19.B16
	VEOR V20.B16, V20.B16, V20.B16
	VEOR V21.B16, V21.B16, V21.B16
	VEOR V22.B16, V22.B16, V22.B16
	VEOR V23.B16, V23.B16, V23.B16
	VEOR V24.B16, V24.B16, V24.B16
	VEOR V25.B16, V25.B16, V25.B16
	VEOR V26.B16, V26.B16, V26.B16
	VEOR V27.B16, V27.B16, V27.B16
	VEOR V28.B16, V28.B16, V28.B16
	VEOR V29.B16, V29.B16, V29.B16
	VEOR V30.B16, V30.B16, V30.B16
	VEOR V31.B16, V31.B16, V31.B16
	CMP $16, R1
	BLT cosine_step8

cosine_loop16:
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VFMLA V0.D2, V4.D2, V8.D2
	VFMLA V0.D2, V0.D2, V16.D2
	VFMLA V4.D2, V4.D2, V24.D2
	VFMLA V1.D2, V5.D2, V9.D2
	VFMLA V1.D2, V1.D2, V17.D2
	VFMLA V5.D2, V5.D2, V25.D2
	VFMLA V2.D2, V6.D2, V10.D2
	VFMLA V2.D2, V2.D2, V18.D2
	VFMLA V6.D2, V6.D2, V26.D2
	VFMLA V3.D2, V7.D2, V11.D2
	VFMLA V3.D2, V3.D2, V19.D2
	VFMLA V7.D2, V7.D2, V27.D2
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VFMLA V0.D2, V4.D2, V12.D2
	VFMLA V0.D2, V0.D2, V20.D2
	VFMLA V4.D2, V4.D2, V28.D2
	VFMLA V1.D2, V5.D2, V13.D2
	VFMLA V1.D2, V1.D2, V21.D2
	VFMLA V5.D2, V5.D2, V29.D2
	VFMLA V2.D2, V6.D2, V14.D2
	VFMLA V2.D2, V2.D2, V22.D2
	VFMLA V6.D2, V6.D2, V30.D2
	VFMLA V3.D2, V7.D2, V15.D2
	VFMLA V3.D2, V3.D2, V23.D2
	VFMLA V7.D2, V7.D2, V31.D2
	SUB $16, R1
	CMP $16, R1
	BGE cosine_loop16

cosine_step8:
	CMP $8, R1
	BLT cosine_reduce
	VLD1.P 64(R0), [V0.D2, V1.D2, V2.D2, V3.D2]
	VLD1.P 64(R2), [V4.D2, V5.D2, V6.D2, V7.D2]
	VFMLA V0.D2, V4.D2, V8.D2
	VFMLA V0.D2, V0.D2, V16.D2
	VFMLA V4.D2, V4.D2, V24.D2
	VFMLA V1.D2, V5.D2, V9.D2
	VFMLA V1.D2, V1.D2, V17.D2
	VFMLA V5.D2, V5.D2, V25.D2
	VFMLA V2.D2, V6.D2, V10.D2
	VFMLA V2.D2, V2.D2, V18.D2
	VFMLA V6.D2, V6.D2, V26.D2
	VFMLA V3.D2, V7.D2, V11.D2
	VFMLA V3.D2, V3.D2, V19.D2
	VFMLA V7.D2, V7.D2, V27.D2
	SUB $8, R1

cosine_reduce:
	WORD $0x4e6cd508 // fadd v8.2d, v8.2d, v12.2d
	WORD $0x4e6dd529 // fadd v9.2d, v9.2d, v13.2d
	WORD $0x4e6ed54a // fadd v10.2d, v10.2d, v14.2d
	WORD $0x4e6fd56b // fadd v11.2d, v11.2d, v15.2d
	WORD $0x4e6ad508 // fadd v8.2d, v8.2d, v10.2d
	WORD $0x4e6bd529 // fadd v9.2d, v9.2d, v11.2d
	WORD $0x4e69d508 // fadd v8.2d, v8.2d, v9.2d
	WORD $0x7e70d900 // faddp d0, v8.2d
	WORD $0x4e74d610 // fadd v16.2d, v16.2d, v20.2d
	WORD $0x4e75d631 // fadd v17.2d, v17.2d, v21.2d
	WORD $0x4e76d652 // fadd v18.2d, v18.2d, v22.2d
	WORD $0x4e77d673 // fadd v19.2d, v19.2d, v23.2d
	WORD $0x4e72d610 // fadd v16.2d, v16.2d, v18.2d
	WORD $0x4e73d631 // fadd v17.2d, v17.2d, v19.2d
	WORD $0x4e71d610 // fadd v16.2d, v16.2d, v17.2d
	WORD $0x7e70da01 // faddp d1, v16.2d
	WORD $0x4e7cd718 // fadd v24.2d, v24.2d, v28.2d
	WORD $0x4e7dd739 // fadd v25.2d, v25.2d, v29.2d
	WORD $0x4e7ed75a // fadd v26.2d, v26.2d, v30.2d
	WORD $0x4e7fd77b // fadd v27.2d, v27.2d, v31.2d
	WORD $0x4e7ad718 // fadd v24.2d, v24.2d, v26.2d
	WORD $0x4e7bd739 // fadd v25.2d, v25.2d, v27.2d
	WORD $0x4e79d718 // fadd v24.2d, v24.2d, v25.2d
	WORD $0x7e70db02 // faddp d2, v24.2d

cosine_tail:
	CBZ R1, cosine_done
	FMOVD.P 8(R0), F3
	FMOVD.P 8(R2), F4
	FMADDD F4, F0, F3, F0
	FMADDD F3, F1, F3, F1
	FMADDD F4, F2, F4, F2
	SUB $1, R1
	B cosine_tail

cosine_done:
	FMOVD F0, dot+48(FP)
	FMOVD F1, normA2+56(FP)
	FMOVD F2, normB2+64(FP)
	RET

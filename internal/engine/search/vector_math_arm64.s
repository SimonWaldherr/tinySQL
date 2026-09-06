#include "textflag.h"

// vectorDotNEON computes the dot product of two equally sized float64 slices.
// ARM64 guarantees ASIMD/NEON. The main loop processes eight float64 values
// per iteration with four independent accumulators to hide FMA latency.
TEXT ·vectorDotNEON(SB), NOSPLIT, $0-56
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	FMOVD ZR, F0
	CMP $8, R1
	BLT tail

	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16

loop:
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V3.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V4.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V5.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V6.D2
	SUB $8, R1
	CMP $8, R1
	BGE loop

	VMOV V3.D[0], R3
	VMOV V3.D[1], R4
	FMOVD R3, F0
	FMOVD R4, F1
	FADDD F1, F0, F0
	VMOV V4.D[0], R3
	VMOV V4.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	VMOV V5.D[0], R3
	VMOV V5.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	VMOV V6.D[0], R3
	VMOV V6.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0

tail:
	CBZ R1, done
	FMOVD.P 8(R0), F1
	FMOVD.P 8(R2), F2
	FMADDD F2, F0, F1, F0
	SUB $1, R1
	B tail

done:
	FMOVD F0, ret+48(FP)
	RET

// vectorL2SquaredNEON computes the squared Euclidean distance for two equally
// sized float64 slices. It mirrors the dot kernel structure so L2 vector search
// can use SIMD instead of the portable Go loop.
TEXT ·vectorL2SquaredNEON(SB), NOSPLIT, $0-56
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	FMOVD ZR, F0
	CMP $8, R1
	BLT l2_tail

	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16

l2_loop:
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4ee2d427 // fsub v7.2d, v1.2d, v2.2d
	VFMLA V7.D2, V7.D2, V3.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4ee2d427 // fsub v7.2d, v1.2d, v2.2d
	VFMLA V7.D2, V7.D2, V4.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4ee2d427 // fsub v7.2d, v1.2d, v2.2d
	VFMLA V7.D2, V7.D2, V5.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	WORD $0x4ee2d427 // fsub v7.2d, v1.2d, v2.2d
	VFMLA V7.D2, V7.D2, V6.D2
	SUB $8, R1
	CMP $8, R1
	BGE l2_loop

	VMOV V3.D[0], R3
	VMOV V3.D[1], R4
	FMOVD R3, F0
	FMOVD R4, F1
	FADDD F1, F0, F0
	VMOV V4.D[0], R3
	VMOV V4.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	VMOV V5.D[0], R3
	VMOV V5.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	VMOV V6.D[0], R3
	VMOV V6.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0

l2_tail:
	CBZ R1, l2_done
	FMOVD.P 8(R0), F1
	FMOVD.P 8(R2), F2
	FSUBD F2, F1, F1
	FMADDD F1, F0, F1, F0
	SUB $1, R1
	B l2_tail

l2_done:
	FMOVD F0, ret+48(FP)
	RET

// vectorL1NEON computes the Manhattan distance of two equally sized vectors.
// Four independent accumulators process eight float64 values per iteration.
TEXT ·vectorL1NEON(SB), NOSPLIT, $0-56
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	FMOVD ZR, F0
	CMP $8, R1
	BLT l1_tail

	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16

l1_loop:
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFSUB V2.D2, V1.D2, V7.D2
	VFABS V7.D2, V7.D2
	VFADD V7.D2, V3.D2, V3.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFSUB V2.D2, V1.D2, V7.D2
	VFABS V7.D2, V7.D2
	VFADD V7.D2, V4.D2, V4.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFSUB V2.D2, V1.D2, V7.D2
	VFABS V7.D2, V7.D2
	VFADD V7.D2, V5.D2, V5.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFSUB V2.D2, V1.D2, V7.D2
	VFABS V7.D2, V7.D2
	VFADD V7.D2, V6.D2, V6.D2
	SUB $8, R1
	CMP $8, R1
	BGE l1_loop

	VMOV V3.D[0], R3
	VMOV V3.D[1], R4
	FMOVD R3, F0
	FMOVD R4, F1
	FADDD F1, F0, F0
	VMOV V4.D[0], R3
	VMOV V4.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	VMOV V5.D[0], R3
	VMOV V5.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0
	VMOV V6.D[0], R3
	VMOV V6.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F2
	FADDD F1, F0, F0
	FADDD F2, F0, F0

l1_tail:
	CBZ R1, l1_done
	FMOVD.P 8(R0), F1
	FMOVD.P 8(R2), F2
	FSUBD F2, F1, F1
	FABSD F1, F1
	FADDD F1, F0, F0
	SUB $1, R1
	B l1_tail

l1_done:
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
	VFADD V2.D2, V1.D2, V1.D2
	VST1.P [V1.B16], 16(R0)
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFADD V2.D2, V1.D2, V1.D2
	VST1.P [V1.B16], 16(R0)
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFADD V2.D2, V1.D2, V1.D2
	VST1.P [V1.B16], 16(R0)
	VLD1.P 16(R3), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFADD V2.D2, V1.D2, V1.D2
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

// vectorCosineNEON computes dot(a,b), dot(a,a), and dot(b,b) in one pass.
// Four independent accumulators per result hide FMA latency while the input
// vectors are loaded once. This is the scalar VEC_COSINE_SIMILARITY hot path.
TEXT ·vectorCosineNEON(SB), NOSPLIT, $0-72
	MOVD a_base+0(FP), R0
	MOVD a_len+8(FP), R1
	MOVD b_base+24(FP), R2

	FMOVD ZR, F0
	FMOVD ZR, F1
	FMOVD ZR, F2
	CMP $8, R1
	BLT cosine_tail

	VEOR V3.B16, V3.B16, V3.B16
	VEOR V4.B16, V4.B16, V4.B16
	VEOR V5.B16, V5.B16, V5.B16
	VEOR V6.B16, V6.B16, V6.B16
	VEOR V7.B16, V7.B16, V7.B16
	VEOR V8.B16, V8.B16, V8.B16
	VEOR V9.B16, V9.B16, V9.B16
	VEOR V10.B16, V10.B16, V10.B16
	VEOR V11.B16, V11.B16, V11.B16
	VEOR V12.B16, V12.B16, V12.B16
	VEOR V13.B16, V13.B16, V13.B16
	VEOR V14.B16, V14.B16, V14.B16

cosine_loop:
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V3.D2
	VFMLA V1.D2, V1.D2, V7.D2
	VFMLA V2.D2, V2.D2, V11.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V4.D2
	VFMLA V1.D2, V1.D2, V8.D2
	VFMLA V2.D2, V2.D2, V12.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V5.D2
	VFMLA V1.D2, V1.D2, V9.D2
	VFMLA V2.D2, V2.D2, V13.D2
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R2), [V2.B16]
	VFMLA V1.D2, V2.D2, V6.D2
	VFMLA V1.D2, V1.D2, V10.D2
	VFMLA V2.D2, V2.D2, V14.D2
	SUB $8, R1
	CMP $8, R1
	BGE cosine_loop

	VMOV V3.D[0], R3
	VMOV V3.D[1], R4
	FMOVD R3, F0
	FMOVD R4, F3
	FADDD F3, F0, F0
	VMOV V4.D[0], R3
	VMOV V4.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F0, F0
	FADDD F4, F0, F0
	VMOV V5.D[0], R3
	VMOV V5.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F0, F0
	FADDD F4, F0, F0
	VMOV V6.D[0], R3
	VMOV V6.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F0, F0
	FADDD F4, F0, F0

	VMOV V7.D[0], R3
	VMOV V7.D[1], R4
	FMOVD R3, F1
	FMOVD R4, F3
	FADDD F3, F1, F1
	VMOV V8.D[0], R3
	VMOV V8.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F1, F1
	FADDD F4, F1, F1
	VMOV V9.D[0], R3
	VMOV V9.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F1, F1
	FADDD F4, F1, F1
	VMOV V10.D[0], R3
	VMOV V10.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F1, F1
	FADDD F4, F1, F1

	VMOV V11.D[0], R3
	VMOV V11.D[1], R4
	FMOVD R3, F2
	FMOVD R4, F3
	FADDD F3, F2, F2
	VMOV V12.D[0], R3
	VMOV V12.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F2, F2
	FADDD F4, F2, F2
	VMOV V13.D[0], R3
	VMOV V13.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F2, F2
	FADDD F4, F2, F2
	VMOV V14.D[0], R3
	VMOV V14.D[1], R4
	FMOVD R3, F3
	FMOVD R4, F4
	FADDD F3, F2, F2
	FADDD F4, F2, F2

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

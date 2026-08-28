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

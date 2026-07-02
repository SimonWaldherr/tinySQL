#include "textflag.h"

// vectorDotSSE2 computes the dot product of two equally sized float64 slices.
// amd64 guarantees SSE2, so this path is safe for x86_64 Linux without runtime
// CPU feature checks. The main loop consumes eight float64 values per iteration
// with four independent accumulators to reduce the dependency chain.
TEXT ·vectorDotSSE2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	XORPD X0, X0
	CMPQ CX, $8
	JLT tail
	XORPD X3, X3
	XORPD X4, X4
	XORPD X5, X5

loop:
	MOVUPD (SI), X1
	MOVUPD (DI), X2
	MULPD X2, X1
	ADDPD X1, X0
	MOVUPD 16(SI), X1
	MOVUPD 16(DI), X2
	MULPD X2, X1
	ADDPD X1, X3
	MOVUPD 32(SI), X1
	MOVUPD 32(DI), X2
	MULPD X2, X1
	ADDPD X1, X4
	MOVUPD 48(SI), X1
	MOVUPD 48(DI), X2
	MULPD X2, X1
	ADDPD X1, X5
	ADDQ $64, SI
	ADDQ $64, DI
	SUBQ $8, CX
	CMPQ CX, $8
	JGE loop

	ADDPD X3, X0
	ADDPD X5, X4
	ADDPD X4, X0
	MOVAPD X0, X3
	UNPCKHPD X3, X3
	ADDSD X3, X0

tail:
	TESTQ CX, CX
	JEQ done

tail_loop:
	MOVSD (SI), X1
	MULSD (DI), X1
	ADDSD X1, X0
	ADDQ $8, SI
	ADDQ $8, DI
	DECQ CX
	JNZ tail_loop

done:
	MOVSD X0, ret+48(FP)
	RET

// vectorL2SquaredSSE2 computes the squared Euclidean distance for two equally
// sized float64 slices using baseline SSE2 packed double operations.
TEXT ·vectorL2SquaredSSE2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	XORPD X0, X0
	CMPQ CX, $8
	JLT l2_tail
	XORPD X3, X3
	XORPD X4, X4
	XORPD X5, X5

l2_loop:
	MOVUPD (SI), X1
	SUBPD (DI), X1
	MULPD X1, X1
	ADDPD X1, X0
	MOVUPD 16(SI), X1
	SUBPD 16(DI), X1
	MULPD X1, X1
	ADDPD X1, X3
	MOVUPD 32(SI), X1
	SUBPD 32(DI), X1
	MULPD X1, X1
	ADDPD X1, X4
	MOVUPD 48(SI), X1
	SUBPD 48(DI), X1
	MULPD X1, X1
	ADDPD X1, X5
	ADDQ $64, SI
	ADDQ $64, DI
	SUBQ $8, CX
	CMPQ CX, $8
	JGE l2_loop

	ADDPD X3, X0
	ADDPD X5, X4
	ADDPD X4, X0
	MOVAPD X0, X3
	UNPCKHPD X3, X3
	ADDSD X3, X0

l2_tail:
	TESTQ CX, CX
	JEQ l2_done

l2_tail_loop:
	MOVSD (SI), X1
	SUBSD (DI), X1
	MULSD X1, X1
	ADDSD X1, X0
	ADDQ $8, SI
	ADDQ $8, DI
	DECQ CX
	JNZ l2_tail_loop

l2_done:
	MOVSD X0, ret+48(FP)
	RET

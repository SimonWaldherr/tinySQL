#include "textflag.h"

// x86cpuid executes the CPUID instruction for the given leaf (AX) and
// subleaf (CX) and returns all four output registers. Used only once at
// startup by detectAVX2FMA (vector_math_amd64.go).
TEXT ·x86cpuid(SB), NOSPLIT, $0-24
	MOVL eaxArg+0(FP), AX
	MOVL ecxArg+4(FP), CX
	CPUID
	MOVL AX, eax+8(FP)
	MOVL BX, ebx+12(FP)
	MOVL CX, ecx+16(FP)
	MOVL DX, edx+20(FP)
	RET

// x86xgetbv reads XCR0 (extended control register 0), which reports which
// register states the OS saves/restores on context switch. Only executed
// after CPUID confirmed OSXSAVE support.
TEXT ·x86xgetbv(SB), NOSPLIT, $0-8
	MOVL $0, CX
	XGETBV
	MOVL AX, eax+0(FP)
	MOVL DX, edx+4(FP)
	RET

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

// vectorL1SSE2 computes the Manhattan (L1) distance for two equally sized
// float64 slices. SSE2 has no packed-double ABS instruction, so each lane's
// sign bit is cleared with ANDPD against a mask built from PCMPEQL+PSRLQ
// (compare-self-equal sets every bit, then a 1-bit logical shift right
// clears just the sign bit of each 64-bit lane) — the standard SIMD
// float-abs idiom, avoiding a data-section constant.
TEXT ·vectorL1SSE2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	PCMPEQL X7, X7
	PSRLQ $1, X7 // X7 = 0x7FFFFFFFFFFFFFFF in each 64-bit lane (abs mask)

	XORPD X0, X0
	CMPQ CX, $8
	JLT l1_tail
	XORPD X3, X3
	XORPD X4, X4
	XORPD X5, X5

l1_loop:
	MOVUPD (SI), X1
	SUBPD (DI), X1
	ANDPD X7, X1
	ADDPD X1, X0
	MOVUPD 16(SI), X1
	SUBPD 16(DI), X1
	ANDPD X7, X1
	ADDPD X1, X3
	MOVUPD 32(SI), X1
	SUBPD 32(DI), X1
	ANDPD X7, X1
	ADDPD X1, X4
	MOVUPD 48(SI), X1
	SUBPD 48(DI), X1
	ANDPD X7, X1
	ADDPD X1, X5
	ADDQ $64, SI
	ADDQ $64, DI
	SUBQ $8, CX
	CMPQ CX, $8
	JGE l1_loop

	ADDPD X3, X0
	ADDPD X5, X4
	ADDPD X4, X0
	MOVAPD X0, X3
	UNPCKHPD X3, X3
	ADDSD X3, X0

l1_tail:
	TESTQ CX, CX
	JEQ l1_done

l1_tail_loop:
	MOVSD (SI), X1
	SUBSD (DI), X1
	ANDPD X7, X1
	ADDSD X1, X0
	ADDQ $8, SI
	ADDQ $8, DI
	DECQ CX
	JNZ l1_tail_loop

l1_done:
	MOVSD X0, ret+48(FP)
	RET

// vectorCosineSSE2 computes dot(a,b), dot(a,a) and dot(b,b) in one pass.
// Cosine similarity without cached norms needs all three; fusing them keeps
// the memory traffic of a single dot product instead of three separate
// loops. Two independent accumulator groups (X0/X2/X4 and X1/X3/X5) hide the
// ADDPD latency chain; each 2-element group costs two loads and three
// multiply-adds.
TEXT ·vectorCosineSSE2(SB), NOSPLIT, $0-72
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	XORPD X0, X0 // dot   (even group)
	XORPD X1, X1 // dot   (odd group)
	XORPD X2, X2 // normA2 (even group)
	XORPD X3, X3 // normA2 (odd group)
	XORPD X4, X4 // normB2 (even group)
	XORPD X5, X5 // normB2 (odd group)

	CMPQ CX, $4
	JLT cos_tail

cos_loop:
	MOVUPD (SI), X6
	MOVUPD (DI), X7
	MOVAPD X6, X8
	MULPD  X7, X8
	ADDPD  X8, X0
	MULPD  X6, X6
	ADDPD  X6, X2
	MULPD  X7, X7
	ADDPD  X7, X4
	MOVUPD 16(SI), X9
	MOVUPD 16(DI), X10
	MOVAPD X9, X11
	MULPD  X10, X11
	ADDPD  X11, X1
	MULPD  X9, X9
	ADDPD  X9, X3
	MULPD  X10, X10
	ADDPD  X10, X5
	ADDQ   $32, SI
	ADDQ   $32, DI
	SUBQ   $4, CX
	CMPQ   CX, $4
	JGE    cos_loop

cos_tail:
	TESTQ CX, CX
	JEQ cos_reduce

cos_tail_loop:
	MOVSD  (SI), X6
	MOVSD  (DI), X7
	MOVAPD X6, X8
	MULSD  X7, X8
	ADDSD  X8, X0
	MULSD  X6, X6
	ADDSD  X6, X2
	MULSD  X7, X7
	ADDSD  X7, X4
	ADDQ   $8, SI
	ADDQ   $8, DI
	DECQ   CX
	JNZ    cos_tail_loop

cos_reduce:
	ADDPD  X1, X0
	ADDPD  X3, X2
	ADDPD  X5, X4
	MOVAPD X0, X6
	UNPCKHPD X6, X6
	ADDSD  X6, X0
	MOVAPD X2, X6
	UNPCKHPD X6, X6
	ADDSD  X6, X2
	MOVAPD X4, X6
	UNPCKHPD X6, X6
	ADDSD  X6, X4
	MOVSD  X0, dot+48(FP)
	MOVSD  X2, normA2+56(FP)
	MOVSD  X4, normB2+64(FP)
	RET

// vectorDotAVX2 computes the dot product with AVX2+FMA: 16 float64 per
// iteration across four independent YMM accumulators, each step a single
// VFMADD231PD. Only dispatched when CPUID reports AVX2 and FMA (see
// vector_math_amd64.go); lengths below one main-loop iteration stay on the
// SSE2 kernel, so the scalar tail here runs at most 15 iterations after at
// least one full vector pass.
TEXT ·vectorDotAVX2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	VXORPD Y0, Y0, Y0
	VXORPD Y1, Y1, Y1
	VXORPD Y2, Y2, Y2
	VXORPD Y3, Y3, Y3
	CMPQ CX, $16
	JLT dot_avx_reduce

dot_avx_loop:
	VMOVUPD (SI), Y4
	VMOVUPD 32(SI), Y5
	VMOVUPD 64(SI), Y6
	VMOVUPD 96(SI), Y7
	VFMADD231PD (DI), Y4, Y0
	VFMADD231PD 32(DI), Y5, Y1
	VFMADD231PD 64(DI), Y6, Y2
	VFMADD231PD 96(DI), Y7, Y3
	ADDQ $128, SI
	ADDQ $128, DI
	SUBQ $16, CX
	CMPQ CX, $16
	JGE dot_avx_loop

dot_avx_reduce:
	VADDPD Y1, Y0, Y0
	VADDPD Y3, Y2, Y2
	VADDPD Y2, Y0, Y0
	VEXTRACTF128 $1, Y0, X1
	VADDPD X1, X0, X0
	VZEROUPPER
	MOVAPD X0, X1
	UNPCKHPD X1, X1
	ADDSD X1, X0

	TESTQ CX, CX
	JEQ dot_avx_done

dot_avx_tail:
	MOVSD (SI), X1
	MULSD (DI), X1
	ADDSD X1, X0
	ADDQ $8, SI
	ADDQ $8, DI
	DECQ CX
	JNZ dot_avx_tail

dot_avx_done:
	MOVSD X0, ret+48(FP)
	RET

// vectorL2SquaredAVX2 computes the squared Euclidean distance with AVX2+FMA:
// d = a-b per lane, then acc += d*d via VFMADD231PD with the difference used
// as both multiplicands.
TEXT ·vectorL2SquaredAVX2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	VXORPD Y0, Y0, Y0
	VXORPD Y1, Y1, Y1
	VXORPD Y2, Y2, Y2
	VXORPD Y3, Y3, Y3
	CMPQ CX, $16
	JLT l2_avx_reduce

l2_avx_loop:
	VMOVUPD (SI), Y4
	VMOVUPD 32(SI), Y5
	VMOVUPD 64(SI), Y6
	VMOVUPD 96(SI), Y7
	VSUBPD (DI), Y4, Y4
	VSUBPD 32(DI), Y5, Y5
	VSUBPD 64(DI), Y6, Y6
	VSUBPD 96(DI), Y7, Y7
	VFMADD231PD Y4, Y4, Y0
	VFMADD231PD Y5, Y5, Y1
	VFMADD231PD Y6, Y6, Y2
	VFMADD231PD Y7, Y7, Y3
	ADDQ $128, SI
	ADDQ $128, DI
	SUBQ $16, CX
	CMPQ CX, $16
	JGE l2_avx_loop

l2_avx_reduce:
	VADDPD Y1, Y0, Y0
	VADDPD Y3, Y2, Y2
	VADDPD Y2, Y0, Y0
	VEXTRACTF128 $1, Y0, X1
	VADDPD X1, X0, X0
	VZEROUPPER
	MOVAPD X0, X1
	UNPCKHPD X1, X1
	ADDSD X1, X0

	TESTQ CX, CX
	JEQ l2_avx_done

l2_avx_tail:
	MOVSD (SI), X1
	SUBSD (DI), X1
	MULSD X1, X1
	ADDSD X1, X0
	ADDQ $8, SI
	ADDQ $8, DI
	DECQ CX
	JNZ l2_avx_tail

l2_avx_done:
	MOVSD X0, ret+48(FP)
	RET

// vectorL1AVX2 computes the Manhattan (L1) distance with AVX2. The abs mask
// (sign bit cleared in every 64-bit lane) is built with VPCMPEQQ+VPSRLQ —
// the same idiom as the SSE2 kernel, widened to YMM.
TEXT ·vectorL1AVX2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	VPCMPEQQ Y8, Y8, Y8
	VPSRLQ $1, Y8, Y8 // Y8 = 0x7FFF... abs mask per lane

	VXORPD Y0, Y0, Y0
	VXORPD Y1, Y1, Y1
	VXORPD Y2, Y2, Y2
	VXORPD Y3, Y3, Y3
	CMPQ CX, $16
	JLT l1_avx_reduce

l1_avx_loop:
	VMOVUPD (SI), Y4
	VMOVUPD 32(SI), Y5
	VMOVUPD 64(SI), Y6
	VMOVUPD 96(SI), Y7
	VSUBPD (DI), Y4, Y4
	VSUBPD 32(DI), Y5, Y5
	VSUBPD 64(DI), Y6, Y6
	VSUBPD 96(DI), Y7, Y7
	VANDPD Y8, Y4, Y4
	VANDPD Y8, Y5, Y5
	VANDPD Y8, Y6, Y6
	VANDPD Y8, Y7, Y7
	VADDPD Y4, Y0, Y0
	VADDPD Y5, Y1, Y1
	VADDPD Y6, Y2, Y2
	VADDPD Y7, Y3, Y3
	ADDQ $128, SI
	ADDQ $128, DI
	SUBQ $16, CX
	CMPQ CX, $16
	JGE l1_avx_loop

l1_avx_reduce:
	VADDPD Y1, Y0, Y0
	VADDPD Y3, Y2, Y2
	VADDPD Y2, Y0, Y0
	VEXTRACTF128 $1, Y0, X1
	VADDPD X1, X0, X0
	VZEROUPPER
	MOVAPD X0, X1
	UNPCKHPD X1, X1
	ADDSD X1, X0

	TESTQ CX, CX
	JEQ l1_avx_done

	// X8 keeps the low lane of the Y8 abs mask across VZEROUPPER.
l1_avx_tail:
	MOVSD (SI), X1
	SUBSD (DI), X1
	ANDPD X8, X1
	ADDSD X1, X0
	ADDQ $8, SI
	ADDQ $8, DI
	DECQ CX
	JNZ l1_avx_tail

l1_avx_done:
	MOVSD X0, ret+48(FP)
	RET

// vectorCosineAVX2 fuses dot(a,b), dot(a,a) and dot(b,b) into one AVX2+FMA
// pass: 8 float64 per iteration, two loads feeding three FMAs per 4-wide
// group, with two independent accumulator groups per quantity to hide FMA
// latency.
TEXT ·vectorCosineAVX2(SB), NOSPLIT, $0-72
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI

	VXORPD Y0, Y0, Y0 // dot    (group 0)
	VXORPD Y1, Y1, Y1 // dot    (group 1)
	VXORPD Y2, Y2, Y2 // normA2 (group 0)
	VXORPD Y3, Y3, Y3 // normA2 (group 1)
	VXORPD Y4, Y4, Y4 // normB2 (group 0)
	VXORPD Y5, Y5, Y5 // normB2 (group 1)
	CMPQ CX, $8
	JLT cos_avx_reduce

cos_avx_loop:
	VMOVUPD (SI), Y6
	VMOVUPD (DI), Y7
	VMOVUPD 32(SI), Y8
	VMOVUPD 32(DI), Y9
	VFMADD231PD Y7, Y6, Y0
	VFMADD231PD Y6, Y6, Y2
	VFMADD231PD Y7, Y7, Y4
	VFMADD231PD Y9, Y8, Y1
	VFMADD231PD Y8, Y8, Y3
	VFMADD231PD Y9, Y9, Y5
	ADDQ $64, SI
	ADDQ $64, DI
	SUBQ $8, CX
	CMPQ CX, $8
	JGE cos_avx_loop

cos_avx_reduce:
	VADDPD Y1, Y0, Y0
	VADDPD Y3, Y2, Y2
	VADDPD Y5, Y4, Y4
	VEXTRACTF128 $1, Y0, X1
	VADDPD X1, X0, X0
	VEXTRACTF128 $1, Y2, X3
	VADDPD X3, X2, X2
	VEXTRACTF128 $1, Y4, X5
	VADDPD X5, X4, X4
	VZEROUPPER
	MOVAPD X0, X6
	UNPCKHPD X6, X6
	ADDSD X6, X0
	MOVAPD X2, X6
	UNPCKHPD X6, X6
	ADDSD X6, X2
	MOVAPD X4, X6
	UNPCKHPD X6, X6
	ADDSD X6, X4

	TESTQ CX, CX
	JEQ cos_avx_done

cos_avx_tail:
	MOVSD  (SI), X6
	MOVSD  (DI), X7
	MOVAPD X6, X8
	MULSD  X7, X8
	ADDSD  X8, X0
	MULSD  X6, X6
	ADDSD  X6, X2
	MULSD  X7, X7
	ADDSD  X7, X4
	ADDQ   $8, SI
	ADDQ   $8, DI
	DECQ   CX
	JNZ    cos_avx_tail

cos_avx_done:
	MOVSD X0, dot+48(FP)
	MOVSD X2, normA2+56(FP)
	MOVSD X4, normB2+64(FP)
	RET

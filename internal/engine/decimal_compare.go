package engine

import (
	"math/big"
	"math/bits"
)

// compareDecimalRats compares common DECIMAL values without the temporary
// arbitrary-precision cross products used by Rat.Cmp. Operands remain borrowed
// and exact; larger magnitudes still use Rat.Cmp.
func compareDecimalRats(a, b *big.Rat) int {
	aSign, bSign := a.Sign(), b.Sign()
	if aSign < bSign {
		return -1
	}
	if aSign > bSign {
		return 1
	}
	if aSign == 0 {
		return 0
	}

	aNum, bNum := a.Num(), b.Num()
	aDenom, bDenom := a.Denom(), b.Denom()
	if aDenom.Cmp(bDenom) == 0 {
		return aNum.Cmp(bNum)
	}
	an, anOK := decimalUint64Magnitude(aNum)
	bn, bnOK := decimalUint64Magnitude(bNum)
	ad, adOK := decimalUint64Magnitude(aDenom)
	bd, bdOK := decimalUint64Magnitude(bDenom)
	if !anOK || !bnOK || !adOK || !bdOK {
		return a.Cmp(b)
	}

	// A uint64 numerator times a uint64 denominator fits in exactly 128 bits.
	// Compare both halves before applying the common sign.
	aHi, aLo := bits.Mul64(an, bd)
	bHi, bLo := bits.Mul64(bn, ad)
	if aHi < bHi || aHi == bHi && aLo < bLo {
		return -aSign
	}
	if aHi > bHi || aHi == bHi && aLo > bLo {
		return aSign
	}
	return 0
}

// Bits exposes the absolute value, including for negative numerators. Reading
// its words avoids copying a big.Int merely to obtain an unsigned magnitude.
func decimalUint64Magnitude(value *big.Int) (uint64, bool) {
	words := value.Bits()
	if len(words) == 0 {
		return 0, true
	}
	if bits.UintSize == 64 {
		if len(words) == 1 {
			return uint64(words[0]), true
		}
		return 0, false
	}
	if len(words) > 2 {
		return 0, false
	}
	magnitude := uint64(words[0])
	if len(words) == 2 {
		magnitude |= uint64(words[1]) << 32
	}
	return magnitude, true
}

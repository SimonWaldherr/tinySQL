// Hash, encoding and identifier functions: MD5, SHA-1/256/512, Base64, hex,
// UUID and Soundex.
package engine

import (
	"crypto/md5"
	crand "crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// soundexMapping is a package-level table so SOUNDEX doesn't allocate and
// populate a fresh 20-entry map on every single call -- this runs per row of
// a scan, and the mapping is a fixed constant, never mutated after init.
var soundexMapping = [256]byte{
	'B': '1', 'F': '1', 'P': '1', 'V': '1',
	'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
	'D': '3', 'T': '3',
	'L': '4',
	'M': '5', 'N': '5',
	'R': '6',
}

func evalSoundex(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SOUNDEX expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := strings.ToUpper(valueText(val))
	if len(s) == 0 {
		return "", nil
	}
	// Soundex algorithm
	result := []byte{s[0]}
	lastCode := soundexMapping[s[0]]
	for i := 1; i < len(s) && len(result) < 4; i++ {
		c := s[i]
		if code := soundexMapping[c]; code != 0 && code != lastCode {
			result = append(result, code)
			lastCode = code
		} else if c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U' || c == 'H' || c == 'W' || c == 'Y' {
			lastCode = 0
		}
		// Anything else (a mapped-but-repeated consonant, or a character with
		// no soundex code at all -- e.g. a digit or punctuation) leaves
		// lastCode untouched, matching the original map-based lookup's
		// `code, ok := mapping[c]; ok && code != lastCode` gate exactly: a
		// zero value from the array here (soundexMapping[c] for a byte with
		// no entry) must not be mistaken for a real "code 0" reset.
	}
	for len(result) < 4 {
		result = append(result, '0')
	}
	return string(result), nil
}

// evalHex implements SQLite's hex(X): the upper-case hexadecimal rendering of
// X's bytes. For a BLOB that is the blob's own bytes; for every other type it
// is the UTF-8 bytes of X's text rendering, which is why hex(123) is '313233'
// (the digits) and not '7B'.
//
// The []byte case must be handled before valueText, not through it: []byte has
// no fast case in valueText, so it fell through to fmt's %v, whose []byte form
// is the decimal byte list "[1 2]". hex(X'0102') therefore returned
// '5B3120325D' — the hex of that Go rendering, punctuation and all — instead of
// '0102'.
//
// Deliberately not blobDecode: that helper decodes a hex-looking *string* as
// hex, but SQLite's hex('0102') is the hex of the four text characters
// ('30313032'), so only a real []byte counts as a blob here.
func evalHex(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("HEX expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		// NULL in, NULL out, like MD5/SHA*/BASE64 below. SQLite's own
		// hex(NULL) yields '' because it casts NULL to an empty blob, so
		// this is a deliberate deviation in favour of the engine-wide
		// NULL-propagation rule for scalar functions; the previous
		// behaviour — the hex of the string "<nil>" — matched neither.
		return nil, nil
	}
	if b, ok := val.([]byte); ok {
		return fmt.Sprintf("%X", b), nil
	}
	return fmt.Sprintf("%X", []byte(valueText(val))), nil
}

func evalUnhex(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UNHEX expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := valueText(val)
	if len(s)%2 != 0 {
		s = "0" + s
	}
	// fmt.Sscanf(s[i:i+2], "%02X", &b) per byte pair used to drive this: each
	// call re-parses the "%02X" format string and goes through reflection to
	// set b, which dominates UNHEX's cost far more than the actual decode
	// work. encoding/hex accepts the same upper/lower-case hex digits in one
	// allocation-bounded pass.
	result, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("UNHEX: invalid hex string")
	}
	return string(result), nil
}

func evalUuid(env ExecEnv, args []Expr, row Row) (any, error) {
	// Generate a simple UUID v4
	b := make([]byte, 16)
	crand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func evalMD5(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MD5 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str := valueText(val)

	// MD5 hash
	hasher := md5.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalSHA1(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA1 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str := valueText(val)

	// SHA1 hash
	hasher := sha1.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalSHA256(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA256 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str := valueText(val)

	// SHA256 hash
	hasher := sha256.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalSHA512(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SHA512 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str := valueText(val)

	// SHA512 hash
	hasher := sha512.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

func evalBase64(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BASE64 expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str := valueText(val)

	// Base64 encode
	encoded := base64.StdEncoding.EncodeToString([]byte(str))
	return encoded, nil
}

func evalBase64Decode(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BASE64_DECODE expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str := valueText(val)

	// Base64 decode
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, fmt.Errorf("BASE64_DECODE: %v", err)
	}

	return string(decoded), nil
}

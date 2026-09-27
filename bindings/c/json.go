//go:build cgo

package main

import (
	"strconv"
	"unicode/utf8"
)

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string. Invalid UTF-8 becomes U+FFFD
// and U+2028/U+2029 are escaped, matching encoding/json without its HTML
// escaping, which native hosts never need.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '"', '\\':
				dst = append(dst, '\\', b)
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

func appendInt(dst []byte, v int64) []byte { return strconv.AppendInt(dst, v, 10) }

func appendUint(dst []byte, v uint64) []byte { return strconv.AppendUint(dst, v, 10) }

// appendFloat writes the shortest representation that parses back to the
// same float64. Callers reject NaN and infinities first.
func appendFloat(dst []byte, v float64) []byte { return strconv.AppendFloat(dst, v, 'g', -1, 64) }

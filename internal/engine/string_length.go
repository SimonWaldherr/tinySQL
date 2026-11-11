package engine

// Keep the interpreter and bound functions on the same NULL, BLOB and Unicode
// rules. Named JSON byte slices retain their text representation via valueText.
func stringLengthValue(value any) any {
	if value == nil {
		return nil
	}
	if blob, ok := value.([]byte); ok {
		return len(blob)
	}
	return stringCharCount(valueText(value))
}

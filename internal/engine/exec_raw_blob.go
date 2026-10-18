package engine

// BLOB calls consume raw values directly, avoiding pooled expression wrappers.
// Their value kernels are shared with the ordinary evaluator.
func evalRawBlobFuncCall(plan *simpleSelectPlan, raw []any, ex *FuncCall) (any, error) {
	want := 1
	switch ex.Name {
	case "BLOB_SUBSTR":
		want = 3
	case "BLOB_CONCAT", "BLOB_EQUAL":
		want = 2
	}
	if len(ex.Args) != want {
		return evalFuncCall(ExecEnv{}, ex, rawEmptyRow)
	}
	var values [3]any
	for i, arg := range ex.Args {
		if literal, ok := arg.(*Literal); ok {
			values[i] = literal.Val
			continue
		}
		value, err := evalRawExpr(plan, raw, arg)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	switch ex.Name {
	case "BLOB_LENGTH":
		return blobLengthValue(values[0])
	case "BLOB_HEX":
		return blobHexValue(values[0])
	case "BLOB_FROM_HEX":
		return blobFromHexValue(values[0])
	case "BLOB_SUBSTR":
		return blobSubstrValue(values[0], values[1], values[2])
	case "BLOB_CONCAT":
		return blobConcatValue(values[0], values[1])
	case "BLOB_TO_BASE64":
		return blobToBase64Value(values[0])
	case "BLOB_FROM_BASE64":
		return blobFromBase64Value(values[0])
	case "BLOB_EQUAL":
		return blobEqualValue(values[0], values[1])
	}
	return evalFuncCall(ExecEnv{}, ex, rawEmptyRow)
}

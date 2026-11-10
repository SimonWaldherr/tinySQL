package engine

import "math"

// A heap is useful for a small page, but keeping nearly the complete input
// only adds heap maintenance before the final sort. Restrict the switch to an
// unfiltered scan with direct, consistently typed keys: mixed-type and NaN
// comparisons are not a total order and must retain their existing algorithm.
func preferFullRawSort(env ExecEnv, plan *simpleSelectPlan, rows [][]any, keepCount int) (bool, error) {
	if len(rows) < 64 || keepCount <= 0 || (keepCount < len(rows) && keepCount <= len(rows)-len(rows)/10) ||
		plan.where != nil || plan.rowIDs != nil || len(plan.orderCols) == 0 {
		return false, nil
	}
	kinds := make([]byte, len(plan.orderCols))
	for i, row := range rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return false, err
			}
		}
		for key, column := range plan.orderCols {
			if column < 0 || column >= len(row) {
				return false, nil
			}
			var kind byte
			switch value := row[column].(type) {
			case nil:
				continue
			case int:
				kind = 1
			case int64:
				kind = 2
			case float64:
				if math.IsNaN(value) {
					return false, nil
				}
				kind = 3
			case string:
				kind = 4
			case bool:
				kind = 5
			case []byte:
				kind = 6
			default:
				return false, nil
			}
			if kinds[key] != 0 && kinds[key] != kind {
				return false, nil
			}
			kinds[key] = kind
		}
	}
	return true, nil
}

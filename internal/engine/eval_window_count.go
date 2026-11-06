package engine

import "strings"

// cachedWindowFrameCount builds a query-local prefix count for long ROWS
// frames. Restrict it to direct columns: expressions and filters may have
// errors or side effects whose evaluation order must remain unchanged.
func cachedWindowFrameCount(env ExecEnv, ex *FuncCall, row Row, rows []Row, current int) (int, bool, error) {
	if ex.Name != "COUNT" || ex.Star || ex.Distinct || ex.Filter != nil || len(ex.Args) != 1 || env.triggerRow != nil || env.windowPartitions == nil || ex.Over.Frame == nil || ex.Over.Frame.Mode != "ROWS" || current < 0 || current >= len(rows) {
		return 0, false, nil
	}
	ref, ok := ex.Args[0].(*VarRef)
	if !ok {
		return 0, false, nil
	}
	frame := ex.Over.Frame
	start := rowsWindowBound(current, len(rows), frame.StartType, frame.StartValue, true)
	end := rowsWindowBound(current, len(rows), frame.EndType, frame.EndValue, false)
	if start > end || start >= len(rows) || end < 0 {
		return 0, true, nil
	}
	if end-start < 63 {
		return 0, false, nil
	}
	cache := env.windowPartitions
	shape := cache.shapeFor(ex)
	if !shape.shareable {
		return 0, false, nil
	}
	key := windowPartitionCacheKey{shape: shape, partition: formatWindowPartitionKey(env, shape.partitionBy, row)}
	entry := cache.entries[key]
	if entry == nil {
		return 0, false, nil
	}
	name := ref.Lower
	if name == "" {
		name = strings.ToLower(ref.Name)
	}
	prefix, found := entry.countPrefixes[name]
	if !found {
		prefix = make([]int, len(rows)+1)
		for i, r := range rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return 0, true, err
				}
			}
			value, ok := getValLower(r, name)
			if !ok {
				return 0, false, nil
			}
			prefix[i+1] = prefix[i]
			if value != nil {
				prefix[i+1]++
			}
		}
		if entry.countPrefixes == nil {
			entry.countPrefixes = make(map[string][]int)
		}
		entry.countPrefixes[name] = prefix
	}
	return prefix[end+1] - prefix[start], true, nil
}

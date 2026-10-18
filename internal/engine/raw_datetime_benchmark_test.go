package engine

import "testing"

var rawDateFunctionSink any

// This is the former eager raw argument wrapper, using current date handlers
// so the comparison isolates dispatch overhead for valid, non-NULL inputs.
func previousRawDateCall(plan *simpleSelectPlan, raw []any, ex *FuncCall) (any, error) {
	sc := rawCallScratchPool.Get().(*rawCallScratch)
	defer func() { sc.reset(); rawCallScratchPool.Put(sc) }()
	if cap(sc.args) < len(ex.Args) {
		sc.args = make([]Expr, len(ex.Args))
		sc.lits = make([]Literal, len(ex.Args))
	}
	args, lits := sc.args[:len(ex.Args)], sc.lits[:len(ex.Args)]
	for i, arg := range ex.Args {
		if literal, ok := arg.(*Literal); ok {
			args[i] = literal
			continue
		}
		value, err := evalRawExpr(plan, raw, arg)
		if err != nil {
			return nil, err
		}
		lits[i].Val = value
		args[i] = &lits[i]
	}
	sc.call = FuncCall{Name: ex.Name, Args: args, Star: ex.Star, Distinct: ex.Distinct}
	return boundFuncHandler(ex)(ExecEnv{now: plan.now}, &sc.call, rawEmptyRow)
}

func BenchmarkRawDateFunctions(b *testing.B) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"value": 0}}
	row := []any{"2024-03-05 06:07:08.123"}
	for _, call := range []string{
		"DATE_ADD(value, 1, 'DAY')",
		"STRFTIME('%Y-%m-%d %H:%M:%S', value)",
		"DATE(value)",
	} {
		expr := mustParse("SELECT " + call).(*Select).Projs[0].Expr.(*FuncCall)
		for _, previous := range []bool{false, true} {
			name := "Ordered"
			eval := evalRawFuncCall
			if previous {
				name, eval = "Previous", previousRawDateCall
			}
			b.Run(call+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					value, err := eval(plan, row, expr)
					if err != nil {
						b.Fatal(err)
					}
					rawDateFunctionSink = value
				}
			})
		}
	}
}

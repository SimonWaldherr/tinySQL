// Parse-time constant folding for pure vector functions.
//
// RAG queries almost always embed the query vector as a JSON literal:
//
//	SELECT ... FROM docs
//	ORDER BY VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[0.1, ...]')) DESC
//
// Without folding, VEC_FROM_JSON re-parses that literal for every row the
// expression is evaluated against — profiling showed ~85% of such a query's
// CPU time inside encoding/json. Folding the call to a Literal([]float64)
// once at parse time removes that per-row cost entirely, for every
// evaluation path (regular, raw fast path, WHERE, ORDER BY, table-function
// arguments).
package engine

import "strings"

// foldableConstFuncs lists functions that are pure (result depends only on
// their arguments) and deterministic, and are therefore safe to evaluate a
// single time at parse when every argument is already a literal. Keep this
// list conservative: anything env-, row-, time- or randomness-dependent
// (VEC_RANDOM, NOW, ...) must never appear here.
var foldableConstFuncs = map[string]bool{
	"VEC_FROM_JSON":  true,
	"VEC_FROM_BYTES": true,
	"VEC_NORMALIZE":  true,
}

// foldConstFuncCall evaluates fc at parse time and returns the result as a
// *Literal when fc is a whitelisted pure function whose arguments are all
// literals. Because expressions are parsed bottom-up, nested calls such as
// VEC_NORMALIZE(VEC_FROM_JSON('...')) fold naturally from the inside out.
//
// If evaluation fails (e.g. malformed JSON) the call is returned unfolded so
// the error still surfaces at execution time with today's semantics — a
// query that never evaluates the expression keeps never erroring.
func foldConstFuncCall(fc *FuncCall) Expr {
	if fc.Over != nil || fc.Star || fc.Distinct {
		return fc
	}
	if !foldableConstFuncs[strings.ToUpper(fc.Name)] {
		return fc
	}
	for _, arg := range fc.Args {
		if _, ok := arg.(*Literal); !ok {
			return fc
		}
	}
	v, err := evalFuncCall(ExecEnv{}, fc, Row{})
	if err != nil {
		return fc
	}
	return &Literal{Val: v}
}

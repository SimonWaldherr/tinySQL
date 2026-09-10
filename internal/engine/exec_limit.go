package engine

import "strings"

// boundedLimitRows caps LIMIT+OFFSET before adding, avoiding integer overflow
// and keeping sorting buffers bounded by the number of available candidates.
func boundedLimitRows(limit, offset *int, available int) int {
	if limit == nil {
		return available
	}
	skip := 0
	if offset != nil {
		skip = min(max(*offset, 0), available)
	}
	return skip + min(max(*limit, 0), available-skip)
}

// simpleLimitZeroPlan resolves schema without seeking indexes or scanning row
// data. In paged storage the caller can supply schema-only metadata, avoiding
// both point-read decoding and full table materialization.
func simpleLimitZeroPlan(env ExecEnv, plan *simpleSelectPlan) (*simpleSelectPlan, bool, error) {
	if err := checkCtx(env.ctx); err != nil {
		return nil, true, err
	}
	// Column errors must not disappear just because no row will be evaluated.
	if err := validateRawColumnRefs(plan.colIndex, plan.where); err != nil {
		return nil, true, err
	}
	for _, p := range plan.projs {
		if err := validateRawColumnRefs(plan.colIndex, p.expr); err != nil {
			return nil, true, err
		}
	}
	for _, e := range plan.orderExprs {
		if err := validateRawColumnRefs(plan.colIndex, e); err != nil {
			return nil, true, err
		}
	}
	resetSimplePlanAccess(plan, 0)
	plan.rowIDs = []int{}
	plan.scanType = "LIMIT ZERO"
	return plan, true, nil
}

// Only called for expressions already accepted by the raw SELECT planner.
func validateRawColumnRefs(cols map[string]int, e Expr) error {
	var children []Expr
	switch ex := e.(type) {
	case *CaseExpr:
		if err := validateRawColumnRefs(cols, ex.Operand); err != nil {
			return err
		}
		for _, branch := range ex.Whens {
			if err := validateRawColumnRefs(cols, branch.When); err != nil {
				return err
			}
			if err := validateRawColumnRefs(cols, branch.Then); err != nil {
				return err
			}
		}
		return validateRawColumnRefs(cols, ex.Else)
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		if _, ok := cols[key]; !ok {
			return unknownColumnErr(ex.Name, columnSuggestion(ex.Name, cols))
		}
	case *Unary:
		children = []Expr{ex.Expr}
	case *Binary:
		children = []Expr{ex.Left, ex.Right}
	case *IsNull:
		children = []Expr{ex.Expr}
	case *LikeExpr:
		children = []Expr{ex.Expr, ex.Pattern, ex.Escape}
	case *RegexpExpr:
		children = []Expr{ex.Expr, ex.Pattern}
	case *BetweenExpr:
		children = []Expr{ex.Expr, ex.Lo, ex.Hi}
	case *InExpr:
		if err := validateRawColumnRefs(cols, ex.Expr); err != nil {
			return err
		}
		children = ex.Values
	case *FuncCall:
		children = ex.Args
	}
	for _, child := range children {
		if err := validateRawColumnRefs(cols, child); err != nil {
			return err
		}
	}
	return nil
}

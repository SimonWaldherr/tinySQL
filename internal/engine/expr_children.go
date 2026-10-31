package engine

// anyExprChild visits children in the current query scope. Subqueries own
// their aggregates and windows and are deliberately not traversed here.
func anyExprChild(e Expr, visit func(Expr) bool) bool {
	switch ex := e.(type) {
	case *Unary:
		return visit(ex.Expr)
	case *Binary:
		return visit(ex.Left) || visit(ex.Right)
	case *IsNull:
		return visit(ex.Expr)
	case *FuncCall:
		if visit(ex.Filter) {
			return true
		}
		for _, arg := range ex.Args {
			if visit(arg) {
				return true
			}
		}
		if ex.Over != nil {
			for _, part := range ex.Over.PartitionBy {
				if visit(part) {
					return true
				}
			}
		}
	case *InExpr:
		if visit(ex.Expr) {
			return true
		}
		for _, value := range ex.Values {
			if visit(value) {
				return true
			}
		}
	case *BetweenExpr:
		return visit(ex.Expr) || visit(ex.Lo) || visit(ex.Hi)
	case *LikeExpr:
		return visit(ex.Expr) || visit(ex.Pattern) || visit(ex.Escape)
	case *RegexpExpr:
		return visit(ex.Expr) || visit(ex.Pattern)
	case *CaseExpr:
		if visit(ex.Operand) {
			return true
		}
		for _, branch := range ex.Whens {
			if visit(branch.When) || visit(branch.Then) {
				return true
			}
		}
		return visit(ex.Else)
	}
	return false
}

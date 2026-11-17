package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var parsedCheckExprCache sync.Map // string -> Expr

func parseCheckExprCached(exprText string) (Expr, error) {
	if cached, ok := parsedCheckExprCache.Load(exprText); ok {
		return cached.(Expr), nil
	}
	p := NewParser(exprText)
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ != tEOF {
		return nil, p.errf("unexpected token %q in CHECK constraint", p.cur.Val)
	}
	parsedCheckExprCache.Store(exprText, expr)
	return expr, nil
}

// validateCheckExprAST rejects expression forms that SQL forbids inside CHECK
// constraints (subqueries, aggregate functions, window functions, and bound
// parameters).
func validateCheckExprAST(expr Expr) error {
	switch e := expr.(type) {
	case nil, *Literal, *VarRef:
		if lit, ok := e.(*Literal); ok && lit.Parameter {
			return fmt.Errorf("CHECK constraint cannot contain parameters")
		}
		return nil
	case *Unary:
		return validateCheckExprAST(e.Expr)
	case *Binary:
		if err := validateCheckExprAST(e.Left); err != nil {
			return err
		}
		return validateCheckExprAST(e.Right)
	case *IsNull:
		return validateCheckExprAST(e.Expr)
	case *InExpr:
		if e.Subquery {
			return fmt.Errorf("CHECK constraint cannot contain subqueries")
		}
		if err := validateCheckExprAST(e.Expr); err != nil {
			return err
		}
		for _, v := range e.Values {
			if err := validateCheckExprAST(v); err != nil {
				return err
			}
		}
		return nil
	case *LikeExpr:
		if err := validateCheckExprAST(e.Expr); err != nil {
			return err
		}
		if err := validateCheckExprAST(e.Pattern); err != nil {
			return err
		}
		return validateCheckExprAST(e.Escape)
	case *BetweenExpr:
		if err := validateCheckExprAST(e.Expr); err != nil {
			return err
		}
		if err := validateCheckExprAST(e.Lo); err != nil {
			return err
		}
		return validateCheckExprAST(e.Hi)
	case *CaseExpr:
		if err := validateCheckExprAST(e.Operand); err != nil {
			return err
		}
		for _, w := range e.Whens {
			if err := validateCheckExprAST(w.When); err != nil {
				return err
			}
			if err := validateCheckExprAST(w.Then); err != nil {
				return err
			}
		}
		return validateCheckExprAST(e.Else)
	case *SubqueryExpr, *ExistsExpr:
		return fmt.Errorf("CHECK constraint cannot contain subqueries")
	case *FuncCall:
		if e.Over != nil {
			return fmt.Errorf("CHECK constraint cannot contain window functions")
		}
		if isAggregateName(strings.ToUpper(e.Name)) {
			return fmt.Errorf("CHECK constraint cannot contain aggregate functions")
		}
		for _, a := range e.Args {
			if err := validateCheckExprAST(a); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

func validateCheckExprColumns(tableName string, cols []storage.Column, expr Expr) error {
	colSet := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		colSet[strings.ToLower(c.Name)] = struct{}{}
	}
	tableLower := strings.ToLower(tableName)
	var walk func(Expr) error
	walk = func(node Expr) error {
		switch e := node.(type) {
		case nil, *Literal:
			return nil
		case *VarRef:
			name := e.Lower
			if name == "" {
				name = strings.ToLower(e.Name)
			}
			if idx := strings.IndexByte(name, '.'); idx >= 0 {
				prefix := name[:idx]
				col := name[idx+1:]
				if prefix != tableLower {
					return fmt.Errorf("CHECK constraint references unknown table %q", prefix)
				}
				name = col
			}
			if _, ok := colSet[name]; !ok {
				return fmt.Errorf("CHECK constraint references unknown column %q", e.Name)
			}
			return nil
		case *Unary:
			return walk(e.Expr)
		case *Binary:
			if err := walk(e.Left); err != nil {
				return err
			}
			return walk(e.Right)
		case *IsNull:
			return walk(e.Expr)
		case *InExpr:
			if err := walk(e.Expr); err != nil {
				return err
			}
			for _, v := range e.Values {
				if err := walk(v); err != nil {
					return err
				}
			}
			return nil
		case *LikeExpr:
			if err := walk(e.Expr); err != nil {
				return err
			}
			if err := walk(e.Pattern); err != nil {
				return err
			}
			return walk(e.Escape)
		case *BetweenExpr:
			if err := walk(e.Expr); err != nil {
				return err
			}
			if err := walk(e.Lo); err != nil {
				return err
			}
			return walk(e.Hi)
		case *CaseExpr:
			if err := walk(e.Operand); err != nil {
				return err
			}
			for _, w := range e.Whens {
				if err := walk(w.When); err != nil {
					return err
				}
				if err := walk(w.Then); err != nil {
					return err
				}
			}
			return walk(e.Else)
		case *FuncCall:
			for _, a := range e.Args {
				if err := walk(a); err != nil {
					return err
				}
			}
			return nil
		default:
			return nil
		}
	}
	return walk(expr)
}

func validateCheckConstraintSchema(tableName string, cols []storage.Column, checks []storage.CheckConstraint) error {
	for _, ck := range checks {
		expr, err := parseCheckExprCached(ck.Expr)
		if err != nil {
			return fmt.Errorf("invalid CHECK constraint %q: %w", ck.Expr, err)
		}
		if err := validateCheckExprAST(expr); err != nil {
			return err
		}
		if err := validateCheckExprColumns(tableName, cols, expr); err != nil {
			return err
		}
	}
	return nil
}

// validateCheckConstraints enforces SQL three-valued CHECK semantics on row:
// TRUE and NULL (UNKNOWN) pass; FALSE fails with an error.
func validateCheckConstraints(env ExecEnv, t *storage.Table, row []any) error {
	if len(t.CheckConstraints) == 0 {
		return nil
	}
	var rowMap Row
	var keys tableRowKeys
	var colIndex map[string]int
	for _, ck := range t.CheckConstraints {
		expr, err := parseCheckExprCached(ck.Expr)
		if err != nil {
			return fmt.Errorf("invalid CHECK constraint %q: %w", ck.Expr, err)
		}
		var val any
		if isSimpleRawExpr(expr) {
			if colIndex == nil {
				colIndex = simpleColumnIndex(t, t.Name)
			}
			plan := simpleSelectPlan{
				table:    t,
				colIndex: colIndex,
				now:      env.now,
			}
			val, err = evalRawExpr(&plan, row, expr)
		} else {
			if rowMap == nil {
				keys = newTableRowKeys(t.Cols, strings.ToLower(t.Name)+".")
				rowMap = buildTableRow(keys, row)
			}
			val, err = evalExpr(env, expr, rowMap)
		}
		if err != nil {
			return err
		}
		if toTri(val) == tvFalse {
			label := ck.Expr
			if ck.Name != "" {
				label = ck.Name
			}
			return fmt.Errorf("CHECK constraint failed: %s", label)
		}
	}
	return nil
}

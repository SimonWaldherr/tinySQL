package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type Row map[string]any

type ResultSet struct {
	Cols []string
	Rows []Row
}

type ExecEnv struct {
	ctx    context.Context
	tenant string
	db     *storage.DB
}

func Execute(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultSet, error) {
	env := ExecEnv{ctx: ctx, tenant: tenant, db: db}
	switch s := stmt.(type) {
	case *CreateTable:
		if s.AsSelect == nil {
			t := storage.NewTable(s.Name, s.Cols, s.IsTemp)
			db.Put(tenant, t)
			return nil, nil
		}
		rs, err := Execute(ctx, db, tenant, s.AsSelect)
		if err != nil {
			return nil, err
		}
		cols := make([]storage.Column, len(rs.Cols))
		if len(rs.Rows) > 0 {
			for i, c := range rs.Cols {
				cols[i] = storage.Column{Name: c, Type: inferType(rs.Rows[0][strings.ToLower(c)])}
			}
		} else {
			for i, c := range rs.Cols {
				cols[i] = storage.Column{Name: c, Type: storage.TextType}
			}
		}
		t := storage.NewTable(s.Name, cols, s.IsTemp)
		for _, r := range rs.Rows {
			row := make([]any, len(cols))
			for i, c := range cols {
				row[i] = r[strings.ToLower(c.Name)]
			}
			t.Rows = append(t.Rows, row)
		}
		db.Put(tenant, t)
		return nil, nil
	case *DropTable:
		return nil, db.Drop(tenant, s.Name)
	case *Insert:
		t, err := db.Get(tenant, s.Table)
		if err != nil {
			return nil, err
		}
		if len(s.Cols) > 0 && len(s.Vals) != len(s.Cols) {
			return nil, fmt.Errorf("INSERT column/value mismatch")
		}
		row := make([]any, len(t.Cols))
		for i := range row {
			row[i] = nil
		}
		tmp := Row{}
		if len(s.Cols) == 0 {
			if len(s.Vals) != len(t.Cols) {
				return nil, fmt.Errorf("INSERT expects %d values", len(t.Cols))
			}
			for i, e := range s.Vals {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, e, tmp)
				if err != nil {
					return nil, err
				}
				cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
				}
				row[i] = cv
			}
		} else {
			for i, name := range s.Cols {
				idx, err := t.ColIndex(name)
				if err != nil {
					return nil, err
				}
				v, err := evalExpr(env, s.Vals[i], tmp)
				if err != nil {
					return nil, err
				}
				cv, err := coerceToTypeAllowNull(v, t.Cols[idx].Type)
				if err != nil {
					return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
				}
				row[idx] = cv
			}
		}
		t.Rows = append(t.Rows, row)
		t.Version++
		return nil, nil
	case *Update:
		t, err := db.Get(tenant, s.Table)
		if err != nil {
			return nil, err
		}
		setIdx := map[int]Expr{}
		for name, ex := range s.Sets {
			i, err := t.ColIndex(name)
			if err != nil {
				return nil, err
			}
			setIdx[i] = ex
		}
		n := 0
		for ri, r := range t.Rows {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
			row := Row{}
			for i, c := range t.Cols {
				putVal(row, c.Name, r[i])
				putVal(row, s.Table+"."+c.Name, r[i])
			}
			ok := true
			if s.Where != nil {
				v, err := evalExpr(env, s.Where, row)
				if err != nil {
					return nil, err
				}
				ok = (toTri(v) == tvTrue)
			}
			if ok {
				for i, ex := range setIdx {
					v, err := evalExpr(env, ex, row)
					if err != nil {
						return nil, err
					}
					cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
					if err != nil {
						return nil, err
					}
					t.Rows[ri][i] = cv
				}
				n++
			}
		}
		t.Version++
		return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": n}}}, nil
	case *Delete:
		t, err := db.Get(tenant, s.Table)
		if err != nil {
			return nil, err
		}
		var kept [][]any
		del := 0
		for _, r := range t.Rows {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
			row := Row{}
			for i, c := range t.Cols {
				putVal(row, c.Name, r[i])
				putVal(row, s.Table+"."+c.Name, r[i])
			}
			keep := true
			if s.Where != nil {
				v, err := evalExpr(env, s.Where, row)
				if err != nil {
					return nil, err
				}
				if toTri(v) == tvTrue {
					keep = false
				}
			}
			if keep {
				kept = append(kept, r)
			} else {
				del++
			}
		}
		t.Rows = kept
		t.Version++
		return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
	case *Select:
		// FROM
		leftT, err := db.Get(tenant, s.From.Table)
		if err != nil {
			return nil, err
		}
		leftRows, _ := rowsFromTable(leftT, aliasOr(s.From))
		cur := leftRows
		// JOINs
		for _, j := range s.Joins {
			rt, err := db.Get(tenant, j.Right.Table)
			if err != nil {
				return nil, err
			}
			rightRows, _ := rowsFromTable(rt, aliasOr(j.Right))
			switch j.Type {
			case JoinInner:
				var joined []Row
				for _, l := range cur {
					if err := checkCtx(env.ctx); err != nil {
						return nil, err
					}
					for _, r := range rightRows {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(env, j.On, m)
							if err != nil {
								return nil, err
							}
							ok = (toTri(val) == tvTrue)
						}
						if ok {
							joined = append(joined, m)
						}
					}
				}
				cur = joined
			case JoinLeft:
				var joined []Row
				for _, l := range cur {
					if err := checkCtx(env.ctx); err != nil {
						return nil, err
					}
					matched := false
					for _, r := range rightRows {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(env, j.On, m)
							if err != nil {
								return nil, err
							}
							ok = (toTri(val) == tvTrue)
						}
						if ok {
							joined = append(joined, m)
							matched = true
						}
					}
					if !matched {
						m := cloneRow(l)
						addRightNulls(m, aliasOr(j.Right), rt)
						joined = append(joined, m)
					}
				}
				cur = joined
			case JoinRight:
				var joined []Row
				leftKeys := []string{}
				if len(cur) > 0 {
					leftKeys = keysOfRow(cur[0])
				}
				for _, r := range rightRows {
					if err := checkCtx(env.ctx); err != nil {
						return nil, err
					}
					matched := false
					for _, l := range cur {
						m := mergeRows(l, r)
						ok := true
						if j.On != nil {
							val, err := evalExpr(env, j.On, m)
							if err != nil {
								return nil, err
							}
							ok = (toTri(val) == tvTrue)
						}
						if ok {
							joined = append(joined, m)
							matched = true
						}
					}
					if !matched {
						m := cloneRow(r)
						for _, k := range leftKeys {
							m[k] = nil
						}
						joined = append(joined, m)
					}
				}
				cur = joined
			}
		}
		// WHERE
		filtered := cur
		if s.Where != nil {
			var tmp []Row
			for _, r := range filtered {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, s.Where, r)
				if err != nil {
					return nil, err
				}
				if toTri(v) == tvTrue {
					tmp = append(tmp, r)
				}
			}
			filtered = tmp
		}
		// GROUP/HAVING
		needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)
		var outRows []Row
		var outCols []string
		if needAgg {
			groups := map[string][]Row{}
			var orderKeys []string
			for _, r := range filtered {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				var parts []string
				for _, g := range s.GroupBy {
					v, err := evalExpr(env, &g, r)
					if err != nil {
						return nil, err
					}
					parts = append(parts, fmtKeyPart(v))
				}
				ks := strings.Join(parts, "\x1f")
				if _, ok := groups[ks]; !ok {
					orderKeys = append(orderKeys, ks)
				}
				groups[ks] = append(groups[ks], r)
			}
			for _, k := range orderKeys {
				rows := groups[k]
				if s.Having != nil {
					hv, err := evalAggregate(env, s.Having, rows)
					if err != nil {
						return nil, err
					}
					if toTri(hv) != tvTrue {
						continue
					}
				}
				out := Row{}
				for i, it := range s.Projs {
					if it.Star {
						if len(rows) > 0 {
							for col, v := range rows[0] {
								if strings.Contains(col, ".") {
									putVal(out, col, v)
									outCols = appendUnique(outCols, col)
								}
							}
						}
						continue
					}
					name := projName(it, i)
					var val any
					var err error
					if isAggregate(it.Expr) || len(s.GroupBy) > 0 {
						val, err = evalAggregate(env, it.Expr, rows)
					} else {
						val, err = evalExpr(env, it.Expr, rows[0])
					}
					if err != nil {
						return nil, err
					}
					putVal(out, name, val)
					outCols = appendUnique(outCols, name)
				}
				outRows = append(outRows, out)
			}
		} else {
			for _, r := range filtered {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				out := Row{}
				for i, it := range s.Projs {
					if it.Star {
						for col, v := range r {
							if strings.Contains(col, ".") {
								putVal(out, col, v)
								outCols = appendUnique(outCols, col)
							}
						}
						continue
					}
					val, err := evalExpr(env, it.Expr, r)
					if err != nil {
						return nil, err
					}
					name := projName(it, i)
					putVal(out, name, val)
					outCols = appendUnique(outCols, name)
				}
				outRows = append(outRows, out)
			}
		}
		// DISTINCT
		if s.Distinct {
			outRows = distinctRows(outRows, outCols)
		}
		// ORDER BY
		if len(s.OrderBy) > 0 {
			sort.SliceStable(outRows, func(i, j int) bool {
				a := outRows[i]
				b := outRows[j]
				for _, oi := range s.OrderBy {
					av, _ := getVal(a, oi.Col)
					bv, _ := getVal(b, oi.Col)
					cmp := compareForOrder(av, bv, oi.Desc)
					if cmp == 0 {
						continue
					}
					if oi.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
				return false
			})
		}
		// OFFSET/LIMIT
		start := 0
		if s.Offset != nil && *s.Offset > 0 {
			start = *s.Offset
		}
		if start > len(outRows) {
			outRows = []Row{}
		} else {
			outRows = outRows[start:]
		}
		if s.Limit != nil && *s.Limit < len(outRows) {
			outRows = outRows[:*s.Limit]
		}
		if len(outCols) == 0 {
			outCols = columnsFromRows(outRows)
		}
		return &ResultSet{Cols: outCols, Rows: outRows}, nil
	}
	return nil, fmt.Errorf("unknown statement")
}

// -------------------- Eval, Aggregates, Helpers --------------------

func getVal(row Row, name string) (any, bool) { v, ok := row[strings.ToLower(name)]; return v, ok }
func putVal(row Row, key string, val any)     { row[strings.ToLower(key)] = val }
func isNull(v any) bool                       { return v == nil }
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}

// tri-state
const (
	tvFalse   = 0
	tvTrue    = 1
	tvUnknown = 2
)

func toTri(v any) int {
	if v == nil {
		return tvUnknown
	}
	if truthy(v) {
		return tvTrue
	}
	return tvFalse
}
func triNot(t int) int {
	if t == tvTrue {
		return tvFalse
	}
	if t == tvFalse {
		return tvTrue
	}
	return tvUnknown
}
func triAnd(a, b int) int {
	if a == tvFalse || b == tvFalse {
		return tvFalse
	}
	if a == tvTrue && b == tvTrue {
		return tvTrue
	}
	return tvUnknown
}
func triOr(a, b int) int {
	if a == tvTrue || b == tvTrue {
		return tvTrue
	}
	if a == tvFalse && b == tvFalse {
		return tvFalse
	}
	return tvUnknown
}

func compare(a, b any) (int, error) {
	if a == nil || b == nil {
		return 0, errors.New("cannot compare with NULL")
	}
	switch ax := a.(type) {
	case int:
		if f, ok := numeric(b); ok {
			af := float64(ax)
			if af < f {
				return -1, nil
			}
			if af > f {
				return 1, nil
			}
			return 0, nil
		}
	case float64:
		if f, ok := numeric(b); ok {
			if ax < f {
				return -1, nil
			}
			if ax > f {
				return 1, nil
			}
			return 0, nil
		}
	case string:
		if bs, ok := b.(string); ok {
			if ax < bs {
				return -1, nil
			}
			if ax > bs {
				return 1, nil
			}
			return 0, nil
		}
	case bool:
		if bb, ok := b.(bool); ok {
			if !ax && bb {
				return -1, nil
			}
			if ax && !bb {
				return 1, nil
			}
			return 0, nil
		}
	}
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable %T and %T", a, b)
}
func compareForOrder(a, b any, desc bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		if desc {
			return -1
		}
		return 1
	}
	if b == nil {
		if desc {
			return 1
		}
		return -1
	}
	c, err := compare(a, b)
	if err != nil {
		return 0
	}
	return c
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func evalExpr(env ExecEnv, e Expr, row Row) (any, error) {
	if err := checkCtx(env.ctx); err != nil {
		return nil, err
	}
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		if v, ok := getVal(row, ex.Name); ok {
			return v, nil
		}
		if strings.Contains(ex.Name, ".") {
			return nil, fmt.Errorf("unknown column reference %q", ex.Name)
		}
		if v, ok := getVal(row, ex.Name); ok {
			return v, nil
		}
		return nil, fmt.Errorf("unknown column %q", ex.Name)
	case *IsNull:
		v, err := evalExpr(env, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		is := isNull(v)
		if ex.Negate {
			return !is, nil
		}
		return is, nil
	case *Unary:
		v, err := evalExpr(env, ex.Expr, row)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case "+":
			if f, ok := numeric(v); ok {
				return f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary + non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary - non-numeric")
		case "NOT":
			return triToValue(triNot(toTri(v))), nil
		}
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" {
			lv, err := evalExpr(env, ex.Left, row)
			if err != nil {
				return nil, err
			}
			if ex.Op == "AND" && toTri(lv) == tvFalse {
				return false, nil
			}
			if ex.Op == "OR" && toTri(lv) == tvTrue {
				return true, nil
			}
			rv, err := evalExpr(env, ex.Right, row)
			if err != nil {
				return nil, err
			}
			if ex.Op == "AND" {
				return triToValue(triAnd(toTri(lv), toTri(rv))), nil
			}
			return triToValue(triOr(toTri(lv), toTri(rv))), nil
		}
		lv, err := evalExpr(env, ex.Left, row)
		if err != nil {
			return nil, err
		}
		rv, err := evalExpr(env, ex.Right, row)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case "+", "-", "*", "/":
			if lv == nil || rv == nil {
				return nil, nil
			}
			lf, lok := numeric(lv)
			rf, rok := numeric(rv)
			if !(lok && rok) {
				return nil, fmt.Errorf("%s expects numeric", ex.Op)
			}
			switch ex.Op {
			case "+":
				return lf + rf, nil
			case "-":
				return lf - rf, nil
			case "*":
				return lf * rf, nil
			case "/":
				if rf == 0 {
					return nil, errors.New("division by zero")
				}
				return lf / rf, nil
			}
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			if lv == nil || rv == nil {
				return nil, nil
			}
			cmp, err := compare(lv, rv)
			if err != nil {
				return nil, err
			}
			switch ex.Op {
			case "=":
				return cmp == 0, nil
			case "!=", "<>":
				return cmp != 0, nil
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">":
				return cmp > 0, nil
			case ">=":
				return cmp >= 0, nil
			}
		}
	case *FuncCall:
		switch ex.Name {
		case "COALESCE":
			for _, a := range ex.Args {
				v, err := evalExpr(env, a, row)
				if err != nil {
					return nil, err
				}
				if v != nil {
					return v, nil
				}
			}
			return nil, nil
		case "NULLIF":
			if len(ex.Args) != 2 {
				return nil, fmt.Errorf("NULLIF expects 2 args")
			}
			lv, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			rv, err := evalExpr(env, ex.Args[1], row)
			if err != nil {
				return nil, err
			}
			if lv == nil {
				return nil, nil
			}
			if rv == nil {
				return lv, nil
			}
			cmp, err := compare(lv, rv)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return nil, nil
			}
			return lv, nil
		case "JSON_GET":
			if len(ex.Args) != 2 {
				return nil, fmt.Errorf("JSON_GET expects (json, path)")
			}
			jv, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			pv, err := evalExpr(env, ex.Args[1], row)
			if err != nil {
				return nil, err
			}
			ps, _ := pv.(string)
			return jsonGet(jv, ps), nil
		case "COUNT":
			if ex.Star {
				return 1, nil
			}
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("COUNT expects 1 arg")
			}
			v, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			if v == nil {
				return 0, nil
			}
			return 1, nil
		case "SUM", "AVG", "MIN", "MAX":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
			}
			v, err := evalExpr(env, ex.Args[0], row)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}
	return nil, fmt.Errorf("unknown expression")
}
func triToValue(t int) any {
	if t == tvTrue {
		return true
	}
	if t == tvFalse {
		return false
	}
	return nil
}

func isAggregate(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
	case *Unary:
		return isAggregate(ex.Expr)
	case *Binary:
		return isAggregate(ex.Left) || isAggregate(ex.Right)
	case *IsNull:
		return isAggregate(ex.Expr)
	}
	return false
}

func evalAggregate(env ExecEnv, e Expr, rows []Row) (any, error) {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT":
			if ex.Star {
				return len(rows), nil
			}
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("COUNT expects 1 arg")
			}
			cnt := 0
			for _, r := range rows {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if v != nil {
					cnt++
				}
			}
			return cnt, nil
		case "SUM", "AVG":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
			}
			sum := 0.0
			n := 0
			for _, r := range rows {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if f, ok := numeric(v); ok {
					sum += f
					n++
				}
			}
			if ex.Name == "SUM" {
				return sum, nil
			}
			if n == 0 {
				return nil, nil
			}
			return sum / float64(n), nil
		case "MIN", "MAX":
			if len(ex.Args) != 1 {
				return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
			}
			var have bool
			var best any
			for _, r := range rows {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
				v, err := evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
				if v == nil {
					continue
				}
				if !have {
					best = v
					have = true
				} else {
					cmp, err := compare(v, best)
					if err == nil {
						if ex.Name == "MIN" && cmp < 0 {
							best = v
						}
						if ex.Name == "MAX" && cmp > 0 {
							best = v
						}
					}
				}
			}
			if !have {
				return nil, nil
			}
			return best, nil
		}
	case *Unary:
		v, err := evalAggregate(env, ex.Expr, rows)
		if err != nil {
			return nil, err
		}
		switch ex.Op {
		case "+":
			if f, ok := numeric(v); ok {
				return f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary + non-numeric")
		case "-":
			if f, ok := numeric(v); ok {
				return -f, nil
			}
			if v == nil {
				return nil, nil
			}
			return nil, fmt.Errorf("unary - non-numeric")
		case "NOT":
			return triToValue(triNot(toTri(v))), nil
		}
	case *Binary:
		lv, err := evalAggregate(env, ex.Left, rows)
		if err != nil {
			return nil, err
		}
		rv, err := evalAggregate(env, ex.Right, rows)
		if err != nil {
			return nil, err
		}
		return evalExpr(env, &Binary{Op: ex.Op, Left: &Literal{Val: lv}, Right: &Literal{Val: rv}}, Row{})
	case *IsNull:
		v, err := evalAggregate(env, ex.Expr, rows)
		if err != nil {
			return nil, err
		}
		if ex.Negate {
			return !isNull(v), nil
		}
		return isNull(v), nil
	default:
		if len(rows) == 0 {
			return nil, nil
		}
		return evalExpr(env, e, rows[0])
	}
	return nil, fmt.Errorf("unsupported aggregate")
}

func rowsFromTable(t *storage.Table, alias string) ([]Row, []string) {
	cols := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		cols[i] = strings.ToLower(alias + "." + c.Name)
	}
	var out []Row
	for _, r := range t.Rows {
		row := Row{}
		for i, c := range t.Cols {
			putVal(row, alias+"."+c.Name, r[i])
		}
		for i, c := range t.Cols {
			if _, exists := row[strings.ToLower(c.Name)]; !exists {
				putVal(row, c.Name, r[i])
			}
		}
		out = append(out, row)
	}
	return out, cols
}

func keysOfRow(r Row) []string {
	var ks []string
	for k := range r {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func aliasOr(f FromItem) string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Table
}

func mergeRows(l, r Row) Row {
	m := make(Row, len(l)+len(r))
	for k, v := range l {
		m[k] = v
	}
	for k, v := range r {
		m[k] = v
	}
	return m
}
func cloneRow(r Row) Row {
	m := make(Row, len(r))
	for k, v := range r {
		m[k] = v
	}
	return m
}
func addRightNulls(m Row, alias string, t *storage.Table) {
	for _, c := range t.Cols {
		putVal(m, alias+"."+c.Name, nil)
		if _, ex := m[strings.ToLower(c.Name)]; !ex {
			putVal(m, c.Name, nil)
		}
	}
}

func fmtKeyPart(v any) string {
	switch x := v.(type) {
	case nil:
		return "N:"
	case int:
		return "I:" + strconv.Itoa(x)
	case float64:
		return "F:" + strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "B:1"
		}
		return "B:0"
	case string:
		return "S:" + x
	default:
		b, _ := json.Marshal(x)
		return "J:" + string(b)
	}
}
func distinctRows(rows []Row, cols []string) []Row {
	seen := map[string]bool{}
	var out []Row
	for _, r := range rows {
		var parts []string
		for _, c := range cols {
			parts = append(parts, fmtKeyPart(r[strings.ToLower(c)]))
		}
		key := strings.Join(parts, "|")
		if !seen[key] {
			seen[key] = true
			out = append(out, r)
		}
	}
	return out
}

func inferType(v any) storage.ColType {
	switch v.(type) {
	case int, int64:
		return storage.IntType
	case float64:
		return storage.FloatType
	case bool:
		return storage.BoolType
	case string:
		return storage.TextType
	default:
		return storage.JsonType
	}
}
func coerceToTypeAllowNull(v any, t storage.ColType) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case storage.IntType:
		switch x := v.(type) {
		case int:
			return x, nil
		case int64:
			return int(x), nil
		case float64:
			return int(x), nil
		case string:
			n, err := strconv.Atoi(strings.TrimSpace(x))
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to INT", x)
			}
			return n, nil
		case bool:
			if x {
				return 1, nil
			}
			return 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to INT", v)
		}
	case storage.FloatType:
		switch x := v.(type) {
		case int:
			return float64(x), nil
		case int64:
			return float64(x), nil
		case float64:
			return x, nil
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to FLOAT", x)
			}
			return f, nil
		case bool:
			if x {
				return 1.0, nil
			}
			return 0.0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to FLOAT", v)
		}
	case storage.TextType:
		return fmt.Sprintf("%v", v), nil
	case storage.BoolType:
		switch x := v.(type) {
		case bool:
			return x, nil
		case int, int64:
			return x != 0, nil
		case float64:
			return x != 0, nil
		case string:
			s := strings.ToLower(strings.TrimSpace(x))
			return s == "true" || s == "1" || s == "t" || s == "yes", nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BOOL", v)
		}
	case storage.JsonType:
		switch x := v.(type) {
		case string:
			var anyv any
			if json.Unmarshal([]byte(x), &anyv) == nil {
				return anyv, nil
			}
			return x, nil
		default:
			return x, nil
		}
	default:
		return v, nil
	}
}

// JSON helpers

type pathPart struct {
	key string
	idx int
}

func parseJSONPath(s string) []pathPart {
	var out []pathPart
	cur := ""
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
		case '[':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
			j := i + 1
			for j < len(s) && s[j] != ']' {
				j++
			}
			if j <= len(s)-1 {
				n, _ := strconv.Atoi(s[i+1 : j])
				out = append(out, pathPart{idx: n})
				i = j
			}
		default:
			cur += string(s[i])
		}
	}
	if cur != "" {
		out = append(out, pathPart{key: cur, idx: -1})
	}
	return out
}
func jsonGet(v any, path string) any {
	if v == nil || path == "" {
		return nil
	}
	parts := parseJSONPath(path)
	cur := v
	for _, p := range parts {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[p.key]
		case []any:
			if p.idx >= 0 && p.idx < len(c) {
				cur = c[p.idx]
			} else {
				return nil
			}
		default:
			return nil
		}
	}
	return cur
}

// -------------------- misc helpers --------------------

func columnsFromRows(rows []Row) []string {
	seen := map[string]bool{}
	var cols []string
	for _, r := range rows {
		for k := range r {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	sort.Strings(cols)
	return cols
}

// Helper functions for projName, anyAggInSelect, and appendUnique

// projName generates the column name for a select item
func projName(it SelectItem, idx int) string {
	if it.Alias != "" {
		return it.Alias
	}
	if ref, ok := it.Expr.(*VarRef); ok {
		return ref.Name
	}
	return fmt.Sprintf("col_%d", idx)
}

// anyAggInSelect checks if any select item contains an aggregate function
func anyAggInSelect(items []SelectItem) bool {
	for _, it := range items {
		if isAggregate(it.Expr) {
			return true
		}
	}
	return false
}

// appendUnique appends a column name to the slice if it's not already present
func appendUnique(cols []string, c string) []string {
	for _, existing := range cols {
		if existing == c {
			return cols
		}
	}
	return append(cols, c)
}

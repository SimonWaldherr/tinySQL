package tinysql

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// ExprBuilder is an interface for building SQL expressions.
type ExprBuilder interface {
	Build() engine.Expr
}

// exprWrapper wraps an engine.Expr to implement ExprBuilder.
type exprWrapper struct {
	expr engine.Expr
}

func (e exprWrapper) Build() engine.Expr { return e.expr }

// Col creates a column reference expression.
//
// Example:
//
//	Col("users.id")
//	Col("name")
func Col(name string) ExprBuilder {
	return exprWrapper{&engine.VarRef{Name: name, Lower: strings.ToLower(name)}}
}

// Val creates a literal value expression.
//
// Example:
//
//	Val(42)
//	Val("hello")
//	Val(true)
func Val(value any) ExprBuilder {
	return exprWrapper{&engine.Literal{Val: value}}
}

// Null creates a NULL literal.
func Null() ExprBuilder {
	return exprWrapper{&engine.Literal{Val: nil}}
}

// Eq creates an equality comparison (=).
func Eq(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "=",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Ne creates a not-equal comparison (<>).
func Ne(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "<>",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Lt creates a less-than comparison (<).
func Lt(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "<",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Le creates a less-than-or-equal comparison (<=).
func Le(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "<=",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Gt creates a greater-than comparison (>).
func Gt(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    ">",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Ge creates a greater-than-or-equal comparison (>=).
func Ge(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    ">=",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// And creates a logical AND expression.
func And(exprs ...ExprBuilder) ExprBuilder {
	if len(exprs) == 0 {
		return Val(true)
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	result := exprs[0].Build()
	for i := 1; i < len(exprs); i++ {
		result = &engine.Binary{
			Op:    "AND",
			Left:  result,
			Right: exprs[i].Build(),
		}
	}
	return exprWrapper{result}
}

// Or creates a logical OR expression.
func Or(exprs ...ExprBuilder) ExprBuilder {
	if len(exprs) == 0 {
		return Val(false)
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	result := exprs[0].Build()
	for i := 1; i < len(exprs); i++ {
		result = &engine.Binary{
			Op:    "OR",
			Left:  result,
			Right: exprs[i].Build(),
		}
	}
	return exprWrapper{result}
}

// Not creates a logical NOT expression.
func Not(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Unary{
		Op:   "NOT",
		Expr: expr.Build(),
	}}
}

// IsNull creates an IS NULL expression.
func IsNull(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.IsNull{
		Expr:   expr.Build(),
		Negate: false,
	}}
}

// IsNotNull creates an IS NOT NULL expression.
func IsNotNull(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.IsNull{
		Expr:   expr.Build(),
		Negate: true,
	}}
}

// Add creates an addition expression (+).
func Add(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "+",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Sub creates a subtraction expression (-).
func Sub(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "-",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Mul creates a multiplication expression (*).
func Mul(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "*",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Div creates a division expression (/).
func Div(left, right ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.Binary{
		Op:    "/",
		Left:  left.Build(),
		Right: right.Build(),
	}}
}

// Count creates a COUNT aggregate function.
func Count(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "COUNT",
		Args: []engine.Expr{expr.Build()},
	}}
}

// CountStar creates a COUNT(*) aggregate.
func CountStar() ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "COUNT",
		Args: []engine.Expr{&engine.Literal{Val: "*"}},
	}}
}

// Sum creates a SUM aggregate function.
func Sum(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SUM",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Avg creates an AVG aggregate function.
func Avg(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "AVG",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Min creates a MIN aggregate function.
func Min(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "MIN",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Max creates a MAX aggregate function.
func Max(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "MAX",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Upper creates an UPPER function call.
func Upper(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "UPPER",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Lower creates a LOWER function call.
func Lower(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "LOWER",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Concat creates a CONCAT function call.
func Concat(exprs ...ExprBuilder) ExprBuilder {
	args := make([]engine.Expr, len(exprs))
	for i, e := range exprs {
		args[i] = e.Build()
	}
	return exprWrapper{&engine.FuncCall{
		Name: "CONCAT",
		Args: args,
	}}
}

// Trim creates a TRIM function call.
func Trim(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "TRIM",
		Args: []engine.Expr{expr.Build()},
	}}
}

// MD5 creates an MD5 hash function call.
func MD5(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "MD5",
		Args: []engine.Expr{expr.Build()},
	}}
}

// SHA1 creates a SHA1 hash function call.
func SHA1(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SHA1",
		Args: []engine.Expr{expr.Build()},
	}}
}

// SHA256 creates a SHA256 hash function call.
func SHA256(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SHA256",
		Args: []engine.Expr{expr.Build()},
	}}
}

// SHA512 creates a SHA512 hash function call.
func SHA512(expr ExprBuilder) ExprBuilder {
	return exprWrapper{&engine.FuncCall{
		Name: "SHA512",
		Args: []engine.Expr{expr.Build()},
	}}
}

// Coalesce creates a COALESCE function call.
func Coalesce(exprs ...ExprBuilder) ExprBuilder {
	args := make([]engine.Expr, len(exprs))
	for i, e := range exprs {
		args[i] = e.Build()
	}
	return exprWrapper{&engine.FuncCall{
		Name: "COALESCE",
		Args: args,
	}}
}

// Exists creates an EXISTS predicate for a subquery.
func Exists(query *SelectBuilder) ExprBuilder {
	return exprWrapper{&engine.ExistsExpr{Select: query.Build()}}
}

// NotExists creates a NOT EXISTS predicate for a subquery.
func NotExists(query *SelectBuilder) ExprBuilder {
	return Not(Exists(query))
}

// Package engine provides a hand-written SQL parser for tinySQL.
//
// What: It parses a practical subset of SQL into an AST (statements and
// expressions) used by the execution engine. Supported features include DDL,
// DML, SELECT with JOIN/GROUP/HAVING/ORDER/LIMIT/OFFSET, and set ops.
// How: A straightforward recursive-descent parser over a small token stream
// from the lexer. It favors clarity and precise error messages. Ident-like
// parsing accepts keywords as identifiers to keep the grammar practical for
// common column names.
// Why: A small, readable parser is easy to extend and reason about, enabling
// rapid iteration on language features without a complex generator toolchain.
package engine

import (
	"fmt"
	"strings"
)

// Parser holds the lexer and current/peek tokens for recursive-descent parsing.
type Parser struct {
	// Keep the lexer in the parser allocation. A parser owns exactly one lexer,
	// so a separate heap object only adds allocation and pointer chasing.
	lx   lexer
	cur  token
	peek token
	// depth tracks combined expression/subquery recursion nesting. Without
	// a limit, a maliciously deep input like "((((((...1...))))))" or
	// nested "(SELECT * FROM (SELECT * FROM (SELECT ...)))" recurses once
	// per nesting level through the whole precedence chain — enough levels
	// exhausts the goroutine stack. That failure mode is a Go runtime fatal
	// error ("stack overflow"), not a normal panic: it cannot be caught by
	// recover(), so unlike other engine bugs it would kill the whole
	// process outright, not just fail the one query. maxParseDepth keeps
	// it a plain, recoverable parse error instead.
	depth int
}

// maxParseDepth bounds parseExpr/parseSelect recursion. 200 comfortably
// covers any realistic hand-written or generated query while stopping
// pathological nesting well before it threatens the goroutine stack.
const maxParseDepth = 200

func (p *Parser) enterRecursion() error {
	p.depth++
	if p.depth > maxParseDepth {
		return p.errf("expression or subquery nested too deeply (limit %d)", maxParseDepth)
	}
	return nil
}

func (p *Parser) exitRecursion() {
	p.depth--
}

// NewParser creates a new SQL parser for the provided input string.
func NewParser(sql string) *Parser {
	p := &Parser{lx: lexer{s: sql}}
	p.cur = p.lx.nextToken()
	p.peek = p.lx.nextToken()
	return p
}

func (p *Parser) next() { p.cur, p.peek = p.peek, p.lx.nextToken() }

func (p *Parser) expectSymbol(sym string) error {
	if p.cur.Typ == tSymbol && p.cur.Val == sym {
		p.next()
		return nil
	}
	return p.errf("expected symbol %q", sym)
}

func (p *Parser) expectKeyword(kw string) error {
	if p.cur.Typ == tKeyword && p.cur.Val == kw {
		p.next()
		return nil
	}
	return p.errf("expected keyword %q", kw)
}

func (p *Parser) errf(format string, a ...any) error {
	return fmt.Errorf("parse error near %q: %s", p.cur.Val, fmt.Sprintf(format, a...))
}

func (p *Parser) parseBareTableSelect() (*Select, error) {
	table := p.parseQualifiedIdentLike()
	if table == "" {
		return nil, p.errf("expected table name")
	}
	return &Select{
		From:  FromItem{Table: table, Alias: table},
		Projs: []SelectItem{{Star: true}},
	}, nil
}

func newVarRef(name string) *VarRef {
	return &VarRef{Name: name, Lower: strings.ToLower(name)}
}

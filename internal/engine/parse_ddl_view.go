// Parsing CREATE, DROP and REFRESH for views and materialized views, including
// a materialized view's refresh schedule and staleness options.
package engine

import (
	"strconv"
	"strings"
	"time"
)

func (p *Parser) parseCreateView() (Statement, error) {
	// Already consumed CREATE, check for OR REPLACE
	orReplace := false
	if p.cur.Typ == tKeyword && p.cur.Val == "OR" {
		p.next()
		if err := p.expectKeyword("REPLACE"); err != nil {
			return nil, err
		}
		orReplace = true
	}

	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

	// Check for IF NOT EXISTS
	ifNotExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	viewName := p.parseIdentLike()
	if viewName == "" {
		return nil, p.errf("expected view name")
	}

	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	queryStart := p.cur.Pos
	sel, err := p.parseSelectWithCTE()
	if err != nil {
		return nil, err
	}
	sqlText := p.sqlFragment(queryStart, p.cur.Pos)

	return &CreateView{
		Name:        viewName,
		Select:      sel,
		SQLText:     sqlText,
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
	}, nil
}

func (p *Parser) parseCreateMaterializedView(orReplace bool) (Statement, error) {
	p.next() // consume MATERIALIZED
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

	ifNotExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifNotExists = true
	}

	viewName := p.parseIdentLike()
	if viewName == "" {
		return nil, p.errf("expected materialized view name")
	}
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}

	queryStart := p.cur.Pos
	sel, err := p.parseSelectWithCTE()
	if err != nil {
		return nil, err
	}
	mv := &CreateMaterializedView{
		Name:        viewName,
		Select:      sel,
		SQLText:     p.sqlFragment(queryStart, p.cur.Pos),
		IfNotExists: ifNotExists,
		OrReplace:   orReplace,
		WithData:    true,
	}

	for p.cur.Typ == tKeyword {
		switch p.cur.Val {
		case "REFRESH":
			if err := p.parseMaterializedRefreshClause(mv); err != nil {
				return nil, err
			}
		case "WITH":
			if err := p.parseMaterializedWithData(mv); err != nil {
				return nil, err
			}
		case "INVALIDATE":
			if err := p.parseMaterializedInvalidateClause(mv); err != nil {
				return nil, err
			}
		default:
			return mv, nil
		}
	}
	return mv, nil
}

func (p *Parser) parseMaterializedRefreshClause(mv *CreateMaterializedView) error {
	p.next() // consume REFRESH
	if p.cur.Typ == tKeyword && p.cur.Val == "ON" {
		p.next()
		if p.cur.Typ == tKeyword && p.cur.Val == "STALE" {
			p.next()
			if err := p.expectKeyword("AFTER"); err != nil {
				return err
			}
			ms, err := p.parseDurationMillis()
			if err != nil {
				return err
			}
			mv.StaleAfterMs = ms
			return nil
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "DEMAND" {
			p.next()
			return nil
		}
		return p.errf("expected STALE or DEMAND after REFRESH ON")
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "EVERY" {
		p.next()
		ms, err := p.parseDurationMillis()
		if err != nil {
			return err
		}
		mv.RefreshEveryMs = ms
		return nil
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "DAILY" {
		p.next()
		if err := p.expectKeyword("AT"); err != nil {
			return err
		}
		if p.cur.Typ != tString {
			return p.errf("expected daily refresh time string")
		}
		mv.DailyAt = p.cur.Val
		p.next()
		if p.cur.Typ == tKeyword && p.cur.Val == "TIMEZONE" {
			p.next()
			if p.cur.Typ != tString {
				return p.errf("expected timezone string")
			}
			mv.Timezone = p.cur.Val
			p.next()
		}
		return nil
	}
	return p.errf("expected ON, EVERY, or DAILY after REFRESH")
}

func (p *Parser) parseMaterializedWithData(mv *CreateMaterializedView) error {
	p.next() // consume WITH
	if p.cur.Typ == tKeyword && p.cur.Val == "NO" {
		p.next()
		if err := p.expectKeyword("DATA"); err != nil {
			return err
		}
		mv.WithData = false
		return nil
	}
	if err := p.expectKeyword("DATA"); err != nil {
		return err
	}
	mv.WithData = true
	return nil
}

func (p *Parser) parseMaterializedInvalidateClause(mv *CreateMaterializedView) error {
	p.next() // consume INVALIDATE
	if err := p.expectKeyword("ON"); err != nil {
		return err
	}
	if err := p.expectKeyword("CHANGE"); err != nil {
		return err
	}
	mv.InvalidateOnChange = true
	return nil
}

func (p *Parser) parseDurationMillis() (int64, error) {
	if p.cur.Typ != tNumber {
		return 0, p.errf("expected duration number")
	}
	n, err := strconv.ParseFloat(p.cur.Val, 64)
	if err != nil {
		return 0, p.errf("invalid duration number")
	}
	p.next()
	unit := p.parseIdentLike()
	if unit == "" {
		return 0, p.errf("expected duration unit")
	}
	switch strings.ToUpper(unit) {
	case "MILLISECOND", "MILLISECONDS", "MS":
		return int64(n), nil
	case "SECOND", "SECONDS":
		return int64(n * float64(time.Second/time.Millisecond)), nil
	case "MINUTE", "MINUTES":
		return int64(n * float64(time.Minute/time.Millisecond)), nil
	case "HOUR", "HOURS":
		return int64(n * float64(time.Hour/time.Millisecond)), nil
	case "DAY", "DAYS":
		return int64(n * float64((24*time.Hour)/time.Millisecond)), nil
	default:
		return 0, p.errf("unknown duration unit %q", unit)
	}
}

func (p *Parser) sqlFragment(start, end int) string {
	if start < 0 {
		start = 0
	}
	if end < start || end > len(p.lx.s) {
		end = len(p.lx.s)
	}
	return strings.TrimSpace(p.lx.s[start:end])
}

func (p *Parser) parseDropView() (Statement, error) {
	// Already consumed DROP VIEW
	p.next()

	// Check for IF EXISTS
	ifExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	viewName := p.parseIdentLike()
	if viewName == "" {
		return nil, p.errf("expected view name")
	}

	return &DropView{
		Name:     viewName,
		IfExists: ifExists,
	}, nil
}

func (p *Parser) parseDropMaterializedView() (Statement, error) {
	p.next() // consume MATERIALIZED
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}

	ifExists := false
	if p.cur.Typ == tKeyword && p.cur.Val == "IF" {
		p.next()
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
		ifExists = true
	}

	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected materialized view name")
	}
	return &DropMaterializedView{Name: name, IfExists: ifExists}, nil
}

func (p *Parser) parseRefresh() (Statement, error) {
	p.next() // consume REFRESH
	if err := p.expectKeyword("MATERIALIZED"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	concurrently := false
	if p.cur.Typ == tKeyword && p.cur.Val == "CONCURRENTLY" {
		concurrently = true
		p.next()
	}
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected materialized view name")
	}
	return &RefreshMaterializedView{Name: name, Concurrently: concurrently}, nil
}

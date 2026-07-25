// Parsing ALTER, for tables, views and materialized views.
package engine

import (
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func (p *Parser) parseAlter() (Statement, error) {
	p.next()

	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		return p.parseAlterView()
	}

	if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
		return p.parseAlterMaterializedView()
	}

	if p.cur.Typ == tKeyword && p.cur.Val == "USER" {
		return p.parseAlterUser()
	}

	// Support ALTER JOB <name> ENABLE|DISABLE
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "JOB" {
		p.next()
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected job name")
		}
		if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && (p.cur.Val == "ENABLE" || p.cur.Val == "DISABLE") {
			enable := p.cur.Val == "ENABLE"
			p.next()
			return &AlterJob{Name: name, Enable: &enable}, nil
		}
		return nil, p.errf("expected ENABLE or DISABLE after JOB name")
	}

	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	tableName := p.parseIdentLike()
	if tableName == "" {
		return nil, p.errf("expected table name")
	}

	if err := p.expectKeyword("ADD"); err != nil {
		return nil, err
	}

	// Optional COLUMN keyword
	if p.cur.Typ == tKeyword && p.cur.Val == "COLUMN" {
		p.next()
	}

	// Parse column definition
	colName := p.parseIdentLike()
	if colName == "" {
		return nil, p.errf("expected column name")
	}

	colType, err := p.parseColumnType()
	if err != nil {
		return nil, p.errf("unknown column type")
	}

	col := storage.Column{
		Name:         colName,
		Type:         colType.typ,
		DeclaredType: colType.declared,
		Affinity:     colType.affinity,
	}

	return &AlterTable{
		Table:     tableName,
		AddColumn: &col,
	}, nil
}

func (p *Parser) parseAlterView() (Statement, error) {
	p.next() // consume VIEW
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected view name")
	}
	if (p.cur.Typ != tKeyword && p.cur.Typ != tIdent) || p.cur.Val != "MATERIALIZE" {
		return nil, p.errf("expected MATERIALIZE")
	}
	p.next()
	mv := &CreateMaterializedView{Name: name, WithData: true}
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
			return &AlterViewMaterialize{
				Name:               name,
				WithData:           mv.WithData,
				StaleAfterMs:       mv.StaleAfterMs,
				RefreshEveryMs:     mv.RefreshEveryMs,
				DailyAt:            mv.DailyAt,
				Timezone:           mv.Timezone,
				InvalidateOnChange: mv.InvalidateOnChange,
			}, nil
		}
	}
	return &AlterViewMaterialize{
		Name:               name,
		WithData:           mv.WithData,
		StaleAfterMs:       mv.StaleAfterMs,
		RefreshEveryMs:     mv.RefreshEveryMs,
		DailyAt:            mv.DailyAt,
		Timezone:           mv.Timezone,
		InvalidateOnChange: mv.InvalidateOnChange,
	}, nil
}

func (p *Parser) parseAlterMaterializedView() (Statement, error) {
	p.next() // consume MATERIALIZED
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected materialized view name")
	}
	if err := p.expectKeyword("TO"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("VIEW"); err != nil {
		return nil, err
	}
	return &AlterMaterializedViewToView{Name: name}, nil
}

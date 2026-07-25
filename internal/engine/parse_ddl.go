// Parsing CREATE and DROP for tables, triggers, virtual tables, indexes and
// scheduled jobs.
package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (p *Parser) parseCreate() (Statement, error) {
	p.next()

	stmt, handled, err := p.parseCreateNonTable()
	if err != nil || handled {
		return stmt, err
	}

	return p.parseCreateTable()
}

func (p *Parser) parseCreateNonTable() (Statement, bool, error) {
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "JOB" {
		stmt, err := p.parseCreateJob()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && (p.cur.Val == "USER" || p.cur.Val == "ROLE") {
		stmt, err := p.parseCreateUserOrRole()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "TRIGGER" {
		stmt, err := p.parseCreateTrigger(false)
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "OR" {
		stmt, err := p.parseCreateOrReplace()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
		stmt, err := p.parseCreateMaterializedView(false)
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && (p.cur.Val == "INDEX" || p.cur.Val == "UNIQUE") {
		stmt, err := p.parseCreateIndex()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		stmt, err := p.parseCreateView()
		return stmt, true, err
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "VIRTUAL" {
		stmt, err := p.parseCreateVirtualTable()
		return stmt, true, err
	}
	return nil, false, nil
}

func (p *Parser) parseCreateOrReplace() (Statement, error) {
	p.next() // consume OR
	if p.cur.Typ == tKeyword && p.cur.Val == "REPLACE" {
		p.next() // consume REPLACE
		if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
			return p.parseCreateMaterializedView(true)
		}
		if p.cur.Typ == tKeyword && p.cur.Val == "TRIGGER" {
			return p.parseCreateTrigger(false)
		}
		return p.parseCreateView()
	}
	return p.parseCreateView()
}

func (p *Parser) parseCreateVirtualTable() (Statement, error) {
	p.next()
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	return p.parseVirtualTable()
}

func (p *Parser) parseCreateTable() (Statement, error) {
	isTemp := false
	if p.cur.Typ == tKeyword && p.cur.Val == "TEMP" {
		isTemp = true
		p.next()
	}
	if err := p.expectKeyword("TABLE"); err != nil {
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

	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected table name")
	}
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		cols, err := p.parseColumnDefs()
		if err != nil {
			return nil, err
		}
		return &CreateTable{Name: name, Cols: cols, IsTemp: isTemp, IfNotExists: ifNotExists}, nil
	}
	if p.cur.Typ == tKeyword && p.cur.Val == "AS" {
		p.next()
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &CreateTable{Name: name, IsTemp: isTemp, AsSelect: sel, IfNotExists: ifNotExists}, nil
	}
	return nil, p.errf("expected '(' or AS SELECT")
}

func (p *Parser) parseDrop() (Statement, error) {
	p.next()

	// Check for DROP MATERIALIZED VIEW
	if p.cur.Typ == tKeyword && p.cur.Val == "MATERIALIZED" {
		return p.parseDropMaterializedView()
	}

	// Check for DROP INDEX
	if p.cur.Typ == tKeyword && p.cur.Val == "INDEX" {
		return p.parseDropIndex()
	}

	// Check for DROP VIEW
	if p.cur.Typ == tKeyword && p.cur.Val == "VIEW" {
		return p.parseDropView()
	}

	// Check for DROP TRIGGER
	if p.cur.Typ == tKeyword && p.cur.Val == "TRIGGER" {
		return p.parseDropTrigger()
	}

	// Check for DROP JOB
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "JOB" {
		p.next()
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected job name")
		}
		return &DropJob{Name: name}, nil
	}

	// Check for DROP USER / DROP ROLE
	if p.cur.Typ == tKeyword && (p.cur.Val == "USER" || p.cur.Val == "ROLE") {
		return p.parseDropUserOrRole()
	}

	// DROP TABLE
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}

	// Check for IF EXISTS
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
		return nil, p.errf("expected table name")
	}
	return &DropTable{Name: name, IfExists: ifExists}, nil
}

// parseCreateTrigger parses CREATE [OR REPLACE] TRIGGER ...
// Syntax: CREATE TRIGGER [IF NOT EXISTS] name BEFORE|AFTER|INSTEAD OF INSERT|UPDATE|DELETE
//
//	ON table [FOR EACH ROW] [WHEN (expr)] BEGIN stmt; ... END
func (p *Parser) parseCreateTrigger(orReplace bool) (Statement, error) {
	p.next() // consume TRIGGER

	ifNotExists, err := p.parseTriggerIfNotExists()
	if err != nil {
		return nil, err
	}

	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected trigger name")
	}

	timing, err := p.parseTriggerTiming()
	if err != nil {
		return nil, err
	}

	event, err := p.parseTriggerEvent()
	if err != nil {
		return nil, err
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	table := p.parseIdentLike()
	if table == "" {
		return nil, p.errf("expected table name in trigger")
	}

	forEachRow, err := p.parseTriggerForEachRow()
	if err != nil {
		return nil, err
	}

	whenExpr, whenText, err := p.parseTriggerWhen()
	if err != nil {
		return nil, err
	}

	if err := p.expectKeyword("BEGIN"); err != nil {
		return nil, err
	}

	body, bodyText, err := p.parseTriggerBody()
	if err != nil {
		return nil, err
	}

	return &CreateTrigger{
		Name:        name,
		Timing:      timing,
		Event:       event,
		Table:       table,
		ForEachRow:  forEachRow,
		WhenExpr:    whenExpr,
		WhenText:    whenText,
		Body:        body,
		BodyText:    bodyText,
		IfNotExists: ifNotExists,
	}, nil
}

func (p *Parser) parseTriggerIfNotExists() (bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "IF" {
		return false, nil
	}
	p.next()
	if err := p.expectKeyword("NOT"); err != nil {
		return false, err
	}
	if err := p.expectKeyword("EXISTS"); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Parser) parseTriggerTiming() (string, error) {
	if p.cur.Typ != tKeyword {
		return "", p.errf("expected BEFORE, AFTER, or INSTEAD OF in trigger")
	}
	switch p.cur.Val {
	case "BEFORE":
		p.next()
		return "BEFORE", nil
	case "AFTER":
		p.next()
		return "AFTER", nil
	case "INSTEAD":
		p.next()
		if err := p.expectKeyword("OF"); err != nil {
			return "", err
		}
		return "INSTEAD OF", nil
	default:
		return "", p.errf("expected BEFORE, AFTER, or INSTEAD OF in trigger")
	}
}

func (p *Parser) parseTriggerEvent() (string, error) {
	if p.cur.Typ != tKeyword {
		return "", p.errf("expected INSERT, UPDATE, or DELETE in trigger")
	}
	switch p.cur.Val {
	case "INSERT", "UPDATE", "DELETE":
		event := p.cur.Val
		p.next()
		return event, nil
	default:
		return "", p.errf("expected INSERT, UPDATE, or DELETE in trigger")
	}
}

func (p *Parser) parseTriggerForEachRow() (bool, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "FOR" {
		return false, nil
	}
	p.next()
	if err := p.expectKeyword("EACH"); err != nil {
		return false, err
	}
	if err := p.expectKeyword("ROW"); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Parser) parseTriggerWhen() (Expr, string, error) {
	if p.cur.Typ != tKeyword || p.cur.Val != "WHEN" {
		return nil, "", nil
	}
	p.next()
	if err := p.expectSymbol("("); err != nil {
		return nil, "", err
	}
	startPos := p.cur.Pos
	whenExpr, err := p.parseExpr()
	if err != nil {
		return nil, "", err
	}
	endPos := p.cur.Pos
	if err := p.expectSymbol(")"); err != nil {
		return nil, "", err
	}
	return whenExpr, strings.TrimSpace(p.lx.s[startPos:endPos]), nil
}

// parseTriggerBody parses the statements between BEGIN and END, validating
// their syntax immediately (so a malformed trigger body fails at CREATE
// TRIGGER time rather than silently on every future fire). It also returns
// the verbatim source text of the body, sliced out of the original SQL by
// byte offset — this, not a reprint of the parsed AST, is what gets stored
// for re-parsing each time the trigger fires. An AST-to-SQL printer would
// need to precisely reconstruct every expression (including qualified names
// like NEW.col); capturing the original text sidesteps that entirely and
// can't drift from what the user actually wrote.
func (p *Parser) parseTriggerBody() ([]Statement, string, error) {
	startPos := p.cur.Pos
	var body []Statement
	for (p.cur.Typ != tKeyword || p.cur.Val != "END") && p.cur.Typ != tEOF {
		stmt, err := p.parseStatement()
		if err != nil {
			return nil, "", fmt.Errorf("trigger body: %w", err)
		}
		body = append(body, stmt)
		if p.cur.Typ == tSymbol && p.cur.Val == ";" {
			p.next()
		}
	}
	endPos := p.cur.Pos
	if p.cur.Typ == tEOF {
		return nil, "", p.errf("expected END to close trigger body")
	}
	bodyText := strings.TrimSpace(p.lx.s[startPos:endPos])
	p.next() // consume END
	return body, bodyText, nil
}

// parseDropTrigger parses DROP TRIGGER [IF EXISTS] name.
func (p *Parser) parseDropTrigger() (Statement, error) {
	p.next() // consume TRIGGER

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
		return nil, p.errf("expected trigger name")
	}
	return &DropTrigger{Name: name, IfExists: ifExists}, nil
}

// parseVirtualTable parses CREATE VIRTUAL TABLE name USING fts(col1, col2, ...).
func (p *Parser) parseVirtualTable() (Statement, error) {
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

	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected table name")
	}

	if err := p.expectKeyword("USING"); err != nil {
		return nil, err
	}

	engine := p.parseIdentLike()
	if engine == "" {
		return nil, p.errf("expected virtual table engine name (e.g. fts)")
	}

	var ftsCols []string
	if p.cur.Typ == tSymbol && p.cur.Val == "(" {
		p.next()
		for p.cur.Typ != tSymbol || p.cur.Val != ")" {
			col := p.parseIdentLike()
			if col == "" {
				return nil, p.errf("expected column name in USING clause")
			}
			ftsCols = append(ftsCols, col)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
			}
		}
		p.next() // consume )
	}

	return &CreateTable{
		Name:         name,
		VirtualTable: true,
		Using:        strings.ToLower(engine),
		FTSColumns:   ftsCols,
		IfNotExists:  ifNotExists,
	}, nil
}

//nolint:gocyclo // Index creation grammar includes many optional clauses.
func (p *Parser) parseCreateIndex() (Statement, error) {
	// Already consumed CREATE, cur should be INDEX or UNIQUE
	unique := false
	if p.cur.Typ == tKeyword && p.cur.Val == "UNIQUE" {
		unique = true
		p.next()
		if err := p.expectKeyword("INDEX"); err != nil {
			return nil, err
		}
	} else if err := p.expectKeyword("INDEX"); err != nil {
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

	indexName := p.parseIdentLike()
	if indexName == "" {
		return nil, p.errf("expected index name")
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}

	tableName := p.parseIdentLike()
	if tableName == "" {
		return nil, p.errf("expected table name")
	}

	if err := p.expectSymbol("("); err != nil {
		return nil, err
	}

	var columns []string
	for {
		col := p.parseIdentLike()
		if col == "" {
			return nil, p.errf("expected column name")
		}
		columns = append(columns, col)

		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		break
	}

	return &CreateIndex{
		Name:        indexName,
		Table:       tableName,
		Columns:     columns,
		Unique:      unique,
		IfNotExists: ifNotExists,
	}, nil
}

// parseJobSchedule parses the SCHEDULE clause for CREATE JOB
func (p *Parser) parseJobSchedule(job *CreateJob) error {
	p.next() // consume SCHEDULE

	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "CRON" {
		return p.parseJobScheduleCron(job)
	}
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "INTERVAL" {
		return p.parseJobScheduleInterval(job)
	}
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "ONCE" {
		return p.parseJobScheduleOnce(job)
	}
	return p.errf("expected CRON|INTERVAL|ONCE after SCHEDULE")
}

// parseJobScheduleCron parses SCHEDULE CRON clause
func (p *Parser) parseJobScheduleCron(job *CreateJob) error {
	p.next() // consume CRON
	if p.cur.Typ != tString {
		return p.errf("expected CRON string")
	}
	job.ScheduleType = "CRON"
	job.CronExpr = p.cur.Val
	p.next()
	return nil
}

// parseJobScheduleInterval parses SCHEDULE INTERVAL clause
func (p *Parser) parseJobScheduleInterval(job *CreateJob) error {
	p.next() // consume INTERVAL
	if p.cur.Typ != tNumber {
		return p.errf("expected INTERVAL milliseconds number")
	}
	n, _ := strconv.ParseInt(p.cur.Val, 10, 64)
	job.ScheduleType = "INTERVAL"
	job.IntervalMs = n
	p.next()
	return nil
}

// parseJobScheduleOnce parses SCHEDULE ONCE clause
func (p *Parser) parseJobScheduleOnce(job *CreateJob) error {
	p.next() // consume ONCE
	if p.cur.Typ != tString {
		return p.errf("expected ONCE timestamp string")
	}
	job.ScheduleType = "ONCE"
	// parse time in common layout
	if t, err := time.Parse("2006-01-02 15:04:05", p.cur.Val); err == nil {
		job.RunAt = &t
	}
	p.next()
	return nil
}

// parseJobSQLBody extracts the SQL body of a CREATE JOB statement
func (p *Parser) parseJobSQLBody() string {
	bodyStart := p.cur.Pos
	// Advance until semicolon or EOF
	for (p.cur.Typ != tSymbol || p.cur.Val != ";") && p.cur.Typ != tEOF {
		p.next()
	}
	endPos := p.cur.Pos
	// Extract substring from lexer
	if bodyStart < endPos && endPos <= len(p.lx.s) {
		return p.lx.s[bodyStart:endPos]
	}
	return ""
}

// parseCreateJob handles CREATE JOB statements.
func (p *Parser) parseCreateJob() (Statement, error) {
	// cur is at JOB
	p.next() // consume JOB
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected job name")
	}

	job := &CreateJob{Name: name, Enabled: true}

	// Parse optional clauses until AS
	for p.cur.Typ == tKeyword || p.cur.Typ == tIdent {
		switch p.cur.Val {
		case "SCHEDULE":
			if err := p.parseJobSchedule(job); err != nil {
				return nil, err
			}
		case "TIMEZONE":
			p.next()
			if p.cur.Typ != tString {
				return nil, p.errf("expected timezone string")
			}
			job.Timezone = p.cur.Val
			p.next()
		case "MAX_RUNTIME":
			p.next()
			if p.cur.Typ != tNumber {
				return nil, p.errf("expected number for MAX_RUNTIME")
			}
			n, _ := strconv.ParseInt(p.cur.Val, 10, 64)
			job.MaxRuntimeMs = n
			p.next()
		case "NO_OVERLAP":
			job.NoOverlap = true
			p.next()
		case "CATCH_UP":
			job.CatchUp = true
			p.next()
		case "ENABLED":
			job.Enabled = true
			p.next()
		case "DISABLED":
			job.Enabled = false
			p.next()
		default:
			// stop when we hit AS or other token
			goto afterClauses
		}
	}
afterClauses:
	// If caller provided an explicit AS keyword, consume it; otherwise
	// be permissive and treat the following tokens as the job body.
	if (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && p.cur.Val == "AS" {
		p.next()
	}

	// Capture raw SQL text for the job body
	job.SQLText = p.parseJobSQLBody()

	return job, nil
}

func (p *Parser) parseDropIndex() (Statement, error) {
	// Already consumed DROP INDEX
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

	indexName := p.parseIdentLike()
	if indexName == "" {
		return nil, p.errf("expected index name")
	}

	// Optional: ON table_name
	var tableName string
	if p.cur.Typ == tKeyword && p.cur.Val == "ON" {
		p.next()
		tableName = p.parseIdentLike()
	}

	return &DropIndex{
		Name:     indexName,
		Table:    tableName,
		IfExists: ifExists,
	}, nil
}

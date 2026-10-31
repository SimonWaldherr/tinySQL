package tinysql

import (
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TableBuilder provides a fluent interface for defining tables.
type TableBuilder struct {
	name    string
	columns []storage.Column
	isTemp  bool
}

// NewTableBuilder creates a new table builder for programmatic schema definition.
func NewTableBuilder(name string) *TableBuilder {
	return &TableBuilder{name: name}
}

// Temp marks the table as temporary.
func (tb *TableBuilder) Temp() *TableBuilder {
	tb.isTemp = true
	return tb
}

// Column adds a column to the table.
func (tb *TableBuilder) Column(name string, colType ColType) *TableBuilder {
	tb.columns = append(tb.columns, storage.Column{
		Name: name,
		Type: colType,
	})
	return tb
}

// Int adds an INT column.
func (tb *TableBuilder) Int(name string) *TableBuilder {
	return tb.Column(name, IntType)
}

// Text adds a TEXT column.
func (tb *TableBuilder) Text(name string) *TableBuilder {
	return tb.Column(name, TextType)
}

// Bool adds a BOOL column.
func (tb *TableBuilder) Bool(name string) *TableBuilder {
	return tb.Column(name, BoolType)
}

// Float adds a FLOAT column.
func (tb *TableBuilder) Float(name string) *TableBuilder {
	return tb.Column(name, FloatType)
}

// Timestamp adds a TIMESTAMP column.
func (tb *TableBuilder) Timestamp(name string) *TableBuilder {
	return tb.Column(name, TimestampType)
}

// JSON adds a JSON column.
func (tb *TableBuilder) JSON(name string) *TableBuilder {
	return tb.Column(name, JsonType)
}

// Build creates the table definition.
func (tb *TableBuilder) Build() *engine.CreateTable {
	return &engine.CreateTable{
		Name:   tb.name,
		Cols:   tb.columns,
		IsTemp: tb.isTemp,
	}
}

// Create executes the CREATE TABLE statement.
func (tb *TableBuilder) Create(db *DB, tenant string) error {
	table := NewTable(tb.name, tb.columns, tb.isTemp)
	return db.Put(tenant, table)
}

// InsertBuilder provides a fluent interface for building INSERT statements.
type InsertBuilder struct {
	table string
	cols  []string
	rows  [][]engine.Expr
}

// InsertInto creates a new INSERT builder.
func InsertInto(table string) *InsertBuilder {
	return &InsertBuilder{table: table}
}

// Columns specifies the columns for the insert.
func (ib *InsertBuilder) Columns(cols ...string) *InsertBuilder {
	ib.cols = cols
	return ib
}

// Values specifies the values to insert.
func (ib *InsertBuilder) Values(values ...ExprBuilder) *InsertBuilder {
	row := make([]engine.Expr, len(values))
	for i, v := range values {
		row[i] = v.Build()
	}
	ib.rows = append(ib.rows, row)
	return ib
}

// Build creates the INSERT statement.
func (ib *InsertBuilder) Build() *engine.Insert {
	return &engine.Insert{
		Table: ib.table,
		Cols:  ib.cols,
		Rows:  ib.rows,
	}
}

// UpdateBuilder provides a fluent interface for building UPDATE statements.
type UpdateBuilder struct {
	table string
	sets  map[string]engine.Expr
	where engine.Expr
}

// Update creates a new UPDATE builder.
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{
		table: table,
		sets:  make(map[string]engine.Expr),
	}
}

// Set adds a column assignment.
func (ub *UpdateBuilder) Set(column string, value ExprBuilder) *UpdateBuilder {
	ub.sets[column] = value.Build()
	return ub
}

// Where adds a WHERE condition.
func (ub *UpdateBuilder) Where(condition ExprBuilder) *UpdateBuilder {
	ub.where = condition.Build()
	return ub
}

// Build creates the UPDATE statement.
func (ub *UpdateBuilder) Build() *engine.Update {
	return &engine.Update{
		Table: ub.table,
		Sets:  ub.sets,
		Where: ub.where,
	}
}

// DeleteBuilder provides a fluent interface for building DELETE statements.
type DeleteBuilder struct {
	table string
	where engine.Expr
}

// DeleteFrom creates a new DELETE builder.
func DeleteFrom(table string) *DeleteBuilder {
	return &DeleteBuilder{table: table}
}

// Where adds a WHERE condition.
func (db *DeleteBuilder) Where(condition ExprBuilder) *DeleteBuilder {
	db.where = condition.Build()
	return db
}

// Build creates the DELETE statement.
func (db *DeleteBuilder) Build() *engine.Delete {
	return &engine.Delete{
		Table: db.table,
		Where: db.where,
	}
}

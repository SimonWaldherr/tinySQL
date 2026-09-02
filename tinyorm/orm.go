// Package tinyorm provides a deliberately small ORM layer for tinySQL.
//
// It focuses on the pieces that are useful for embedded tools: struct mapping,
// additive table creation, named parameters, and simple primary-key CRUD.
package tinyorm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// TableNamer lets a model override the default snake_case table name.
type TableNamer interface {
	TableName() string
}

// DB wraps a tinySQL database with a tenant for ORM operations.
type DB struct {
	Raw    *tinysql.DB
	Tenant string
}

// New creates an ORM handle. Empty tenant defaults to "default".
func New(db *tinysql.DB, tenant string) *DB {
	if tenant == "" {
		tenant = "default"
	}
	return &DB{Raw: db, Tenant: tenant}
}

// Exec executes SQL after replacing :name or @name placeholders from params.
func (db *DB) Exec(ctx context.Context, sql string, params any) (*tinysql.ResultSet, error) {
	bound, err := BindNamed(sql, params)
	if err != nil {
		return nil, err
	}
	return db.execSQL(ctx, bound)
}

// Query executes SQL after replacing :name or @name placeholders from params.
func (db *DB) Query(ctx context.Context, sql string, params any) (*tinysql.ResultSet, error) {
	return db.Exec(ctx, sql, params)
}

// AutoMigrate creates tables for the supplied models when they do not exist.
// It is intentionally additive-only; it never drops or rewrites tables.
func (db *DB) AutoMigrate(ctx context.Context, models ...any) error {
	for _, model := range models {
		meta, err := describeModel(model)
		if err != nil {
			return err
		}
		parts := make([]string, 0, len(meta.fields))
		for _, f := range meta.fields {
			col := quoteIdent(f.column) + " " + f.sqlType
			if f.pk {
				col += " PRIMARY KEY"
			}
			if f.unique {
				col += " UNIQUE"
			}
			if f.notNull && !f.pk {
				col += " NOT NULL"
			}
			if f.defaultSQL != "" {
				col += " DEFAULT " + f.defaultSQL
			}
			parts = append(parts, col)
		}
		if len(parts) == 0 {
			return fmt.Errorf("tinyorm: model %s has no mapped fields", meta.typ.Name())
		}
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(meta.table), strings.Join(parts, ", "))
		if _, err := db.execSQL(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

// Insert inserts one struct value.
func (db *DB) Insert(ctx context.Context, model any) error {
	meta, value, err := modelValue(model)
	if err != nil {
		return err
	}
	cols := make([]string, 0, len(meta.fields))
	vals := make([]string, 0, len(meta.fields))
	for _, f := range meta.fields {
		cols = append(cols, quoteIdent(f.column))
		vals = append(vals, sqlLiteral(fieldValue(value, f)))
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdent(meta.table), strings.Join(cols, ", "), strings.Join(vals, ", "))
	_, err = db.execSQL(ctx, sql)
	return err
}

// Create is an ORM-style alias for Insert.
func (db *DB) Create(ctx context.Context, model any) error {
	return db.Insert(ctx, model)
}

// Save updates every mapped, non-primary-key field using the model's primary
// key. A primary key is required so Save never accidentally updates a table.
func (db *DB) Save(ctx context.Context, model any) error {
	meta, value, err := modelValue(model)
	if err != nil {
		return err
	}
	pk := meta.primaryField()
	if pk == nil {
		return fmt.Errorf("tinyorm: model %s has no primary key field", meta.typ.Name())
	}
	assignments := make([]string, 0, len(meta.fields)-1)
	for _, f := range meta.fields {
		if f.pk {
			continue
		}
		assignments = append(assignments, quoteIdent(f.column)+" = "+sqlLiteral(fieldValue(value, f)))
	}
	if len(assignments) == 0 {
		return fmt.Errorf("tinyorm: model %s has no fields to save", meta.typ.Name())
	}
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s", quoteIdent(meta.table), strings.Join(assignments, ", "), quoteIdent(pk.column), sqlLiteral(fieldValue(value, *pk)))
	_, err = db.execSQL(ctx, sql)
	return err
}

// Update is an ORM-style alias for Save.
func (db *DB) Update(ctx context.Context, model any) error {
	return db.Save(ctx, model)
}

// FindByPK loads one row by primary key into dest, which must be a pointer to a struct.
func (db *DB) FindByPK(ctx context.Context, dest any, pk any) error {
	meta, err := describeModel(dest)
	if err != nil {
		return err
	}
	pkField := meta.primaryField()
	if pkField == nil {
		return fmt.Errorf("tinyorm: model %s has no primary key field", meta.typ.Name())
	}
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s = :pk LIMIT 1", meta.selectList(), quoteIdent(meta.table), quoteIdent(pkField.column))
	rs, err := db.Exec(ctx, sql, map[string]any{"pk": pk})
	if err != nil {
		return err
	}
	if len(rs.Rows) == 0 {
		return ErrNotFound
	}
	return scanStruct(dest, rs.Rows[0], meta)
}

// Select loads rows into dest, which must be a pointer to a slice of structs.
// The where argument may be empty or a SQL fragment without the WHERE keyword.
func (db *DB) Select(ctx context.Context, dest any, where string, params any) error {
	sliceValue, elemType, err := sliceDest(dest)
	if err != nil {
		return err
	}
	meta, err := describeType(elemType)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("SELECT %s FROM %s", meta.selectList(), quoteIdent(meta.table))
	if strings.TrimSpace(where) != "" {
		sql += " WHERE " + where
	}
	rs, err := db.Exec(ctx, sql, params)
	if err != nil {
		return err
	}
	sliceType := sliceValue.Type()
	out := reflect.MakeSlice(sliceType, len(rs.Rows), len(rs.Rows))
	pointerElements := sliceType.Elem().Kind() == reflect.Pointer
	for i, row := range rs.Rows {
		if pointerElements {
			item := reflect.New(elemType)
			if err := scanStructValue(item.Elem(), row, meta); err != nil {
				return err
			}
			out.Index(i).Set(item)
			continue
		}
		if err := scanStructValue(out.Index(i), row, meta); err != nil {
			return err
		}
	}
	sliceValue.Set(out)
	return nil
}

// First loads the first row matching where into dest. The where argument may
// be empty or a SQL fragment without the WHERE keyword.
func (db *DB) First(ctx context.Context, dest any, where string, params any) error {
	meta, err := describeModel(dest)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("SELECT %s FROM %s", meta.selectList(), quoteIdent(meta.table))
	if strings.TrimSpace(where) != "" {
		sql += " WHERE " + where
	}
	sql += " LIMIT 1"
	rs, err := db.Exec(ctx, sql, params)
	if err != nil {
		return err
	}
	if len(rs.Rows) == 0 {
		return ErrNotFound
	}
	return scanStruct(dest, rs.Rows[0], meta)
}

// DeleteByPK deletes one row by primary key.
func (db *DB) DeleteByPK(ctx context.Context, model any, pk any) error {
	meta, err := describeModel(model)
	if err != nil {
		return err
	}
	pkField := meta.primaryField()
	if pkField == nil {
		return fmt.Errorf("tinyorm: model %s has no primary key field", meta.typ.Name())
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = :pk", quoteIdent(meta.table), quoteIdent(pkField.column))
	_, err = db.Exec(ctx, sql, map[string]any{"pk": pk})
	return err
}

func (db *DB) execSQL(ctx context.Context, sql string) (*tinysql.ResultSet, error) {
	if db == nil || db.Raw == nil {
		return nil, fmt.Errorf("tinyorm: nil DB")
	}
	tenant := db.Tenant
	if tenant == "" {
		tenant = "default"
	}
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return tinysql.Execute(ctx, db.Raw, tenant, stmt)
}

// ErrNotFound is returned when FindByPK finds no matching row.
var ErrNotFound = fmt.Errorf("tinyorm: not found")

type modelMeta struct {
	typ           reflect.Type
	table         string
	fields        []fieldMeta
	primary       int
	selectListSQL string
}

type fieldMeta struct {
	index       int
	name        string
	column      string
	lowerColumn string
	sqlType     string
	pk          bool
	unique      bool
	notNull     bool
	defaultSQL  string
}

// modelMetaCache keeps the reflection-derived, immutable mapping for each
// struct type. ORM operations use the same model shape repeatedly, so this
// avoids rediscovering tags, column types, and table names on every query.
var modelMetaCache sync.Map // map[reflect.Type]*modelMeta

type namedFieldMeta struct {
	index     int
	columnKey string
	fieldKey  string
}

// namedFieldCache stores the parameter names derived from exported struct
// fields. BindNamed is often called in request paths, where repeating this tag
// parsing and snake_case conversion is unnecessary work.
var namedFieldCache sync.Map // map[reflect.Type][]namedFieldMeta

func (m modelMeta) primaryField() *fieldMeta {
	if m.primary >= 0 && m.primary < len(m.fields) {
		return &m.fields[m.primary]
	}
	return nil
}

func (m modelMeta) selectList() string {
	return m.selectListSQL
}

func describeModel(model any) (modelMeta, error) {
	if model == nil {
		return modelMeta{}, fmt.Errorf("tinyorm: nil model")
	}
	t := reflect.TypeOf(model)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return describeType(t)
}

func describeType(t reflect.Type) (modelMeta, error) {
	if t.Kind() != reflect.Struct {
		return modelMeta{}, fmt.Errorf("tinyorm: model must be a struct, got %s", t.Kind())
	}
	if cached, ok := modelMetaCache.Load(t); ok {
		return *cached.(*modelMeta), nil
	}

	meta := modelMeta{typ: t, table: tableNameFor(t), primary: -1}
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name, opts := parseFieldTag(sf)
		if opts.skip {
			continue
		}
		if name == "" {
			name = snakeCase(sf.Name)
		}
		sqlType := sqlTypeFor(sf.Type)
		if opts.sqlType != "" {
			sqlType = opts.sqlType
		}
		field := fieldMeta{
			index:       i,
			name:        sf.Name,
			column:      name,
			lowerColumn: strings.ToLower(name),
			sqlType:     sqlType,
			pk:          opts.primary,
			unique:      opts.unique,
			notNull:     opts.notNull,
			defaultSQL:  opts.defaultSQL,
		}
		if field.pk && meta.primary == -1 {
			meta.primary = len(meta.fields)
		}
		meta.fields = append(meta.fields, field)
	}
	cols := make([]string, len(meta.fields))
	for i, f := range meta.fields {
		cols[i] = quoteIdent(f.column)
	}
	meta.selectListSQL = strings.Join(cols, ", ")

	actual, _ := modelMetaCache.LoadOrStore(t, &meta)
	return *actual.(*modelMeta), nil
}

func modelValue(model any) (modelMeta, reflect.Value, error) {
	if model == nil {
		return modelMeta{}, reflect.Value{}, fmt.Errorf("tinyorm: nil model")
	}
	v := reflect.ValueOf(model)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return modelMeta{}, reflect.Value{}, fmt.Errorf("tinyorm: nil model pointer")
		}
		v = v.Elem()
	}
	meta, err := describeType(v.Type())
	return meta, v, err
}

func fieldValue(v reflect.Value, f fieldMeta) any {
	return v.Field(f.index).Interface()
}

func tableNameFor(t reflect.Type) string {
	ptr := reflect.New(t)
	if namer, ok := ptr.Interface().(TableNamer); ok {
		if name := strings.TrimSpace(namer.TableName()); name != "" {
			return name
		}
	}
	return snakeCase(t.Name())
}

type tagOptions struct {
	skip       bool
	primary    bool
	unique     bool
	notNull    bool
	sqlType    string
	defaultSQL string
}

// parseFieldTag accepts the compact db tag used by tinyorm/sqlx and the common
// GORM, Bun, go-pg, and XORM spellings. db takes precedence when more than one
// tag is present.
//
// Examples:
//
//	ID    int    `db:"id,pk"`
//	Email string `db:"email,unique,notnull"`
//	State string `gorm:"column:state;not null;default:'active';type:VARCHAR(16)"`
func parseFieldTag(sf reflect.StructField) (string, tagOptions) {
	if tag, ok := sf.Tag.Lookup("db"); ok {
		return parseDBTag(tag)
	}
	if tag, ok := sf.Tag.Lookup("tinyorm"); ok {
		return parseDBTag(tag)
	}
	if tag, ok := sf.Tag.Lookup("gorm"); ok {
		return parseGORMTag(tag)
	}
	if tag, ok := sf.Tag.Lookup("bun"); ok {
		return parseDBTag(tag)
	}
	if tag, ok := sf.Tag.Lookup("pg"); ok {
		return parseDBTag(tag)
	}
	if tag, ok := sf.Tag.Lookup("xorm"); ok {
		return parseXORMTag(tag)
	}
	return "", tagOptions{}
}

func parseDBTag(tag string) (string, tagOptions) {
	if strings.TrimSpace(tag) == "-" {
		return "", tagOptions{skip: true}
	}
	parts := strings.Split(tag, ",")
	name := strings.TrimSpace(parts[0])
	var opts tagOptions
	for _, part := range parts[1:] {
		applyTagOption(&opts, part)
	}
	return name, opts
}

func parseGORMTag(tag string) (string, tagOptions) {
	var name string
	var opts tagOptions
	for _, part := range strings.Split(tag, ";") {
		part = strings.TrimSpace(part)
		lower := strings.ToLower(part)
		if strings.HasPrefix(lower, "column:") {
			name = strings.TrimSpace(part[len("column:"):])
			continue
		}
		applyTagOption(&opts, part)
	}
	return name, opts
}

// parseXORMTag handles XORM's space-separated modifiers and quoted column
// name, such as xorm:"pk 'user_id' notnull". Unsupported XORM modifiers are
// deliberately ignored; they do not change the table shape tinyorm owns.
func parseXORMTag(tag string) (string, tagOptions) {
	tokens := strings.Fields(tag)
	var name string
	var opts tagOptions
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		if len(token) >= 2 && ((token[0] == '\'' && token[len(token)-1] == '\'') || (token[0] == '"' && token[len(token)-1] == '"')) {
			name = token[1 : len(token)-1]
			continue
		}
		if strings.EqualFold(token, "default") && i+1 < len(tokens) {
			i++
			opts.defaultSQL = defaultTagSQL(tokens[i])
			continue
		}
		if isXORMType(token) {
			opts.sqlType = token
			continue
		}
		applyTagOption(&opts, token)
	}
	return name, opts
}

func isXORMType(token string) bool {
	lower := strings.ToLower(token)
	for _, prefix := range []string{
		"int", "integer", "bigint", "smallint", "tinyint", "float", "double",
		"decimal", "numeric", "bool", "char", "varchar", "text", "blob",
		"date", "time", "timestamp", "json",
	} {
		if lower == prefix || strings.HasPrefix(lower, prefix+"(") {
			return true
		}
	}
	return false
}

func applyTagOption(opts *tagOptions, option string) {
	option = strings.TrimSpace(option)
	lower := strings.ToLower(option)
	switch lower {
	case "pk", "primary", "primarykey", "primary_key":
		opts.primary = true
	case "unique", "uniqueindex":
		opts.unique = true
	case "notnull", "not_null", "not null":
		opts.notNull = true
	case "-":
		opts.skip = true
	default:
		if key, value, ok := strings.Cut(option, "="); ok {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "type":
				opts.sqlType = strings.TrimSpace(value)
			case "default":
				opts.defaultSQL = defaultTagSQL(value)
			}
			return
		}
		if key, value, ok := strings.Cut(option, ":"); ok {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "type":
				opts.sqlType = strings.TrimSpace(value)
			case "default":
				opts.defaultSQL = defaultTagSQL(value)
			}
		}
	}
}

// defaultTagSQL turns unquoted tag defaults into SQL literals. Quoted values,
// numbers, booleans, and NULL are retained so model definitions can be read
// naturally without hand-writing SQL for common cases.
func defaultTagSQL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "''"
	}
	lower := strings.ToLower(value)
	if lower == "null" || lower == "true" || lower == "false" {
		return lower
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return value
	}
	if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
		(strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) {
		return value
	}
	return sqlLiteral(value)
}

func sqlTypeFor(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(time.Time{}) {
		return "TEXT"
	}
	switch t.Kind() {
	case reflect.Bool:
		return "BOOL"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INT"
	case reflect.Float32, reflect.Float64:
		return "FLOAT"
	case reflect.String:
		return "TEXT"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BLOB"
		}
		return "JSON"
	case reflect.Map, reflect.Struct:
		return "JSON"
	default:
		return "TEXT"
	}
}

func sliceDest(dest any) (reflect.Value, reflect.Type, error) {
	if dest == nil {
		return reflect.Value{}, nil, fmt.Errorf("tinyorm: nil destination")
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return reflect.Value{}, nil, fmt.Errorf("tinyorm: destination must be pointer to slice")
	}
	s := v.Elem()
	if s.Kind() != reflect.Slice {
		return reflect.Value{}, nil, fmt.Errorf("tinyorm: destination must be pointer to slice")
	}
	elem := s.Type().Elem()
	if elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	if elem.Kind() != reflect.Struct {
		return reflect.Value{}, nil, fmt.Errorf("tinyorm: slice element must be struct")
	}
	return s, elem, nil
}

func scanStruct(dest any, row tinysql.Row, meta modelMeta) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("tinyorm: destination must be pointer to struct")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("tinyorm: destination must be pointer to struct")
	}
	return scanStructValue(v, row, meta)
}

func scanStructValue(v reflect.Value, row tinysql.Row, meta modelMeta) error {
	for _, f := range meta.fields {
		field := v.Field(f.index)
		if !field.CanSet() {
			continue
		}
		val, ok := row[f.lowerColumn]
		if !ok {
			val, ok = row[f.column]
		}
		if !ok {
			continue
		}
		if err := assignValue(field, val); err != nil {
			return fmt.Errorf("tinyorm: scan %s: %w", f.name, err)
		}
	}
	return nil
}

func assignValue(dst reflect.Value, val any) error {
	if val == nil {
		return nil
	}
	for dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	if dst.Type() == reflect.TypeOf(time.Time{}) {
		switch v := val.(type) {
		case time.Time:
			dst.Set(reflect.ValueOf(v))
			return nil
		case string:
			for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
				if parsed, err := time.Parse(layout, v); err == nil {
					dst.Set(reflect.ValueOf(parsed))
					return nil
				}
			}
		}
	}
	switch dst.Kind() {
	case reflect.Bool:
		b, err := toBool(val)
		if err != nil {
			return err
		}
		dst.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := toInt64(val)
		if err != nil {
			return err
		}
		dst.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, err := toInt64(val)
		if err != nil {
			return err
		}
		dst.SetUint(uint64(i))
	case reflect.Float32, reflect.Float64:
		f, err := toFloat64(val)
		if err != nil {
			return err
		}
		dst.SetFloat(f)
	case reflect.String:
		dst.SetString(fmt.Sprintf("%v", val))
	case reflect.Slice:
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			switch v := val.(type) {
			case []byte:
				dst.SetBytes(v)
			case string:
				dst.SetBytes([]byte(v))
			default:
				return fmt.Errorf("cannot assign %T to []byte", val)
			}
			return nil
		}
		return assignJSON(dst, val)
	case reflect.Map, reflect.Struct:
		return assignJSON(dst, val)
	default:
		return fmt.Errorf("unsupported destination kind %s", dst.Kind())
	}
	return nil
}

func assignJSON(dst reflect.Value, val any) error {
	var body []byte
	switch v := val.(type) {
	case string:
		body = []byte(v)
	case []byte:
		body = v
	case json.RawMessage:
		body = v
	default:
		var err error
		body, err = json.Marshal(v)
		if err != nil {
			return err
		}
	}
	ptr := reflect.New(dst.Type())
	if err := json.Unmarshal(body, ptr.Interface()); err != nil {
		return err
	}
	dst.Set(ptr.Elem())
	return nil
}

func toBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		return strconv.ParseBool(x)
	default:
		i, err := toInt64(v)
		return i != 0, err
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	default:
		i, err := toInt64(v)
		return float64(i), err
	}
}

// BindNamed replaces :name and @name placeholders outside SQL string literals.
func BindNamed(sql string, params any) (string, error) {
	values, err := namedValues(params)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.Grow(len(sql) + len(values)*8)
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			b.WriteByte(ch)
			i++
			for ; i < len(sql); i++ {
				b.WriteByte(sql[i])
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i++
						b.WriteByte(sql[i])
						continue
					}
					break
				}
			}
			continue
		}
		if (ch == ':' || ch == '@') && i+1 < len(sql) && isIdentStart(rune(sql[i+1])) {
			j := i + 2
			for j < len(sql) && isIdentPart(rune(sql[j])) {
				j++
			}
			name := sql[i+1 : j]
			val, ok := values[strings.ToLower(name)]
			if !ok {
				return "", fmt.Errorf("tinyorm: missing named parameter %s", name)
			}
			b.WriteString(sqlLiteral(val))
			i = j - 1
			continue
		}
		b.WriteByte(ch)
	}
	return b.String(), nil
}

func namedValues(params any) (map[string]any, error) {
	out := make(map[string]any)
	if params == nil {
		return out, nil
	}
	v := reflect.ValueOf(params)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return out, nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map:
		out = make(map[string]any, v.Len())
		for _, key := range v.MapKeys() {
			out[strings.ToLower(fmt.Sprintf("%v", key.Interface()))] = v.MapIndex(key).Interface()
		}
	case reflect.Struct:
		t := v.Type()
		fields := namedFieldsFor(t)
		out = make(map[string]any, len(fields)*2)
		for _, field := range fields {
			value := v.Field(field.index).Interface()
			out[field.columnKey] = value
			if field.fieldKey != field.columnKey {
				out[field.fieldKey] = value
			}
		}
	default:
		return nil, fmt.Errorf("tinyorm: params must be map or struct")
	}
	return out, nil
}

func namedFieldsFor(t reflect.Type) []namedFieldMeta {
	if cached, ok := namedFieldCache.Load(t); ok {
		return cached.([]namedFieldMeta)
	}

	fields := make([]namedFieldMeta, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name, opts := parseFieldTag(sf)
		if opts.skip {
			continue
		}
		if name == "" {
			name = snakeCase(sf.Name)
		}
		fields = append(fields, namedFieldMeta{
			index:     i,
			columnKey: strings.ToLower(name),
			fieldKey:  strings.ToLower(sf.Name),
		})
	}

	actual, _ := namedFieldCache.LoadOrStore(t, fields)
	return actual.([]namedFieldMeta)
}

func sqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + base64.StdEncoding.EncodeToString(x) + "'"
	case time.Time:
		return "'" + x.UTC().Format(time.RFC3339Nano) + "'"
	default:
		body, err := json.Marshal(x)
		if err != nil {
			return "'" + strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''") + "'"
		}
		return "'" + strings.ReplaceAll(string(body), "'", "''") + "'"
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func snakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// Columns returns mapped column names for a model. It is useful for demos and
// generated SQL in applications that want to stay explicit.
func Columns(model any) ([]string, error) {
	meta, err := describeModel(model)
	if err != nil {
		return nil, err
	}
	cols := make([]string, len(meta.fields))
	for i, f := range meta.fields {
		cols[i] = f.column
	}
	sort.Strings(cols)
	return cols, nil
}

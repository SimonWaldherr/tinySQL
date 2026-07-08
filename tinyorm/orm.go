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
	out := reflect.MakeSlice(sliceValue.Type(), 0, len(rs.Rows))
	for _, row := range rs.Rows {
		item := reflect.New(elemType).Elem()
		if err := scanStructValue(item, row, meta); err != nil {
			return err
		}
		out = reflect.Append(out, item)
	}
	sliceValue.Set(out)
	return nil
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
	typ    reflect.Type
	table  string
	fields []fieldMeta
}

type fieldMeta struct {
	index   int
	name    string
	column  string
	sqlType string
	pk      bool
	unique  bool
}

func (m modelMeta) primaryField() *fieldMeta {
	for i := range m.fields {
		if m.fields[i].pk {
			return &m.fields[i]
		}
	}
	return nil
}

func (m modelMeta) selectList() string {
	cols := make([]string, len(m.fields))
	for i, f := range m.fields {
		cols[i] = quoteIdent(f.column)
	}
	return strings.Join(cols, ", ")
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
	meta := modelMeta{typ: t, table: tableNameFor(t)}
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		tag := sf.Tag.Get("db")
		if tag == "-" {
			continue
		}
		name, opts := parseDBTag(tag)
		if name == "" {
			name = snakeCase(sf.Name)
		}
		meta.fields = append(meta.fields, fieldMeta{
			index:   i,
			name:    sf.Name,
			column:  name,
			sqlType: sqlTypeFor(sf.Type),
			pk:      opts["pk"] || opts["primary"] || opts["primarykey"],
			unique:  opts["unique"],
		})
	}
	return meta, nil
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

func parseDBTag(tag string) (string, map[string]bool) {
	opts := make(map[string]bool)
	parts := strings.Split(tag, ",")
	name := strings.TrimSpace(parts[0])
	for _, part := range parts[1:] {
		part = strings.ToLower(strings.TrimSpace(part))
		if part != "" {
			opts[part] = true
		}
	}
	return name, opts
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
		val, ok := row[strings.ToLower(f.column)]
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
		for _, key := range v.MapKeys() {
			out[strings.ToLower(fmt.Sprintf("%v", key.Interface()))] = v.MapIndex(key).Interface()
		}
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if sf.PkgPath != "" {
				continue
			}
			name, _ := parseDBTag(sf.Tag.Get("db"))
			if name == "" {
				name = snakeCase(sf.Name)
			}
			out[strings.ToLower(name)] = v.Field(i).Interface()
			out[strings.ToLower(sf.Name)] = v.Field(i).Interface()
		}
	default:
		return nil, fmt.Errorf("tinyorm: params must be map or struct")
	}
	return out, nil
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", x)
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

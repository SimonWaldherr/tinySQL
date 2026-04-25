package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Store owns all persistence operations.
type Store struct {
	db *sql.DB
	d  dialect
}

// NewStore creates a Store for db and dialect.
func NewStore(db *sql.DB, d dialect) *Store {
	return &Store{db: db, d: d}
}

// CountUsers returns the number of users in the database.
func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

// CreateUser creates a new user.
func (s *Store) CreateUser(ctx context.Context, username, displayName, passwordHash string, role Role, active bool) (int64, error) {
	now := time.Now().UTC().Format(timeLayout)
	var id int64
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		var err error
		id, err = s.d.insertReturningID(ctx, tx, "users", []string{"username", "display_name", "password_hash", "role", "active", "created_at"}, username, displayName, passwordHash, string(role), boolValue(active), now)
		return err
	})
	return id, err
}

// ListUsers returns all users ordered by username.
func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id, username, display_name, password_hash, role, active, created_at FROM users ORDER BY username")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		var active any
		if err := rows.Scan(&u.ID, &u.Username, &u.DisplayName, &u.PasswordHash, &u.Role, &active, &u.CreatedAt); err != nil {
			return nil, err
		}
		u.Active = scanBool(active)
		users = append(users, u)
	}
	return users, rows.Err()
}

// FindUserByUsername returns an active or inactive user by username.
func (s *Store) FindUserByUsername(ctx context.Context, username string) (User, error) {
	var u User
	var active any
	err := s.db.QueryRowContext(ctx, "SELECT id, username, display_name, password_hash, role, active, created_at FROM users WHERE username = "+s.d.placeholder(1), username).
		Scan(&u.ID, &u.Username, &u.DisplayName, &u.PasswordHash, &u.Role, &active, &u.CreatedAt)
	u.Active = scanBool(active)
	return u, err
}

// FindUserByID returns an active or inactive user by ID.
func (s *Store) FindUserByID(ctx context.Context, id int64) (User, error) {
	var u User
	var active any
	err := s.db.QueryRowContext(ctx, "SELECT id, username, display_name, password_hash, role, active, created_at FROM users WHERE id = "+s.d.placeholder(1), id).
		Scan(&u.ID, &u.Username, &u.DisplayName, &u.PasswordHash, &u.Role, &active, &u.CreatedAt)
	u.Active = scanBool(active)
	return u, err
}

// CreateForm creates a form, its fields, and field options atomically.
func (s *Store) CreateForm(ctx context.Context, input CreateFormInput) (int64, error) {
	now := time.Now().UTC().Format(timeLayout)
	var formID int64
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		var err error
		formID, err = s.d.insertReturningID(ctx, tx, "forms", []string{"title", "description", "created_by", "created_at"}, input.Title, input.Description, input.CreatedBy, now)
		if err != nil {
			return err
		}

		for i, f := range input.Fields {
			if f.SortOrder == 0 {
				f.SortOrder = i
			}
			fieldID, err := s.d.insertReturningID(ctx, tx, "form_fields", []string{"form_id", "type", "label", "required", "default_value", "sort_order"}, formID, string(f.Type), f.Label, boolValue(f.Required), f.DefaultValue, f.SortOrder)
			if err != nil {
				return err
			}
			for j, opt := range f.Options {
				if opt.SortOrder == 0 {
					opt.SortOrder = j
				}
				_, err := s.d.insertReturningID(ctx, tx, "field_options", []string{"field_id", "value", "label", "sort_order"}, fieldID, opt.Value, opt.Label, opt.SortOrder)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return formID, err
}

// ListForms returns all forms ordered by creation time.
func (s *Store) ListForms(ctx context.Context) ([]Form, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id, title, description, created_by, created_at FROM forms ORDER BY created_at DESC, id DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var forms []Form
	for rows.Next() {
		var f Form
		if err := rows.Scan(&f.ID, &f.Title, &f.Description, &f.CreatedBy, &f.CreatedAt); err != nil {
			return nil, err
		}
		forms = append(forms, f)
	}
	return forms, rows.Err()
}

// GetFormDetail returns a form, all fields, and options.
func (s *Store) GetFormDetail(ctx context.Context, id int64) (FormDetail, error) {
	var detail FormDetail
	err := s.db.QueryRowContext(ctx, "SELECT id, title, description, created_by, created_at FROM forms WHERE id = "+s.d.placeholder(1), id).
		Scan(&detail.Form.ID, &detail.Form.Title, &detail.Form.Description, &detail.Form.CreatedBy, &detail.Form.CreatedAt)
	if err != nil {
		return detail, err
	}

	rows, err := s.db.QueryContext(ctx, "SELECT id, form_id, type, label, required, default_value, sort_order FROM form_fields WHERE form_id = "+s.d.placeholder(1)+" ORDER BY sort_order, id", id)
	if err != nil {
		return detail, err
	}
	defer rows.Close()

	for rows.Next() {
		var f Field
		var required any
		if err := rows.Scan(&f.ID, &f.FormID, &f.Type, &f.Label, &required, &f.DefaultValue, &f.SortOrder); err != nil {
			return detail, err
		}
		f.Required = scanBool(required)
		detail.Fields = append(detail.Fields, f)
	}
	if err := rows.Err(); err != nil {
		return detail, err
	}

	if len(detail.Fields) == 0 {
		return detail, nil
	}

	ids := make([]int64, len(detail.Fields))
	byID := make(map[int64]int, len(detail.Fields))
	for i, f := range detail.Fields {
		ids[i] = f.ID
		byID[f.ID] = i
	}

	query, args := s.inClause("SELECT id, field_id, value, label, sort_order FROM field_options WHERE field_id IN (%s) ORDER BY field_id, sort_order, id", ids)
	orows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return detail, err
	}
	defer orows.Close()

	for orows.Next() {
		var o Option
		if err := orows.Scan(&o.ID, &o.FieldID, &o.Value, &o.Label, &o.SortOrder); err != nil {
			return detail, err
		}
		if idx, ok := byID[o.FieldID]; ok {
			detail.Fields[idx].Options = append(detail.Fields[idx].Options, o)
		}
	}
	return detail, orows.Err()
}

// SaveAnswer validates and stores a submission atomically.
func (s *Store) SaveAnswer(ctx context.Context, input SaveAnswerInput) (int64, error) {
	detail, err := s.GetFormDetail(ctx, input.FormID)
	if err != nil {
		return 0, err
	}
	if err := validateAnswer(detail, input.Values); err != nil {
		return 0, err
	}

	now := time.Now().UTC().Format(timeLayout)
	var submissionID int64
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		var err error
		submissionID, err = s.d.insertReturningID(ctx, tx, "submissions", []string{"form_id", "user_id", "submitter_name", "created_at"}, input.FormID, input.UserID, input.SubmitterName, now)
		if err != nil {
			return err
		}
		for _, f := range detail.Fields {
			value := input.Values[f.ID]
			if _, err := tx.ExecContext(ctx, "INSERT INTO submission_values (submission_id, field_id, value) VALUES ("+s.d.placeholder(1)+", "+s.d.placeholder(2)+", "+s.d.placeholder(3)+")", submissionID, f.ID, value); err != nil {
				return err
			}
		}
		return nil
	})
	return submissionID, err
}

// ListAnswers returns display-ready answers for one form.
func (s *Store) ListAnswers(ctx context.Context, formID int64) (FormDetail, []AnswerRow, error) {
	detail, err := s.GetFormDetail(ctx, formID)
	if err != nil {
		return detail, nil, err
	}

	rows, err := s.db.QueryContext(ctx, "SELECT id, user_id, submitter_name, created_at FROM submissions WHERE form_id = "+s.d.placeholder(1)+" ORDER BY created_at DESC, id DESC", formID)
	if err != nil {
		return detail, nil, err
	}
	defer rows.Close()

	var answers []AnswerRow
	for rows.Next() {
		var a AnswerRow
		if err := rows.Scan(&a.ID, &a.UserID, &a.SubmitterName, &a.CreatedAt); err != nil {
			return detail, nil, err
		}
		values, err := s.answerValues(ctx, a.ID)
		if err != nil {
			return detail, nil, err
		}
		for _, f := range detail.Fields {
			cell := Cell{FieldID: f.ID, Label: f.Label, Value: values[f.ID]}
			a.Cells = append(a.Cells, cell)
			a.Values = append(a.Values, cell.Value)
		}
		answers = append(answers, a)
	}
	return detail, answers, rows.Err()
}

// DeleteExpiredSessions removes expired sessions.
func (s *Store) DeleteExpiredSessions(ctx context.Context, now string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM sessions WHERE expires_at < "+s.d.placeholder(1), now)
	return err
}

// CreateSession stores a login session.
func (s *Store) CreateSession(ctx context.Context, tokenHash, csrfToken string, userID int64, expiresAt string) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO sessions (token_hash, user_id, csrf_token, expires_at) VALUES ("+s.d.placeholder(1)+", "+s.d.placeholder(2)+", "+s.d.placeholder(3)+", "+s.d.placeholder(4)+")", tokenHash, userID, csrfToken, expiresAt)
	return err
}

// DeleteSession deletes a session by token hash.
func (s *Store) DeleteSession(ctx context.Context, tokenHash string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM sessions WHERE token_hash = "+s.d.placeholder(1), tokenHash)
	return err
}

// SessionUser returns the active user and CSRF token for a session hash.
func (s *Store) SessionUser(ctx context.Context, tokenHash, now string) (User, string, error) {
	var u User
	var csrf string
	var active any
	query := "SELECT u.id, u.username, u.display_name, u.password_hash, u.role, u.active, u.created_at, s.csrf_token FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = " + s.d.placeholder(1) + " AND s.expires_at > " + s.d.placeholder(2) + " AND u.active = " + s.d.placeholder(3)
	err := s.db.QueryRowContext(ctx, query, tokenHash, now, boolValue(true)).Scan(&u.ID, &u.Username, &u.DisplayName, &u.PasswordHash, &u.Role, &active, &u.CreatedAt, &csrf)
	u.Active = scanBool(active)
	return u, csrf, err
}

// answerValues returns all stored values for one submission.
func (s *Store) answerValues(ctx context.Context, submissionID int64) (map[int64]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT field_id, value FROM submission_values WHERE submission_id = "+s.d.placeholder(1), submissionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make(map[int64]string)
	for rows.Next() {
		var fieldID int64
		var value string
		if err := rows.Scan(&fieldID, &value); err != nil {
			return nil, err
		}
		values[fieldID] = value
	}
	return values, rows.Err()
}

// inClause returns a query with placeholders for ids.
func (s *Store) inClause(format string, ids []int64) (string, []any) {
	parts := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		parts[i] = s.d.placeholder(i + 1)
		args[i] = id
	}
	return fmt.Sprintf(format, strings.Join(parts, ", ")), args
}

// validateAnswer checks required fields and option membership before storage.
func validateAnswer(detail FormDetail, values map[int64]string) error {
	fieldIDs := make(map[int64]Field, len(detail.Fields))
	for _, f := range detail.Fields {
		fieldIDs[f.ID] = f
	}
	for id := range values {
		if _, ok := fieldIDs[id]; !ok {
			return fmt.Errorf("unknown field id %d", id)
		}
	}
	for _, f := range detail.Fields {
		value := strings.TrimSpace(values[f.ID])
		if f.Required && value == "" {
			return fmt.Errorf("field %q is required", f.Label)
		}
		if value == "" {
			continue
		}
		switch f.Type {
		case FieldSelect:
			if !optionContains(f.Options, value) {
				return fmt.Errorf("invalid option for %q", f.Label)
			}
		case FieldCheckboxes:
			parts := strings.Split(value, ",")
			for _, p := range parts {
				if !optionContains(f.Options, strings.TrimSpace(p)) {
					return fmt.Errorf("invalid checkbox option for %q", f.Label)
				}
			}
		case FieldNumber:
			if _, err := strconv.ParseFloat(value, 64); err != nil {
				return fmt.Errorf("invalid number for %q", f.Label)
			}
		case FieldDate:
			if _, err := time.Parse("2006-01-02", value); err != nil {
				return fmt.Errorf("invalid date for %q", f.Label)
			}
		case FieldEmail:
			if !strings.Contains(value, "@") {
				return fmt.Errorf("invalid email for %q", f.Label)
			}
		}
	}
	return nil
}

// optionContains reports whether options contains value.
func optionContains(options []Option, value string) bool {
	for _, o := range options {
		if o.Value == value {
			return true
		}
	}
	return false
}

// parseCreateForm converts HTTP form values to a create-form input.
func parseCreateForm(values map[string][]string, userID int64) (CreateFormInput, error) {
	input := CreateFormInput{
		Title:       strings.TrimSpace(first(values, "title")),
		Description: strings.TrimSpace(first(values, "description")),
		CreatedBy:   userID,
	}
	if input.Title == "" {
		return input, errors.New("title is required")
	}

	types := values["field_type"]
	labels := values["field_label"]
	defaults := values["field_default"]
	options := values["field_options"]
	required := values["field_required"]
	requiredSet := make(map[int]bool)
	for _, raw := range required {
		if i, err := strconv.Atoi(raw); err == nil {
			requiredSet[i] = true
		}
	}

	for i, rawType := range types {
		label := strings.TrimSpace(at(labels, i))
		if label == "" {
			continue
		}
		ft := FieldType(rawType)
		if !ValidFieldType(ft) {
			return input, fmt.Errorf("unsupported field type %q", rawType)
		}
		field := CreateFieldInput{
			Type:         ft,
			Label:        label,
			Required:     requiredSet[i],
			DefaultValue: strings.TrimSpace(at(defaults, i)),
			SortOrder:    len(input.Fields),
		}
		if ft == FieldSelect || ft == FieldCheckboxes {
			field.Options = parseOptionsCSV(at(options, i))
			if len(field.Options) == 0 {
				return input, fmt.Errorf("field %q needs at least one option", label)
			}
		}
		input.Fields = append(input.Fields, field)
	}
	if len(input.Fields) == 0 {
		return input, errors.New("at least one field is required")
	}
	return input, nil
}

// parseOptionsCSV parses comma-separated option values and labels.
func parseOptionsCSV(raw string) []CreateOptionInput {
	seen := map[string]bool{}
	var out []CreateOptionInput
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, CreateOptionInput{Value: value, Label: NormalizeOptionLabel(value, ""), SortOrder: len(out)})
	}
	return out
}

// first returns the first form value by key.
func first(values map[string][]string, key string) string {
	return at(values[key], 0)
}

// at returns values[i] or an empty string.
func at(values []string, i int) string {
	if i < 0 || i >= len(values) {
		return ""
	}
	return values[i]
}

// parseAnswerValues extracts f_<field-id> values from a submitted form.
func parseAnswerValues(values map[string][]string) map[int64]string {
	out := make(map[int64]string)
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasPrefix(key, "f_") || key == "f_" {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(key, "f_"), 10, 64)
		if err != nil {
			continue
		}
		parts := make([]string, 0, len(values[key]))
		for _, v := range values[key] {
			v = strings.TrimSpace(v)
			if v != "" {
				parts = append(parts, v)
			}
		}
		out[id] = strings.Join(parts, ",")
	}
	return out
}

// boolValue returns a DB-friendly boolean value.
func boolValue(v bool) any {
	if v {
		return 1
	}
	return 0
}

// scanBool converts driver-specific bool representations to bool.
func scanBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case int:
		return x != 0
	case int32:
		return x != 0
	case []byte:
		return string(x) == "1" || strings.EqualFold(string(x), "true")
	case string:
		return x == "1" || strings.EqualFold(x, "true")
	default:
		return false
	}
}

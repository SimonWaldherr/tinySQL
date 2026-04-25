package main

import "strings"

const timeLayout = "2006-01-02T15:04:05Z07:00"

// Role describes the simple role-based authorization model.
type Role string

const (
	RoleAdmin  Role = "admin"
	RoleEditor Role = "editor"
	RoleViewer Role = "viewer"
	RoleUser   Role = "user"
)

// FieldType describes the supported HTML form control types.
type FieldType string

const (
	FieldText       FieldType = "text"
	FieldTextarea   FieldType = "textarea"
	FieldSelect     FieldType = "select"
	FieldCheckboxes FieldType = "checkboxes"
	FieldDate       FieldType = "date"
	FieldNumber     FieldType = "number"
	FieldEmail      FieldType = "email"
	FieldPassword   FieldType = "password"
)

// User represents an authenticated application user.
type User struct {
	ID           int64  `json:"id"`
	Username     string `json:"username"`
	DisplayName  string `json:"display_name"`
	Role         Role   `json:"role"`
	Active       bool   `json:"active"`
	CreatedAt    string `json:"created_at"`
	PasswordHash string `json:"-"`
}

// Form represents a form definition.
type Form struct {
	ID          int64  `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	CreatedBy   int64  `json:"created_by"`
	CreatedAt   string `json:"created_at"`
	AllowGuest  bool   `json:"allow_guest"`
	OpensAt     string `json:"opens_at,omitempty"`
	ClosesAt    string `json:"closes_at,omitempty"`
}

// Field represents a stable field definition within a form.
type Field struct {
	ID           int64     `json:"id"`
	FormID       int64     `json:"form_id"`
	Type         FieldType `json:"type"`
	Label        string    `json:"label"`
	Required     bool      `json:"required"`
	DefaultValue string    `json:"default_value"`
	SortOrder    int       `json:"sort_order"`
	Options      []Option  `json:"options,omitempty"`
}

// Option represents one selectable value for select and checkbox fields.
type Option struct {
	ID        int64  `json:"id"`
	FieldID   int64  `json:"field_id"`
	Value     string `json:"value"`
	Label     string `json:"label"`
	SortOrder int    `json:"sort_order"`
}

// FormDetail is a form including its fields and options.
type FormDetail struct {
	Form   Form    `json:"form"`
	Fields []Field `json:"fields"`
}

// AnswerValue is one field value in a submission.
type AnswerValue struct {
	FieldID int64  `json:"field_id"`
	Value   string `json:"value"`
}

// AnswerRow is a display-friendly answer row.
type AnswerRow struct {
	ID            int64    `json:"id"`
	UserID        int64    `json:"user_id"`
	SubmitterName string   `json:"submitter_name"`
	CreatedAt     string   `json:"created_at"`
	Cells         []Cell   `json:"cells"`
	Values        []string `json:"values"`
}

// Cell is one rendered answer cell.
type Cell struct {
	FieldID int64  `json:"field_id"`
	Label   string `json:"label"`
	Value   string `json:"value"`
}

// CreateFormInput is the validated input for creating a form.
type CreateFormInput struct {
	Title       string
	Description string
	CreatedBy   int64
	AllowGuest  bool
	OpensAt     string
	ClosesAt    string
	Fields      []CreateFieldInput
}

// CreateFieldInput is the validated input for creating a field.
type CreateFieldInput struct {
	Type         FieldType
	Label        string
	Required     bool
	DefaultValue string
	SortOrder    int
	Options      []CreateOptionInput
}

// CreateOptionInput is the validated input for creating one field option.
type CreateOptionInput struct {
	Value     string
	Label     string
	SortOrder int
}

// SaveAnswerInput is the validated input for saving a submission.
type SaveAnswerInput struct {
	FormID        int64
	UserID        int64
	SubmitterName string
	Values        map[int64]string
}

// IsInputField reports whether a field type maps to a normal HTML input element.
func IsInputField(t FieldType) bool {
	switch t {
	case FieldText, FieldDate, FieldNumber, FieldEmail, FieldPassword:
		return true
	default:
		return false
	}
}

// ValidFieldType reports whether t is supported by Formigo.
func ValidFieldType(t FieldType) bool {
	switch t {
	case FieldText, FieldTextarea, FieldSelect, FieldCheckboxes, FieldDate, FieldNumber, FieldEmail, FieldPassword:
		return true
	default:
		return false
	}
}

// NormalizeOptionLabel returns value as label when no explicit label was provided.
func NormalizeOptionLabel(value, label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return strings.TrimSpace(value)
	}
	return label
}

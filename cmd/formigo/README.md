# Formigo

Formigo is a single-binary form application written in Go with embedded HTML templates, Bootstrap UI, SQLite by default, optional Microsoft SQL Server support, sessions, CSRF protection, and a simple role-based authorization model.

## Roles

- `admin`: user administration, form creation, answer viewing, form submission
- `editor`: form creation, answer viewing, form submission
- `viewer`: answer viewing only
- `user`: form submission only

## Build

```bash
go mod tidy
go build -o formigo .
```

## Run with SQLite

```bash
./formigo -addr :8080 -dsn "file:formigo.db"
```

The first run creates an admin user automatically.

Default credentials:

```text
admin / admin123
```

Set your own initial admin credentials:

```bash
FORMIGO_ADMIN_USER=admin FORMIGO_ADMIN_PASSWORD='change-me-now' ./formigo
```

## Run with SQL Server

```bash
./formigo -dsn "sqlserver://user:password@localhost:1433?database=formigo&encrypt=disable"
```

## API

Authenticated API requests use the same session cookie as the UI. State-changing API calls must include the CSRF token in the `X-CSRF-Token` header.

```http
GET /api/forms
GET /api/forms/{id}/answers
POST /api/forms/{id}/answers
```

Example answer payload:

```json
{
  "submitter_name": "Max Mustermann",
  "values": {
    "1": "Text",
    "2": "Option 1",
    "3": "A,B"
  }
}
```

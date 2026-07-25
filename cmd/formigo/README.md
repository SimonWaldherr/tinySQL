# Formigo

Single-binary form app: embedded templates, Bootstrap UI, tinySQL storage (or
Microsoft SQL Server), sessions, CSRF protection, role-based authorization.

Roles: `admin` (user administration, form creation, answer viewing,
submission), `editor` (form creation, answer viewing, submission), `viewer`
(answer viewing), `user` (submission).

## Run

```bash
go mod tidy && go build -o formigo .

# tinySQL file-backed (default DSN), in-memory, SQL Server
./formigo -addr :8080 -dsn "file:formigo.db?autosave=1"
./formigo -dsn "mem://"
./formigo -dsn "sqlserver://user:password@localhost:1433?database=formigo&encrypt=disable"
```

`sqlserver://` or `server=` in the DSN selects the SQL Server dialect, anything
else tinySQL. The first run creates the admin user; `-secure-cookie` marks
session cookies `Secure` for HTTPS.

| Flag | Env | Default |
|---|---|---|
| `-admin-user` | `FORMIGO_ADMIN_USER` | `admin` |
| `-admin-password` | `FORMIGO_ADMIN_PASSWORD` | `admin123` |
| `-secure-cookie` | `FORMIGO_SECURE_COOKIE` | `false` |

## API

Auth uses the UI session cookie; state-changing calls need the CSRF token in
the `X-CSRF-Token` header.

```http
GET  /api/forms
GET  /api/forms/{id}/answers
POST /api/forms/{id}/answers
```

Payload:

```json
{"submitter_name": "Max", "values": {"1": "Text", "2": "Option 1", "3": "A,B"}}
```

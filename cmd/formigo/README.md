# Formigo

Part of [tinySQL](../../README.md). Formigo is an application example; consult
the root guide for tinySQL capabilities and storage limitations.

Single-binary form app: embedded templates, Bootstrap UI, tinySQL storage (or
Microsoft SQL Server), sessions, CSRF protection, role-based authorization.

Roles: `admin` (user administration, form creation, answer viewing,
submission), `editor` (form creation, answer viewing, submission), `viewer`
(answer viewing), `user` (submission).

## Run

```bash
go mod tidy && go build -o formigo .

# tinySQL file-backed (default DSN), in-memory, SQL Server
./formigo -dsn "file:formigo.db?autosave=1"
./formigo -dsn "mem://"
./formigo -dsn "sqlserver://user:password@localhost:1433?database=formigo&encrypt=disable"
```

`sqlserver://` or `server=` in the DSN selects the SQL Server dialect, anything
else tinySQL. The first run creates the admin user; `-secure-cookie` marks
session cookies `Secure` for HTTPS.

Formigo listens on `127.0.0.1:8080` by default. Pass `-addr :8080` to accept
connections from other machines — do that only behind TLS, and change the
admin password first.

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

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

`sqlserver://` or `server=` in the DSN selects the SQL Server dialect; other
DSNs use tinySQL. The first run creates the administrator. Later changes to
the credential flags or environment do not replace an existing password.

> **Warning:** The default configuration is a local demo only. On first
> bootstrap, Formigo logs the initial administrator password; when the bundled
> demo password is used, the login page also displays it. Do not bind that
> configuration to a public or shared network.

For any non-local deployment, set a non-default
`FORMIGO_ADMIN_PASSWORD` before the first start, terminate HTTPS in front of
the application, and enable secure cookies:

```bash
FORMIGO_ADMIN_USER=formigo-admin \
FORMIGO_ADMIN_PASSWORD='a-long-unique-secret' \
FORMIGO_SECURE_COOKIE=true \
./formigo -addr 127.0.0.1:8080 -secure-cookie
```

Keep Formigo bound to loopback and let the HTTPS proxy control public access.
The application itself serves HTTP; `-secure-cookie` only instructs browsers to
send session cookies over HTTPS.

| Flag | Env | Default |
|---|---|---|
| `-admin-user` | `FORMIGO_ADMIN_USER` | `admin` |
| `-admin-password` | `FORMIGO_ADMIN_PASSWORD` | unsafe demo credential |
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

-- statement
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('admin','editor','viewer','user')),
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- statement
CREATE TABLE IF NOT EXISTS forms (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  created_by INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  FOREIGN KEY (created_by) REFERENCES users(id)
);

-- statement
CREATE TABLE IF NOT EXISTS form_fields (
  id INTEGER PRIMARY KEY,
  form_id INTEGER NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('text','textarea','select','checkboxes','date','number','email','password')),
  label TEXT NOT NULL,
  required INTEGER NOT NULL DEFAULT 0,
  default_value TEXT NOT NULL DEFAULT '',
  sort_order INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (form_id) REFERENCES forms(id) ON DELETE CASCADE
);

-- statement
CREATE INDEX IF NOT EXISTS idx_form_fields_form_order ON form_fields(form_id, sort_order, id);

-- statement
CREATE TABLE IF NOT EXISTS field_options (
  id INTEGER PRIMARY KEY,
  field_id INTEGER NOT NULL,
  value TEXT NOT NULL,
  label TEXT NOT NULL,
  sort_order INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (field_id) REFERENCES form_fields(id) ON DELETE CASCADE,
  UNIQUE (field_id, value)
);

-- statement
CREATE INDEX IF NOT EXISTS idx_field_options_order ON field_options(field_id, sort_order, id);

-- statement
CREATE TABLE IF NOT EXISTS submissions (
  id INTEGER PRIMARY KEY,
  form_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  submitter_name TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  FOREIGN KEY (form_id) REFERENCES forms(id),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- statement
CREATE INDEX IF NOT EXISTS idx_submissions_form_created ON submissions(form_id, created_at, id);

-- statement
CREATE TABLE IF NOT EXISTS submission_values (
  submission_id INTEGER NOT NULL,
  field_id INTEGER NOT NULL,
  value TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (submission_id, field_id),
  FOREIGN KEY (submission_id) REFERENCES submissions(id) ON DELETE CASCADE,
  FOREIGN KEY (field_id) REFERENCES form_fields(id)
);

-- statement
CREATE TABLE IF NOT EXISTS sessions (
  token_hash TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  csrf_token TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- statement
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

-- statement
CREATE VIEW IF NOT EXISTS v_submission_details AS
SELECT
  s.id AS submission_id,
  s.form_id,
  s.user_id,
  u.username,
  u.display_name,
  s.submitter_name,
  s.created_at AS submitted_at,
  ff.id AS field_id,
  ff.label AS field_label,
  ff.type AS field_type,
  sv.value
FROM submissions s
JOIN users u ON u.id = s.user_id
JOIN submission_values sv ON sv.submission_id = s.id
JOIN form_fields ff ON ff.id = sv.field_id;

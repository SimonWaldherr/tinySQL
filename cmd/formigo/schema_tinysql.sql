-- statement
CREATE TABLE IF NOT EXISTS users (
  id INT,
  username TEXT,
  display_name TEXT,
  password_hash TEXT,
  role TEXT,
  active INT,
  created_at TEXT
);

-- statement
CREATE TABLE IF NOT EXISTS forms (
  id INT,
  title TEXT,
  description TEXT,
  created_by INT,
  created_at TEXT,
  allow_guest INT,
  opens_at TEXT,
  closes_at TEXT
);

-- statement
CREATE TABLE IF NOT EXISTS form_fields (
  id INT,
  form_id INT,
  type TEXT,
  label TEXT,
  required INT,
  default_value TEXT,
  sort_order INT
);

-- statement
CREATE TABLE IF NOT EXISTS field_options (
  id INT,
  field_id INT,
  value TEXT,
  label TEXT,
  sort_order INT
);

-- statement
CREATE TABLE IF NOT EXISTS submissions (
  id INT,
  form_id INT,
  user_id INT,
  submitter_name TEXT,
  created_at TEXT
);

-- statement
CREATE TABLE IF NOT EXISTS submission_values (
  submission_id INT,
  field_id INT,
  value TEXT
);

-- statement
CREATE TABLE IF NOT EXISTS sessions (
  token_hash TEXT,
  user_id INT,
  csrf_token TEXT,
  expires_at TEXT
);

-- statement
ALTER TABLE forms ADD COLUMN allow_guest INT;

-- statement
ALTER TABLE forms ADD COLUMN opens_at TEXT;

-- statement
ALTER TABLE forms ADD COLUMN closes_at TEXT;

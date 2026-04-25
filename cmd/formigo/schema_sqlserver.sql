-- statement
IF OBJECT_ID('dbo.sessions', 'U') IS NULL
CREATE TABLE dbo.sessions (
  token_hash NVARCHAR(128) NOT NULL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  csrf_token NVARCHAR(128) NOT NULL,
  expires_at NVARCHAR(40) NOT NULL,
  created_at NVARCHAR(40) NOT NULL DEFAULT (CONVERT(varchar(19), SYSUTCDATETIME(), 126) + 'Z')
);

-- statement
IF OBJECT_ID('dbo.users', 'U') IS NULL
CREATE TABLE dbo.users (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  username NVARCHAR(190) NOT NULL UNIQUE,
  display_name NVARCHAR(190) NOT NULL,
  password_hash NVARCHAR(255) NOT NULL,
  role NVARCHAR(20) NOT NULL CHECK (role IN ('admin','editor','viewer','user')),
  active BIT NOT NULL DEFAULT 1,
  created_at NVARCHAR(40) NOT NULL DEFAULT (CONVERT(varchar(19), SYSUTCDATETIME(), 126) + 'Z')
);

-- statement
IF OBJECT_ID('dbo.forms', 'U') IS NULL
CREATE TABLE dbo.forms (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  title NVARCHAR(255) NOT NULL,
  description NVARCHAR(MAX) NOT NULL DEFAULT '',
  created_by BIGINT NOT NULL,
  created_at NVARCHAR(40) NOT NULL DEFAULT (CONVERT(varchar(19), SYSUTCDATETIME(), 126) + 'Z'),
  CONSTRAINT fk_forms_created_by FOREIGN KEY (created_by) REFERENCES dbo.users(id)
);

-- statement
IF OBJECT_ID('dbo.form_fields', 'U') IS NULL
CREATE TABLE dbo.form_fields (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  form_id BIGINT NOT NULL,
  type NVARCHAR(30) NOT NULL CHECK (type IN ('text','textarea','select','checkboxes','date','number','email','password')),
  label NVARCHAR(255) NOT NULL,
  required BIT NOT NULL DEFAULT 0,
  default_value NVARCHAR(MAX) NOT NULL DEFAULT '',
  sort_order INT NOT NULL DEFAULT 0,
  CONSTRAINT fk_form_fields_form FOREIGN KEY (form_id) REFERENCES dbo.forms(id) ON DELETE CASCADE
);

-- statement
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_form_fields_form_order')
CREATE INDEX idx_form_fields_form_order ON dbo.form_fields(form_id, sort_order, id);

-- statement
IF OBJECT_ID('dbo.field_options', 'U') IS NULL
CREATE TABLE dbo.field_options (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  field_id BIGINT NOT NULL,
  value NVARCHAR(255) NOT NULL,
  label NVARCHAR(255) NOT NULL,
  sort_order INT NOT NULL DEFAULT 0,
  CONSTRAINT fk_field_options_field FOREIGN KEY (field_id) REFERENCES dbo.form_fields(id) ON DELETE CASCADE,
  CONSTRAINT uq_field_options_value UNIQUE (field_id, value)
);

-- statement
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_field_options_order')
CREATE INDEX idx_field_options_order ON dbo.field_options(field_id, sort_order, id);

-- statement
IF OBJECT_ID('dbo.submissions', 'U') IS NULL
CREATE TABLE dbo.submissions (
  id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
  form_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  submitter_name NVARCHAR(190) NOT NULL,
  created_at NVARCHAR(40) NOT NULL DEFAULT (CONVERT(varchar(19), SYSUTCDATETIME(), 126) + 'Z'),
  CONSTRAINT fk_submissions_form FOREIGN KEY (form_id) REFERENCES dbo.forms(id),
  CONSTRAINT fk_submissions_user FOREIGN KEY (user_id) REFERENCES dbo.users(id)
);

-- statement
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_submissions_form_created')
CREATE INDEX idx_submissions_form_created ON dbo.submissions(form_id, created_at, id);

-- statement
IF OBJECT_ID('dbo.submission_values', 'U') IS NULL
CREATE TABLE dbo.submission_values (
  submission_id BIGINT NOT NULL,
  field_id BIGINT NOT NULL,
  value NVARCHAR(MAX) NOT NULL DEFAULT '',
  CONSTRAINT pk_submission_values PRIMARY KEY (submission_id, field_id),
  CONSTRAINT fk_submission_values_submission FOREIGN KEY (submission_id) REFERENCES dbo.submissions(id) ON DELETE CASCADE,
  CONSTRAINT fk_submission_values_field FOREIGN KEY (field_id) REFERENCES dbo.form_fields(id)
);

-- statement
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_sessions_user')
ALTER TABLE dbo.sessions ADD CONSTRAINT fk_sessions_user FOREIGN KEY (user_id) REFERENCES dbo.users(id) ON DELETE CASCADE;

-- statement
CREATE OR ALTER VIEW dbo.v_submission_details AS
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
FROM dbo.submissions s
JOIN dbo.users u ON u.id = s.user_id
JOIN dbo.submission_values sv ON sv.submission_id = s.id
JOIN dbo.form_fields ff ON ff.id = sv.field_id;

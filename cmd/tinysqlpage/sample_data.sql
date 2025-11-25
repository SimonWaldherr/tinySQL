-- Seed data for the tinysqlpage demo.
-- Executed once when the server boots.

CREATE TABLE customers (
    id INT,
    name TEXT,
    segment TEXT,
    city TEXT,
    active BOOL
);

INSERT INTO customers VALUES
    (1, 'Atlas Analytics',  'Startup',   'Berlin',      true),
    (2, 'Beacon Retail',    'Enterprise','New York',    true),
    (3, 'Cloudcraft Labs',  'Scale-up',  'Stockholm',   true),
    (4, 'Delta Freight',    'SMB',       'Rotterdam',   false),
    (5, 'Evergreen Health', 'Enterprise','Zurich',      true);

CREATE TABLE ui_context (
    id INT
);

INSERT INTO ui_context VALUES (1);

CREATE TABLE invoices (
    id INT,
    customer_id INT,
    amount FLOAT,
    status TEXT,
    issued_at TEXT,
    due_date TEXT
);

INSERT INTO invoices VALUES
    (101, 1, 3200.00, 'PAID', '2025-10-02', '2025-10-17'),
    (102, 2, 8700.00, 'PAID', '2025-09-25', '2025-10-10'),
    (103, 3, 5400.00, 'OPEN', '2025-10-12', '2025-10-27'),
    (104, 1, 3300.00, 'OPEN', '2025-10-18', '2025-11-02'),
    (105, 5, 12900.00, 'PAID', '2025-09-30', '2025-10-15'),
    (106, 4, 2100.00, 'OVERDUE', '2025-09-02', '2025-09-17'),
    (107, 2, 9200.00, 'PAID', '2025-10-05', '2025-10-20');

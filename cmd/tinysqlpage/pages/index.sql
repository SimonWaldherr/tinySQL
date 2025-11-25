-- Landing page for tinysqlpage.
-- Every SELECT that returns a "component" column is rendered into HTML.

SELECT
    'hero'   AS component,
    'tinySQLPage' AS title,
    'A SQL-first UI demo powered entirely by tinySQL' AS subtitle
FROM ui_context;

SELECT
    'text' AS component,
    'Change the SQL in cmd/tinysqlpage/pages/index.sql and refresh the browser to iterate instantly.' AS content
FROM ui_context
UNION ALL
SELECT
    'text' AS component,
    'Each SELECT describes a UI component. The database remains the only source of truth.' AS content
FROM ui_context;

SELECT
    'stat_list' AS component,
    'Active customers' AS label,
    COUNT(*) AS value,
    'Currently enabled accounts' AS info
FROM customers WHERE active = true
UNION ALL
SELECT
    'stat_list',
    'Open invoices',
    COUNT(*) AS value,
    'Awaiting payment'
FROM invoices WHERE status = 'OPEN'
UNION ALL
SELECT
    'stat_list',
    'Paid MRR (k$)',
    ROUND(SUM(amount)/1000, 2) AS value,
    'Converted to thousands'
FROM invoices WHERE status = 'PAID';

SELECT
    'table' AS component,
    'Recent invoices' AS title,
    i.id          AS invoice,
    c.name        AS customer_name,
    c.segment     AS segment,
    i.status      AS status,
    i.amount      AS amount,
    i.issued_at   AS issued_at
FROM invoices i
JOIN customers c ON c.id = i.customer_id
ORDER BY issued_at DESC
LIMIT 5;

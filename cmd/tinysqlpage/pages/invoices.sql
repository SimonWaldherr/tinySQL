-- Invoices page: demonstrates JOINs, filters and table components

SELECT
  'hero' AS component,
  'Invoices' AS title,
  'All invoices, join to customers and show status/amount' AS subtitle
FROM ui_context;

-- Summary stats
SELECT
  'stat_list' AS component,
  'Invoice summary' AS title,
  COUNT(*) AS value,
  'Total invoices' AS info
FROM invoices
UNION ALL
SELECT
  'stat_list',
  'Invoice summary',
  SUM(CASE WHEN status IN ('OPEN','OVERDUE') THEN amount ELSE 0 END) AS value,
  'Outstanding amount' AS info
FROM invoices;

-- Detailed recent invoices
SELECT
  'table' AS component,
  'Recent invoices' AS title,
  i.id AS invoice,
  c.name AS customer,
  i.status AS status,
  i.amount AS amount,
  i.issued_at AS issued_at,
  i.due_date AS due_date
FROM invoices i
JOIN customers c ON c.id = i.customer_id
ORDER BY i.issued_at DESC
LIMIT 25;

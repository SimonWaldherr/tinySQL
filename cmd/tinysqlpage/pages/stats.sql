-- Global stats page for tinysqlpage demo

SELECT
  'hero' AS component,
  'Statistics' AS title,
  'Overview of dataset with aggregates' AS subtitle
FROM ui_context;

SELECT
  'stat_list' AS component,
  'Quick metrics' AS title,
  (SELECT COUNT(*) FROM customers) AS value,
  'Total customers' AS info
FROM ui_context
;

SELECT
  'stat_list' AS component,
  'Quick metrics' AS title,
  (SELECT COUNT(*) FROM invoices WHERE status = 'OPEN') AS value,
  'Open invoices' AS info
FROM ui_context
;

SELECT
  'stat_list' AS component,
  'Quick metrics' AS title,
  (SELECT SUM(amount) FROM invoices WHERE status IN ('OPEN','OVERDUE')) AS value,
  'Outstanding amount' AS info
FROM ui_context
;

-- Segments page: GROUP BY and aggregates

SELECT
  'hero' AS component,
  'Segments' AS title,
  'Revenue and counts per customer segment' AS subtitle
FROM ui_context;

SELECT
  'table' AS component,
  'Segment breakdown' AS title,
  c.segment AS segment,
  COUNT(DISTINCT c.id) AS customers,
  ROUND(SUM(i.amount), 2) AS revenue,
  ROUND(AVG(i.amount), 2) AS avg_invoice
FROM customers c
LEFT JOIN invoices i ON i.customer_id = c.id
GROUP BY c.segment
ORDER BY revenue DESC;

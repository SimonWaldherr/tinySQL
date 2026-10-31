SELECT NOW() as timestamp;
SELECT EXTRACT('YEAR', NOW()) as year, EXTRACT('MONTH', NOW()) as month;
SELECT DATE_TRUNC('MONTH', NOW()) as month_start, EOMONTH(CURRENT_DATE()) as month_end;
SELECT IN_PERIOD('TODAY', CURRENT_DATE()) as today, IN_PERIOD('MTD', CURRENT_DATE()) as mtd;
SELECT UPPER('hello') as upper, LOWER('WORLD') as lower, LENGTH('test') as len;
SELECT REGEXP_MATCH('user@test.com', '.*@.*') as valid_email;
SELECT SPLIT('red,green,blue', ',') as colors;
SELECT ARRAY_LENGTH(SPLIT('a,b,c,d', ',')) as count;
SELECT FIRST(SPLIT('1,2,3', ',')) as first, LAST(SPLIT('1,2,3', ',')) as last;
SELECT ARRAY_JOIN(ARRAY_SORT(SPLIT('c,a,b', ',')), ' -> ') as sorted;
SELECT ROUND(PI() * POWER(10, 2), 2) as area;
SELECT COALESCE(NULL, 'default') as value;

-- Views can use CTEs in their stored definition.
CREATE TABLE orders (customer_id INT, amount INT, status TEXT);
INSERT INTO orders VALUES (1, 10, 'paid'), (1, 4, 'open'), (2, 7, 'paid');
CREATE VIEW paid_customer_totals AS
WITH paid_orders AS (
  SELECT customer_id, amount
  FROM orders
  WHERE status = 'paid'
)
SELECT customer_id, SUM(amount) AS total
FROM paid_orders
GROUP BY customer_id;
SELECT customer_id, total FROM paid_customer_totals ORDER BY customer_id;

-- Materialized views keep a cache and can be refreshed manually or by policy.
CREATE MATERIALIZED VIEW paid_customer_totals_mv AS
WITH paid_orders AS (
  SELECT customer_id, amount
  FROM orders
  WHERE status = 'paid'
)
SELECT customer_id, SUM(amount) AS total
FROM paid_orders
GROUP BY customer_id
REFRESH ON STALE AFTER 6 HOURS
REFRESH EVERY 30 MINUTES
REFRESH DAILY AT '02:00' TIMEZONE 'Europe/Berlin'
INVALIDATE ON CHANGE
WITH DATA;
REFRESH MATERIALIZED VIEW paid_customer_totals_mv;
SELECT name, cache_table_name, last_refresh_at, refresh_every_ms, daily_at, invalidate_on_change
FROM catalog.materialized_views;
SELECT object_name, depends_on_name, depends_on_type
FROM sys.dependencies
WHERE object_name = 'paid_customer_totals_mv';

-- Convert between normal and materialized views without rewriting the query.
ALTER MATERIALIZED VIEW paid_customer_totals_mv TO VIEW;
ALTER VIEW paid_customer_totals_mv MATERIALIZE REFRESH EVERY 15 MINUTES WITH DATA;

-- Schema-qualified names are reflected in system views.
CREATE TABLE sales.orders (id INT PRIMARY KEY, amount INT);
INSERT INTO sales.orders VALUES (1, 10), (2, 120);
CREATE VIEW sales.large_orders AS
SELECT id, amount FROM sales.orders WHERE amount >= 100;
SELECT id, amount FROM sales.large_orders;
SELECT schema, name, full_name FROM sys.tables WHERE schema = 'sales';

-- SQLite-compatible metadata helps existing embedded-DB tooling discover objects.
SELECT type, name, tbl_name
FROM sqlite_schema
WHERE name = 'orders';
SELECT name, sql
FROM sqlite_master
WHERE type = 'view';
PRAGMA table_info('sales.orders');
PRAGMA table_list;
PRAGMA database_list;
PRAGMA journal_mode;
PRAGMA integrity_check;

-- EXPLAIN returns a compact logical plan without executing the query.
EXPLAIN
WITH recent AS (
  SELECT id, amount FROM sales.orders WHERE amount > 10
)
SELECT id
FROM recent
WHERE amount < 200
ORDER BY amount
LIMIT 5;

-- Column-level constraints are enforced.
CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT UNIQUE);
INSERT INTO accounts VALUES (1, 'a@example.test');

-- Unified object status across tables, views, materialized views, and jobs.
CREATE JOB refresh_paid_totals SCHEDULE INTERVAL 900000 AS
REFRESH MATERIALIZED VIEW paid_customer_totals_mv;
SELECT object_type, name, status, rows, is_stale, last_refresh_at, next_run_at
FROM sys.objects
ORDER BY object_type, name;

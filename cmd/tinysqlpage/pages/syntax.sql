-- title: SQL Syntax Reference
-- nav_label: Syntax
-- nav_order: 10

-- ============================================================
-- SQL Syntax Documentation Page for tinySQL
-- ============================================================

SELECT
    'hero'   AS component,
    'SQL Syntax Reference' AS title,
    'Complete documentation for the tinySQL dialect' AS subtitle
FROM ui_context;

-- ============================================================
-- OVERVIEW SECTION
-- ============================================================

SELECT
    'text' AS component,
    'tinySQL implements a practical subset of SQL designed for education and lightweight applications. This reference covers all supported statements, clauses, and functions.' AS content
FROM ui_context;

-- ============================================================
-- DATA DEFINITION LANGUAGE (DDL)
-- ============================================================

SELECT
    'table' AS component,
    '📋 Data Definition Language (DDL)' AS title,
    'CREATE TABLE' AS statement,
    'CREATE [TEMP] TABLE [IF NOT EXISTS] name (col1 TYPE, col2 TYPE, ...)' AS syntax,
    'Creates a new table with specified columns' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CREATE TABLE ... AS SELECT' AS statement,
    'CREATE TABLE name AS SELECT ...' AS syntax,
    'Creates a table from a query result' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DROP TABLE' AS statement,
    'DROP TABLE [IF EXISTS] name' AS syntax,
    'Removes a table from the database' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ALTER TABLE' AS statement,
    'ALTER TABLE name ADD COLUMN col TYPE' AS syntax,
    'Adds a new column to an existing table' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CREATE VIEW' AS statement,
    'CREATE [OR REPLACE] VIEW name AS SELECT ...' AS syntax,
    'Creates a virtual table from a query' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DROP VIEW' AS statement,
    'DROP VIEW [IF EXISTS] name' AS syntax,
    'Removes a view from the database' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CREATE INDEX' AS statement,
    'CREATE [UNIQUE] INDEX name ON table(col1, col2, ...)' AS syntax,
    'Creates an index on table columns' AS description
FROM ui_context;

-- ============================================================
-- SUPPORTED DATA TYPES
-- ============================================================

SELECT
    'table' AS component,
    '🔢 Supported Data Types' AS title,
    'INT / INTEGER' AS type,
    'Integer numbers' AS description,
    '42, -17, 0' AS examples
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'FLOAT / REAL' AS type,
    'Floating-point numbers' AS description,
    '3.14, -2.5, 1.0e10' AS examples
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'TEXT / VARCHAR' AS type,
    'Variable-length strings' AS description,
    '''Hello'', ''World''' AS examples
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'BOOL / BOOLEAN' AS type,
    'Boolean true/false values' AS description,
    'true, false' AS examples
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DATE' AS type,
    'Date values' AS description,
    '''2024-12-25''' AS examples
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DATETIME / TIMESTAMP' AS type,
    'Date and time values' AS description,
    '''2024-12-25 14:30:00''' AS examples
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'JSON / JSONB' AS type,
    'JSON data' AS description,
    '''{"key": "value"}''' AS examples
FROM ui_context;

-- ============================================================
-- DATA MANIPULATION LANGUAGE (DML)
-- ============================================================

SELECT
    'table' AS component,
    '✏️ Data Manipulation Language (DML)' AS title,
    'INSERT' AS statement,
    'INSERT INTO table [(cols)] VALUES (vals), (vals), ...' AS syntax,
    'Inserts one or more rows into a table' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'UPDATE' AS statement,
    'UPDATE table SET col1=val1, col2=val2 [WHERE condition]' AS syntax,
    'Modifies existing rows in a table' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DELETE' AS statement,
    'DELETE FROM table [WHERE condition]' AS syntax,
    'Removes rows from a table' AS description
FROM ui_context;

-- ============================================================
-- SELECT STATEMENT
-- ============================================================

SELECT
    'table' AS component,
    '🔍 SELECT Statement Clauses' AS title,
    'SELECT' AS clause,
    'SELECT [DISTINCT] cols FROM table' AS syntax,
    'Specifies columns to retrieve, use * for all' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'FROM' AS clause,
    'FROM table [alias]' AS syntax,
    'Specifies the source table(s)' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'JOIN' AS clause,
    '[INNER|LEFT|RIGHT] JOIN table ON condition' AS syntax,
    'Combines rows from multiple tables' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'WHERE' AS clause,
    'WHERE condition' AS syntax,
    'Filters rows based on a condition' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'GROUP BY' AS clause,
    'GROUP BY col1, col2, ...' AS syntax,
    'Groups rows for aggregate calculations' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'HAVING' AS clause,
    'HAVING aggregate_condition' AS syntax,
    'Filters groups after aggregation' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ORDER BY' AS clause,
    'ORDER BY col [ASC|DESC], ...' AS syntax,
    'Sorts result rows' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LIMIT / OFFSET' AS clause,
    'LIMIT n [OFFSET m]' AS syntax,
    'Limits result set size and starting position' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'WITH (CTE)' AS clause,
    'WITH name AS (SELECT ...) SELECT ...' AS syntax,
    'Defines Common Table Expressions' AS description
FROM ui_context;

-- ============================================================
-- SET OPERATIONS
-- ============================================================

SELECT
    'table' AS component,
    '🔄 Set Operations' AS title,
    'UNION' AS operation,
    'SELECT ... UNION SELECT ...' AS syntax,
    'Combines results, removes duplicates' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'UNION ALL' AS operation,
    'SELECT ... UNION ALL SELECT ...' AS syntax,
    'Combines results, keeps duplicates' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'INTERSECT' AS operation,
    'SELECT ... INTERSECT SELECT ...' AS syntax,
    'Returns rows common to both queries' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'EXCEPT' AS operation,
    'SELECT ... EXCEPT SELECT ...' AS syntax,
    'Returns rows in first query but not second' AS description
FROM ui_context;

-- ============================================================
-- OPERATORS
-- ============================================================

SELECT
    'table' AS component,
    '⚡ Operators' AS title,
    'Arithmetic' AS category,
    '+ - * / %' AS operators,
    'Addition, subtraction, multiplication, division, modulo' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'Comparison' AS category,
    '= != <> < <= > >=' AS operators,
    'Equality and comparison operators' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'Logical' AS category,
    'AND OR NOT' AS operators,
    'Boolean logic operators' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'Special' AS category,
    'IS NULL, IS NOT NULL' AS operators,
    'NULL value checks' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'Pattern' AS category,
    'LIKE, NOT LIKE' AS operators,
    'Pattern matching with % and _ wildcards' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'Membership' AS category,
    'IN, NOT IN' AS operators,
    'Value list membership tests' AS description
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'Conditional' AS category,
    'CASE WHEN ... THEN ... ELSE ... END' AS operators,
    'Conditional expressions' AS description
FROM ui_context;

-- ============================================================
-- DATE AND TIME FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '⏰ Date and Time Functions' AS title,
    'NOW()' AS function,
    'Returns current date and time' AS description,
    'SELECT NOW()' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CURRENT_DATE()' AS function,
    'Returns current date' AS description,
    'SELECT CURRENT_DATE()' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CURRENT_TIME()' AS function,
    'Returns current time' AS description,
    'SELECT CURRENT_TIME()' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'YEAR(date)' AS function,
    'Extracts year from date' AS description,
    'SELECT YEAR(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MONTH(date)' AS function,
    'Extracts month from date' AS description,
    'SELECT MONTH(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DAY(date)' AS function,
    'Extracts day from date' AS description,
    'SELECT DAY(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'HOUR(date)' AS function,
    'Extracts hour from date/time' AS description,
    'SELECT HOUR(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MINUTE(date)' AS function,
    'Extracts minute from date/time' AS description,
    'SELECT MINUTE(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SECOND(date)' AS function,
    'Extracts second from date/time' AS description,
    'SELECT SECOND(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'QUARTER(date)' AS function,
    'Returns quarter (1-4) from date' AS description,
    'SELECT QUARTER(NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'EXTRACT(part, date)' AS function,
    'Extracts specified part from date' AS description,
    'SELECT EXTRACT(''YEAR'', NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DATE_ADD(date, n, unit)' AS function,
    'Adds interval to date' AS description,
    'SELECT DATE_ADD(NOW(), 7, ''DAY'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DATE_SUB(date, n, unit)' AS function,
    'Subtracts interval from date' AS description,
    'SELECT DATE_SUB(NOW(), 1, ''MONTH'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DATEDIFF(unit, d1, d2)' AS function,
    'Returns difference between dates' AS description,
    'SELECT DATEDIFF(''DAYS'', ''2024-01-01'', NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ADD_MONTHS(date, n)' AS function,
    'Adds/subtracts months from date' AS description,
    'SELECT ADD_MONTHS(NOW(), 3)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DATE_TRUNC(unit, date)' AS function,
    'Truncates date to specified unit' AS description,
    'SELECT DATE_TRUNC(''MONTH'', NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'EOMONTH(date [, offset])' AS function,
    'Returns end of month' AS description,
    'SELECT EOMONTH(CURRENT_DATE())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'IN_PERIOD(period, date)' AS function,
    'Checks if date is in period' AS description,
    'SELECT IN_PERIOD(''MTD'', NOW())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'STRFTIME(format, date)' AS function,
    'Formats date as string' AS description,
    'SELECT STRFTIME(''%Y-%m-%d'', NOW())' AS example
FROM ui_context;

-- ============================================================
-- STRING FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '📝 String Functions' AS title,
    'UPPER(str)' AS function,
    'Converts to uppercase' AS description,
    'SELECT UPPER(''hello'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LOWER(str)' AS function,
    'Converts to lowercase' AS description,
    'SELECT LOWER(''HELLO'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'INITCAP(str)' AS function,
    'Capitalizes first letter of each word' AS description,
    'SELECT INITCAP(''hello world'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LENGTH(str)' AS function,
    'Returns string length' AS description,
    'SELECT LENGTH(''hello'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CONCAT(s1, s2, ...)' AS function,
    'Concatenates strings' AS description,
    'SELECT CONCAT(''a'', ''b'', ''c'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CONCAT_WS(sep, s1, s2, ...)' AS function,
    'Concatenates with separator' AS description,
    'SELECT CONCAT_WS(''-'', ''a'', ''b'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SUBSTRING(str, start, len)' AS function,
    'Extracts substring' AS description,
    'SELECT SUBSTRING(''hello'', 1, 3)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LEFT(str, n)' AS function,
    'Returns leftmost n characters' AS description,
    'SELECT LEFT(''hello'', 2)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'RIGHT(str, n)' AS function,
    'Returns rightmost n characters' AS description,
    'SELECT RIGHT(''hello'', 2)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'TRIM(str)' AS function,
    'Removes leading/trailing spaces' AS description,
    'SELECT TRIM(''  hello  '')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LTRIM(str) / RTRIM(str)' AS function,
    'Removes leading/trailing spaces' AS description,
    'SELECT LTRIM(''  hello'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LPAD(str, len, pad)' AS function,
    'Pads string on the left' AS description,
    'SELECT LPAD(''42'', 5, ''0'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'RPAD(str, len, pad)' AS function,
    'Pads string on the right' AS description,
    'SELECT RPAD(''42'', 5, ''0'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'REPLACE(str, find, repl)' AS function,
    'Replaces occurrences' AS description,
    'SELECT REPLACE(''foo'', ''o'', ''a'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'REVERSE(str)' AS function,
    'Reverses string' AS description,
    'SELECT REVERSE(''hello'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'REPEAT(str, n)' AS function,
    'Repeats string n times' AS description,
    'SELECT REPEAT(''ab'', 3)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'INSTR(str, sub)' AS function,
    'Returns position of substring' AS description,
    'SELECT INSTR(''hello'', ''l'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SPLIT(str, delim)' AS function,
    'Splits string into array' AS description,
    'SELECT SPLIT(''a,b,c'', '','')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SPLIT_PART(str, delim, n)' AS function,
    'Returns nth part after split' AS description,
    'SELECT SPLIT_PART(''a,b,c'', '','', 2)' AS example
FROM ui_context;

-- ============================================================
-- REGEX FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '🔎 Regex Functions' AS title,
    'REGEXP_MATCH(str, pattern)' AS function,
    'Tests if string matches pattern' AS description,
    'SELECT REGEXP_MATCH(''test@mail.com'', ''.*@.*'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'REGEXP_EXTRACT(str, pattern)' AS function,
    'Extracts matching substring' AS description,
    'SELECT REGEXP_EXTRACT(''Order #123'', ''#[0-9]+'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'REGEXP_REPLACE(str, pat, repl)' AS function,
    'Replaces matching patterns' AS description,
    'SELECT REGEXP_REPLACE(''a1b2'', ''[0-9]'', ''X'')' AS example
FROM ui_context;

-- ============================================================
-- NUMERIC FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '🔢 Numeric Functions' AS title,
    'ABS(n)' AS function,
    'Returns absolute value' AS description,
    'SELECT ABS(-42)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SIGN(n)' AS function,
    'Returns sign (-1, 0, 1)' AS description,
    'SELECT SIGN(-15)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ROUND(n, d)' AS function,
    'Rounds to d decimal places' AS description,
    'SELECT ROUND(3.14159, 2)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'FLOOR(n)' AS function,
    'Rounds down to integer' AS description,
    'SELECT FLOOR(3.7)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'CEIL(n) / CEILING(n)' AS function,
    'Rounds up to integer' AS description,
    'SELECT CEIL(3.2)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'TRUNCATE(n, d) / TRUNC(n, d)' AS function,
    'Truncates to d decimal places' AS description,
    'SELECT TRUNCATE(3.14159, 2)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'POWER(base, exp) / POW(b, e)' AS function,
    'Returns base raised to power' AS description,
    'SELECT POWER(2, 8)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SQRT(n)' AS function,
    'Returns square root' AS description,
    'SELECT SQRT(16)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MOD(a, b)' AS function,
    'Returns remainder of division' AS description,
    'SELECT MOD(17, 5)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LOG(n) / LN(n)' AS function,
    'Returns natural logarithm' AS description,
    'SELECT LOG(100)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LOG10(n)' AS function,
    'Returns base-10 logarithm' AS description,
    'SELECT LOG10(1000)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LOG2(n)' AS function,
    'Returns base-2 logarithm' AS description,
    'SELECT LOG2(1024)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'EXP(n)' AS function,
    'Returns e raised to power n' AS description,
    'SELECT EXP(1)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'PI()' AS function,
    'Returns value of pi' AS description,
    'SELECT PI()' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SIN, COS, TAN, ASIN, ACOS, ATAN' AS function,
    'Trigonometric functions' AS description,
    'SELECT SIN(PI()/2)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'DEGREES(rad) / RADIANS(deg)' AS function,
    'Converts between degrees/radians' AS description,
    'SELECT DEGREES(PI())' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'RANDOM() / RAND()' AS function,
    'Returns random number [0,1)' AS description,
    'SELECT RANDOM()' AS example
FROM ui_context;

-- ============================================================
-- AGGREGATE FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '📊 Aggregate Functions' AS title,
    'COUNT(*) / COUNT(col)' AS function,
    'Counts rows or non-null values' AS description,
    'SELECT COUNT(*) FROM table' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'COUNT(DISTINCT col)' AS function,
    'Counts distinct non-null values' AS description,
    'SELECT COUNT(DISTINCT status) FROM orders' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SUM(col)' AS function,
    'Returns sum of values' AS description,
    'SELECT SUM(amount) FROM orders' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'AVG(col)' AS function,
    'Returns average of values' AS description,
    'SELECT AVG(price) FROM products' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MIN(col) / MAX(col)' AS function,
    'Returns minimum/maximum value' AS description,
    'SELECT MIN(date), MAX(date) FROM events' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MIN_BY(val, order) / MAX_BY(val, order)' AS function,
    'Returns value from row with min/max of order column' AS description,
    'SELECT MIN_BY(name, date) FROM events' AS example
FROM ui_context;

-- ============================================================
-- ARRAY FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '📋 Array Functions' AS title,
    'FIRST(arr)' AS function,
    'Returns first array element' AS description,
    'SELECT FIRST(SPLIT(''a,b,c'', '',''))' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LAST(arr)' AS function,
    'Returns last array element' AS description,
    'SELECT LAST(SPLIT(''a,b,c'', '',''))' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ARRAY_LENGTH(arr)' AS function,
    'Returns array size' AS description,
    'SELECT ARRAY_LENGTH(SPLIT(''a,b,c'', '',''))' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ARRAY_CONTAINS(arr, val)' AS function,
    'Checks if array contains value' AS description,
    'SELECT ARRAY_CONTAINS(SPLIT(''a,b,c'', '',''), ''b'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ARRAY_JOIN(arr, sep)' AS function,
    'Joins array elements with separator' AS description,
    'SELECT ARRAY_JOIN(SPLIT(''a,b'', '',''), ''-'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ARRAY_DISTINCT(arr)' AS function,
    'Returns unique elements' AS description,
    'SELECT ARRAY_DISTINCT(SPLIT(''a,b,a'', '',''))' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'ARRAY_SORT(arr)' AS function,
    'Returns sorted array' AS description,
    'SELECT ARRAY_SORT(SPLIT(''c,a,b'', '',''))' AS example
FROM ui_context;

-- ============================================================
-- WINDOW FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '🪟 Window Functions' AS title,
    'ROW_NUMBER() OVER(...)' AS function,
    'Assigns unique row numbers' AS description,
    'ROW_NUMBER() OVER (ORDER BY date)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LAG(col, n) OVER(...)' AS function,
    'Returns value from n rows before' AS description,
    'LAG(amount, 1) OVER (ORDER BY date)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LEAD(col, n) OVER(...)' AS function,
    'Returns value from n rows after' AS description,
    'LEAD(amount, 1) OVER (ORDER BY date)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'FIRST_VALUE(col) OVER(...)' AS function,
    'Returns first value in window' AS description,
    'FIRST_VALUE(name) OVER (ORDER BY score DESC)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LAST_VALUE(col) OVER(...)' AS function,
    'Returns last value in window' AS description,
    'LAST_VALUE(name) OVER (ORDER BY score)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MOVING_SUM(n, col) OVER(...)' AS function,
    'Calculates moving sum over n rows' AS description,
    'MOVING_SUM(3, amount) OVER (ORDER BY date)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'MOVING_AVG(n, col) OVER(...)' AS function,
    'Calculates moving average over n rows' AS description,
    'MOVING_AVG(3, amount) OVER (ORDER BY date)' AS example
FROM ui_context;

-- ============================================================
-- CONDITIONAL FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '🔀 Conditional Functions' AS title,
    'COALESCE(v1, v2, ...)' AS function,
    'Returns first non-null value' AS description,
    'SELECT COALESCE(NULL, ''default'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'NVL(val, default) / IFNULL' AS function,
    'Returns default if val is null' AS description,
    'SELECT NVL(name, ''Unknown'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'NULLIF(v1, v2)' AS function,
    'Returns null if values are equal' AS description,
    'SELECT NULLIF(5, 5)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'IF(cond, then, else) / IIF' AS function,
    'Conditional expression' AS description,
    'SELECT IF(x > 0, ''positive'', ''negative'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'GREATEST(v1, v2, ...)' AS function,
    'Returns maximum value' AS description,
    'SELECT GREATEST(1, 5, 3)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'LEAST(v1, v2, ...)' AS function,
    'Returns minimum value' AS description,
    'SELECT LEAST(1, 5, 3)' AS example
FROM ui_context;

-- ============================================================
-- CRYPTOGRAPHIC FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '🔐 Cryptographic Functions' AS title,
    'MD5(str)' AS function,
    'Returns MD5 hash' AS description,
    'SELECT MD5(''password'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SHA1(str)' AS function,
    'Returns SHA-1 hash' AS description,
    'SELECT SHA1(''password'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SHA256(str)' AS function,
    'Returns SHA-256 hash' AS description,
    'SELECT SHA256(''password'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'SHA512(str)' AS function,
    'Returns SHA-512 hash' AS description,
    'SELECT SHA512(''password'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'BASE64(str)' AS function,
    'Encodes string as base64' AS description,
    'SELECT BASE64(''hello'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'BASE64_DECODE(str)' AS function,
    'Decodes base64 string (aGVsbG8= = hello)' AS description,
    'SELECT BASE64_DECODE(''aGVsbG8='')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'HEX(str)' AS function,
    'Encodes string as hexadecimal' AS description,
    'SELECT HEX(''hello'')' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'UNHEX(str)' AS function,
    'Decodes hex string (48656C6C6F = Hello)' AS description,
    'SELECT UNHEX(''48656C6C6F'')' AS example
FROM ui_context;

-- ============================================================
-- TYPE AND UTILITY FUNCTIONS
-- ============================================================

SELECT
    'table' AS component,
    '🛠️ Type and Utility Functions' AS title,
    'CAST(val AS type)' AS function,
    'Converts value to specified type' AS description,
    'SELECT CAST(''123'' AS INTEGER)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'TYPEOF(val)' AS function,
    'Returns type name of value' AS description,
    'SELECT TYPEOF(42)' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'UUID()' AS function,
    'Generates a unique identifier' AS description,
    'SELECT UUID()' AS example
FROM ui_context
UNION ALL
SELECT
    'table' AS component,
    '' AS title,
    'VERSION()' AS function,
    'Returns tinySQL version' AS description,
    'SELECT VERSION()' AS example
FROM ui_context;

-- ============================================================
-- FOOTER
-- ============================================================

SELECT
    'text' AS component,
    'This documentation is auto-generated from the tinySQL engine. For more examples, see the FUNCTIONS.sql file in the repository.' AS content
FROM ui_context;

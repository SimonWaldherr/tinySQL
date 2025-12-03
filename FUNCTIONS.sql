-- tinySQL Function Examples
-- This file demonstrates all supported SQL functions in tinySQL
-- Execute these examples to learn the complete SQL dialect

-- ============================================================
-- DATE AND TIME FUNCTIONS
-- ============================================================

-- Current date and time
SELECT NOW() as current_timestamp;
SELECT CURRENT_DATE() as today;
SELECT CURRENT_TIME() as current_time;

-- Date part extraction
SELECT YEAR(NOW()) as current_year;
SELECT MONTH(NOW()) as current_month;
SELECT DAY(NOW()) as current_day;
SELECT HOUR(NOW()) as current_hour;
SELECT MINUTE(NOW()) as current_minute;
SELECT SECOND(NOW()) as current_second;
SELECT QUARTER(NOW()) as current_quarter;
SELECT DAYOFWEEK(NOW()) as day_of_week;
SELECT DAYOFYEAR(NOW()) as day_of_year;
SELECT WEEKOFYEAR(NOW()) as week_of_year;

-- NEW: EXTRACT function (alternative to individual functions)
SELECT EXTRACT('YEAR', NOW()) as year;
SELECT EXTRACT('MONTH', NOW()) as month;
SELECT EXTRACT('DAY', NOW()) as day;
SELECT EXTRACT('QUARTER', NOW()) as quarter;
SELECT EXTRACT('WEEK', NOW()) as week;
SELECT EXTRACT('DAYOFWEEK', NOW()) as dow;

-- Date arithmetic
SELECT DATE_ADD(CURRENT_DATE(), 7, 'DAY') as next_week;
SELECT DATE_SUB(CURRENT_DATE(), 30, 'DAY') as last_month;
SELECT DATEDIFF('DAYS', '2024-01-01', '2024-12-31') as days_in_2024;

-- NEW: ADD_MONTHS function
SELECT ADD_MONTHS(CURRENT_DATE(), 3) as three_months_later;
SELECT ADD_MONTHS(CURRENT_DATE(), -6) as six_months_ago;

-- NEW: DATE_TRUNC function (truncate to period start)
SELECT DATE_TRUNC('YEAR', NOW()) as start_of_year;
SELECT DATE_TRUNC('QUARTER', NOW()) as start_of_quarter;
SELECT DATE_TRUNC('MONTH', NOW()) as start_of_month;
SELECT DATE_TRUNC('WEEK', NOW()) as start_of_week;
SELECT DATE_TRUNC('DAY', NOW()) as start_of_day;
SELECT DATE_TRUNC('HOUR', NOW()) as start_of_hour;

-- NEW: EOMONTH function (end of month)
SELECT EOMONTH(CURRENT_DATE()) as end_of_current_month;
SELECT EOMONTH(CURRENT_DATE(), 1) as end_of_next_month;
SELECT EOMONTH(CURRENT_DATE(), -1) as end_of_last_month;

-- NEW: IN_PERIOD function (period membership checks)
SELECT IN_PERIOD('TODAY', CURRENT_DATE()) as is_today;
SELECT IN_PERIOD('YTD', '2024-06-15') as is_year_to_date;
SELECT IN_PERIOD('MTD', CURRENT_DATE()) as is_month_to_date;
SELECT IN_PERIOD('QTD', CURRENT_DATE()) as is_quarter_to_date;
SELECT IN_PERIOD('L12M', DATE_SUB(CURRENT_DATE(), 180, 'DAY')) as is_last_12_months;
SELECT IN_PERIOD('CURRENT_QUARTER', CURRENT_DATE()) as is_current_quarter;
SELECT IN_PERIOD('PREVIOUS_QUARTER', DATE_SUB(CURRENT_DATE(), 120, 'DAY')) as was_previous_quarter;
SELECT IN_PERIOD('CURRENT_YEAR_FULL', CURRENT_DATE()) as is_current_year;

-- Date formatting
SELECT STRFTIME('%Y-%m-%d', NOW()) as iso_date;
SELECT STRFTIME('%d.%m.%Y', NOW()) as german_date;
SELECT STRFTIME('%A, %B %d, %Y', NOW()) as long_date;
SELECT DATE(NOW()) as date_only;
SELECT TIME(NOW()) as time_only;

-- ============================================================
-- STRING FUNCTIONS
-- ============================================================

-- Case conversion
SELECT UPPER('hello world') as uppercase;
SELECT LOWER('HELLO WORLD') as lowercase;
SELECT INITCAP('hello world from tinysql') as title_case;

-- String manipulation
SELECT CONCAT('Hello', ' ', 'World') as concatenated;
SELECT CONCAT_WS(', ', 'Apple', 'Banana', 'Cherry') as joined_with_separator;
SELECT LENGTH('Hello World') as string_length;
SELECT CHAR_LENGTH('Hello') as char_length;
SELECT SUBSTRING('Hello World', 7, 5) as substring_result;
SELECT SUBSTR('Hello World', 1, 5) as substr_result;
SELECT LEFT('Hello World', 5) as left_chars;
SELECT RIGHT('Hello World', 5) as right_chars;
SELECT REVERSE('Hello') as reversed;
SELECT REPEAT('Ha', 3) as repeated;

-- String trimming
SELECT TRIM('  Hello  ') as trimmed;
SELECT LTRIM('  Hello') as left_trimmed;
SELECT RTRIM('Hello  ') as right_trimmed;

-- String padding
SELECT LPAD('42', 5, '0') as left_padded;
SELECT RPAD('42', 5, '0') as right_padded;

-- String replacement
SELECT REPLACE('Hello World', 'World', 'Universe') as replaced;

-- String search
SELECT INSTR('Hello World', 'World') as position;
SELECT LOCATE('World', 'Hello World') as locate_position;

-- String formatting
SELECT PRINTF('Value: %d, Name: %s', 42, 'Test') as formatted;
SELECT FORMAT('Pi is approximately %.2f', 3.14159) as formatted_number;

-- NEW: SPLIT function (returns array)
SELECT SPLIT('apple,banana,cherry', ',') as fruit_array;

-- Split and extract part (existing function)
SELECT SPLIT_PART('apple,banana,cherry', ',', 2) as second_fruit;

-- Special string functions
SELECT SPACE(10) as ten_spaces;
SELECT ASCII('A') as ascii_value;
SELECT CHAR(65) as char_from_ascii;
SELECT SOUNDEX('Smith') as soundex_code;
SELECT QUOTE('It''s a test') as quoted_string;

-- ============================================================
-- NEW: REGEX FUNCTIONS
-- ============================================================

-- Pattern matching
SELECT REGEXP_MATCH('test@example.com', '.*@.*\.com') as is_email;
SELECT REGEXP_MATCH('12345', '^\d+$') as is_numeric;

-- Pattern extraction
SELECT REGEXP_EXTRACT('Order #12345 processed', '#\d+') as order_number;
SELECT REGEXP_EXTRACT('Email: test@example.com', '\w+@\w+\.\w+') as email;

-- Pattern replacement
SELECT REGEXP_REPLACE('2024-11-27', '-', '/') as date_with_slashes;
SELECT REGEXP_REPLACE('Hello123World456', '\d+', 'X') as numbers_replaced;

-- ============================================================
-- NUMERIC FUNCTIONS
-- ============================================================

-- Basic math
SELECT ABS(-42) as absolute_value;
SELECT SIGN(-15) as sign_of_number;
SELECT ROUND(3.14159, 2) as rounded;
SELECT FLOOR(3.7) as floor_value;
SELECT CEIL(3.2) as ceiling_value;
SELECT CEILING(3.2) as ceiling_value2;
SELECT TRUNCATE(3.14159, 2) as truncated;
SELECT TRUNC(3.14159, 2) as trunc_value;

-- Advanced math
SELECT POWER(2, 8) as power_of_two;
SELECT POW(3, 3) as cube;
SELECT SQRT(16) as square_root;
SELECT MOD(17, 5) as modulo;

-- Logarithms and exponentials
SELECT LOG(100) as natural_log;
SELECT LN(2.71828) as ln_value;
SELECT LOG10(1000) as log_base_10;
SELECT LOG2(1024) as log_base_2;
SELECT EXP(1) as euler_number;

-- Trigonometry
SELECT PI() as pi_value;
SELECT SIN(PI() / 2) as sine_90_degrees;
SELECT COS(0) as cosine_0_degrees;
SELECT TAN(PI() / 4) as tangent_45_degrees;
SELECT ASIN(1) as arcsine;
SELECT ACOS(0) as arccosine;
SELECT ATAN(1) as arctangent;
SELECT ATAN2(1, 1) as atan2_value;
SELECT DEGREES(PI()) as radians_to_degrees;
SELECT RADIANS(180) as degrees_to_radians;

-- Random numbers
SELECT RANDOM() as random_value;
SELECT RAND() as rand_value;

-- ============================================================
-- NEW: ARRAY FUNCTIONS
-- ============================================================

-- Create arrays
SELECT SPLIT('red,green,blue', ',') as colors;

-- Array element access
SELECT FIRST(SPLIT('alpha,beta,gamma', ',')) as first_element;
SELECT LAST(SPLIT('alpha,beta,gamma', ',')) as last_element;

-- Array properties
SELECT ARRAY_LENGTH(SPLIT('one,two,three,four', ',')) as array_size;

-- Array membership
SELECT ARRAY_CONTAINS(SPLIT('apple,banana,cherry', ','), 'banana') as has_banana;
SELECT IN_ARRAY(SPLIT('1,2,3,4,5', ','), '3') as has_three;

-- Array to string
SELECT ARRAY_JOIN(SPLIT('hello,world', ','), ' ') as joined_array;
SELECT ARRAY_JOIN(SPLIT('a,b,c', ','), ' - ') as joined_with_dash;

-- Array manipulation
SELECT ARRAY_DISTINCT(SPLIT('a,b,a,c,b,d', ',')) as unique_elements;
SELECT ARRAY_SORT(SPLIT('zebra,apple,mango,banana', ',')) as sorted_array;

-- ============================================================
-- AGGREGATE FUNCTIONS
-- ============================================================

-- Create a sample table for aggregate examples
CREATE TABLE temp_numbers (group_id INT, value INT);
INSERT INTO temp_numbers VALUES (1, 10), (1, 20), (1, 30);
INSERT INTO temp_numbers VALUES (2, 5), (2, 15), (2, 25);  
INSERT INTO temp_numbers VALUES (3, 100), (3, 200);
-- COUNT
SELECT group_id, COUNT(*) as total_count FROM temp_numbers GROUP BY group_id;
SELECT group_id, COUNT(value) as non_null_count FROM temp_numbers GROUP BY group_id;

-- Basic aggregates (use with GROUP BY in real queries)
-- SELECT COUNT(*) as total_count FROM table;
-- SELECT COUNT(column) as non_null_count FROM table;
-- SELECT SUM(amount) as total FROM table;
-- SELECT AVG(value) as average FROM table;
-- SELECT MIN(price) as minimum FROM table;
-- SELECT MAX(price) as maximum FROM table;

-- NEW: MIN_BY and MAX_BY - Get value from row with min/max of another column
-- These are useful for time-series data and finding associated values

-- Create a sample sales table
CREATE TABLE temp_sales (product TEXT, sale_date TEXT, amount INT);
INSERT INTO temp_sales VALUES ('Laptop', '2025-01-15', 1200);
INSERT INTO temp_sales VALUES ('Mouse', '2025-03-20', 25);
INSERT INTO temp_sales VALUES ('Keyboard', '2025-02-10', 75);
INSERT INTO temp_sales VALUES ('Monitor', '2025-01-05', 350);

-- Get the product name from the row with the earliest date
SELECT MIN_BY(product, sale_date) as earliest_product FROM temp_sales;

-- Get the product name from the row with the latest date
SELECT MAX_BY(product, sale_date) as latest_product FROM temp_sales;

-- Get the product name with the lowest amount
SELECT MIN_BY(product, amount) as cheapest FROM temp_sales;

-- Get the product name with the highest amount
SELECT MAX_BY(product, amount) as most_expensive FROM temp_sales;

-- Get the amount from the row with the earliest date
SELECT MIN_BY(amount, sale_date) as earliest_amount FROM temp_sales;

-- Get the amount from the row with the latest date
SELECT MAX_BY(amount, sale_date) as latest_amount FROM temp_sales;

-- Aliases ARG_MIN and ARG_MAX are also supported
SELECT ARG_MIN(product, amount) as cheapest_via_argmin FROM temp_sales;
SELECT ARG_MAX(product, amount) as most_expensive_via_argmax FROM temp_sales;

-- Clean up
DROP TABLE temp_sales;
DROP TABLE temp_numbers;-- ============================================================
-- CONDITIONAL AND NULL HANDLING
-- ============================================================

-- NULL handling
SELECT COALESCE(NULL, NULL, 'default') as first_non_null;
SELECT NVL(NULL, 'replacement') as nvl_result;
SELECT IFNULL(NULL, 'value') as ifnull_result;
SELECT NULLIF(5, 5) as nullif_equal;
SELECT NULLIF(5, 3) as nullif_different;

-- Conditional
SELECT IF(10 > 5, 'yes', 'no') as if_result;
SELECT IIF(2 + 2 = 4, 'correct', 'wrong') as iif_result;
SELECT GREATEST(10, 25, 15, 30) as greatest_value;
SELECT LEAST(10, 25, 15, 30) as least_value;

-- CASE expressions
SELECT 
    CASE 
        WHEN 10 > 20 THEN 'impossible'
        WHEN 10 > 5 THEN 'correct'
        ELSE 'wrong'
    END as case_result;

-- ============================================================
-- CRYPTOGRAPHIC AND ENCODING FUNCTIONS
-- ============================================================

-- Hashing
SELECT MD5('password') as md5_hash;
SELECT SHA1('password') as sha1_hash;
SELECT SHA256('password') as sha256_hash;
SELECT SHA512('password') as sha512_hash;

-- Encoding
SELECT BASE64('Hello World') as base64_encoded;
SELECT BASE64_DECODE('SGVsbG8gV29ybGQ=') as base64_decoded;
SELECT HEX('Hello') as hex_encoded;
SELECT UNHEX('48656C6C6F') as hex_decoded;

-- ============================================================
-- JSON FUNCTIONS
-- ============================================================

-- JSON extraction
-- SELECT JSON_GET('{"name":"John","age":30}', 'name') as json_name;
-- SELECT JSON_EXTRACT('{"user":{"name":"Jane"}}', 'user.name') as nested_value;

-- ============================================================
-- TYPE CONVERSION AND INTROSPECTION
-- ============================================================

-- Type casting
SELECT CAST('123' AS INTEGER) as string_to_int;
SELECT CAST(3.14 AS INTEGER) as float_to_int;
SELECT CAST(42 AS TEXT) as int_to_string;

-- Type checking
SELECT TYPEOF(42) as integer_type;
SELECT TYPEOF('text') as string_type;
SELECT TYPEOF(3.14) as float_type;

-- ============================================================
-- UTILITY FUNCTIONS
-- ============================================================

-- UUID generation
SELECT UUID() as unique_id;

-- Version info
SELECT VERSION() as tinysql_version;

-- ============================================================
-- WINDOW FUNCTIONS - FULLY IMPLEMENTED
-- ============================================================

-- Window functions operate on a set of rows (window) relative to the current row
-- Syntax: function() OVER (PARTITION BY ... ORDER BY ... frame_clause)

-- Create sample table for window function examples
CREATE TABLE window_demo (
    product TEXT,
    category TEXT,
    sale_date TEXT,
    amount INT
);

INSERT INTO window_demo VALUES ('Laptop', 'Electronics', '2025-01-15', 1200);
INSERT INTO window_demo VALUES ('Mouse', 'Electronics', '2025-01-20', 25);
INSERT INTO window_demo VALUES ('Desk', 'Furniture', '2025-01-25', 350);
INSERT INTO window_demo VALUES ('Keyboard', 'Electronics', '2025-02-10', 75);
INSERT INTO window_demo VALUES ('Chair', 'Furniture', '2025-02-15', 200);

-- ROW_NUMBER: Assign unique row numbers
SELECT 
    product,
    amount,
    ROW_NUMBER() OVER (ORDER BY amount) as row_num
FROM window_demo
ORDER BY amount;

-- ROW_NUMBER with PARTITION BY: Restart numbering per partition
SELECT 
    product,
    category,
    amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) as row_in_category
FROM window_demo
ORDER BY category, amount;

-- LAG: Access previous row value
SELECT 
    product,
    amount,
    LAG(amount, 1) OVER (ORDER BY sale_date) as previous_amount
FROM window_demo
ORDER BY sale_date;

-- LAG with default value for first row
SELECT 
    product,
    amount,
    LAG(amount, 1, 0) OVER (ORDER BY sale_date) as previous_or_zero
FROM window_demo
ORDER BY sale_date;

-- LEAD: Access next row value
SELECT 
    product,
    amount,
    LEAD(amount, 1) OVER (ORDER BY sale_date) as next_amount
FROM window_demo
ORDER BY sale_date;

-- FIRST_VALUE: Get first value in window
SELECT 
    product,
    amount,
    FIRST_VALUE(product) OVER (ORDER BY amount) as cheapest_product
FROM window_demo
ORDER BY amount;

-- FIRST_VALUE with PARTITION BY
SELECT 
    product,
    category,
    amount,
    FIRST_VALUE(product) OVER (PARTITION BY category ORDER BY amount) as cheapest_in_category
FROM window_demo
ORDER BY category, amount;

-- LAST_VALUE: Get last value in window (up to current row by default)
SELECT 
    product,
    amount,
    LAST_VALUE(product) OVER (ORDER BY amount) as most_expensive_so_far
FROM window_demo
ORDER BY amount;

-- MOVING_SUM: Calculate moving/rolling sum
SELECT 
    product,
    sale_date,
    amount,
    MOVING_SUM(3, amount) OVER (ORDER BY sale_date) as rolling_sum_3days
FROM window_demo
ORDER BY sale_date;

-- MOVING_AVG: Calculate moving/rolling average
SELECT 
    product,
    sale_date,
    amount,
    MOVING_AVG(3, amount) OVER (ORDER BY sale_date) as rolling_avg_3days
FROM window_demo
ORDER BY sale_date;

-- Complex example: LAG with PARTITION BY
SELECT 
    product,
    category,
    amount,
    LAG(amount, 1) OVER (PARTITION BY category ORDER BY sale_date) as prev_in_category
FROM window_demo
ORDER BY category, sale_date;

-- Clean up
DROP TABLE window_demo;

-- ============================================================
-- COMPLEX QUERIES COMBINING MULTIPLE FUNCTIONS
-- ============================================================

-- Example 1: Date range analysis with period check
SELECT 
    CURRENT_DATE() as today,
    DATE_TRUNC('MONTH', CURRENT_DATE()) as month_start,
    EOMONTH(CURRENT_DATE()) as month_end,
    IN_PERIOD('MTD', CURRENT_DATE()) as is_month_to_date,
    DATEDIFF('DAYS', DATE_TRUNC('MONTH', CURRENT_DATE()), CURRENT_DATE()) as days_into_month;

-- Example 2: String processing with regex
SELECT 
    REGEXP_EXTRACT('Order #12345', '#(\d+)') as order_id,
    UPPER(REGEXP_REPLACE('test@EXAMPLE.com', '@.*', '')) as username,
    ARRAY_LENGTH(SPLIT('item1,item2,item3', ',')) as item_count;

-- Example 3: Numeric calculations
SELECT 
    ROUND(PI() * POWER(5, 2), 2) as circle_area_r5,
    DEGREES(ATAN2(3, 4)) as angle_degrees,
    ABS(SIGN(-42) * 100) as absolute_signed;

-- Example 4: Array operations
SELECT 
    FIRST(ARRAY_SORT(ARRAY_DISTINCT(SPLIT('zebra,apple,zebra,mango', ',')))) as first_sorted_unique,
    ARRAY_JOIN(ARRAY_SORT(SPLIT('3,1,4,1,5,9', ',')), ' -> ') as sorted_numbers;

-- Example 5: Conditional logic with dates
SELECT 
    CASE 
        WHEN IN_PERIOD('TODAY', CURRENT_DATE()) THEN 'Today'
        WHEN IN_PERIOD('MTD', CURRENT_DATE()) THEN 'This Month'
        WHEN IN_PERIOD('QTD', CURRENT_DATE()) THEN 'This Quarter'
        WHEN IN_PERIOD('YTD', CURRENT_DATE()) THEN 'This Year'
        ELSE 'Historical'
    END as period_classification;

-- Example 6: NULL handling with coalesce chain
SELECT 
    COALESCE(
        NULL,
        NULLIF('', ''),
        REGEXP_EXTRACT('No match here', 'PATTERN'),
        'Final Default'
    ) as coalesce_result;

-- ============================================================
-- PRACTICAL USE CASES
-- ============================================================

-- Use Case 1: Email validation and extraction
SELECT 
    REGEXP_MATCH('user@example.com', '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') as is_valid_email,
    SPLIT_PART('user@example.com', '@', 1) as username,
    SPLIT_PART('user@example.com', '@', 2) as domain;

-- Use Case 2: Date-based filtering helper
SELECT 
    IN_PERIOD('LAST_12_MONTHS', ADD_MONTHS(CURRENT_DATE(), -6)) as in_last_year,
    IN_PERIOD('CURRENT_QUARTER', CURRENT_DATE()) as in_current_quarter,
    EXTRACT('QUARTER', CURRENT_DATE()) as current_quarter_number;

-- Use Case 3: String normalization
SELECT 
    UPPER(TRIM(REGEXP_REPLACE('  Hello   World  ', '\s+', ' '))) as normalized_string;

-- Use Case 4: Array processing from CSV
SELECT 
    ARRAY_LENGTH(SPLIT('red,green,blue,yellow', ',')) as color_count,
    ARRAY_CONTAINS(SPLIT('red,green,blue', ','), 'green') as has_green,
    ARRAY_JOIN(ARRAY_SORT(ARRAY_DISTINCT(SPLIT('b,a,c,a,b', ','))), ',') as sorted_unique;

-- Use Case 5: Complex date arithmetic
SELECT 
    ADD_MONTHS(DATE_TRUNC('YEAR', CURRENT_DATE()), 6) as mid_year,
    EOMONTH(ADD_MONTHS(CURRENT_DATE(), 3), 0) as end_of_quarter,
    DATEDIFF('DAYS', 
        DATE_TRUNC('YEAR', CURRENT_DATE()), 
        EOMONTH(DATE_TRUNC('YEAR', CURRENT_DATE()), 11)
    ) as days_in_year;

-- ============================================================
-- END OF EXAMPLES
-- ============================================================

-- Note: This file demonstrates the complete tinySQL function library.
-- All functions shown are available and tested.
-- Window functions (ROW_NUMBER, LAG, LEAD, MOVING_SUM, MOVING_AVG) 
-- are planned for future implementation.

const functionData = {
  datetime: {
    title: 'Date and Time Functions',
    functions: [
      {
        name: 'NOW()',
        syntax: 'NOW()',
        description: 'Gibt das aktuelle Datum und die aktuelle Uhrzeit zurück.',
        examples: ['SELECT NOW() as current_timestamp;']
      },
      {
        name: 'CURRENT_DATE()',
        syntax: 'CURRENT_DATE()',
        description: 'Gibt das aktuelle Datum zurück.',
        examples: ['SELECT CURRENT_DATE() as today;']
      },
      {
        name: 'YEAR(date)',
        syntax: 'YEAR(date)',
        description: 'Extrahiert das Jahr aus einem Datum.',
        examples: ['SELECT YEAR(NOW()) as current_year;']
      },
      {
        name: 'MONTH(date)',
        syntax: 'MONTH(date)',
        description: 'Extrahiert den Monat aus einem Datum.',
        examples: ['SELECT MONTH(NOW()) as current_month;']
      },
      {
        name: 'DAY(date)',
        syntax: 'DAY(date)',
        description: 'Extrahiert den Tag aus einem Datum.',
        examples: ['SELECT DAY(NOW()) as current_day;']
      },
      {
        name: 'DATE_ADD(date, value, unit)',
        syntax: 'DATE_ADD(date, value, unit)',
        description: 'Addiert einen Zeitraum zu einem Datum. Units: DAY, MONTH, YEAR, HOUR, MINUTE, SECOND',
        examples: [
          "SELECT DATE_ADD(CURRENT_DATE(), 7, 'DAY') as next_week;",
          "SELECT DATE_ADD(NOW(), 2, 'HOUR') as in_two_hours;"
        ]
      },
      {
        name: 'DATE_SUB(date, value, unit)',
        syntax: 'DATE_SUB(date, value, unit)',
        description: 'Subtrahiert einen Zeitraum von einem Datum.',
        examples: ["SELECT DATE_SUB(CURRENT_DATE(), 30, 'DAY') as last_month;"]
      },
      {
        name: 'DATEDIFF(unit, date1, date2)',
        syntax: 'DATEDIFF(unit, date1, date2)',
        description: 'Berechnet die Differenz zwischen zwei Daten.',
        examples: ["SELECT DATEDIFF('DAYS', '2024-01-01', '2024-12-31') as days;"]
      },
      {
        name: 'EXTRACT(part, date)',
        syntax: 'EXTRACT(part, date)',
        description: 'Extrahiert einen bestimmten Teil aus einem Datum. Parts: YEAR, MONTH, DAY, QUARTER, WEEK, DAYOFWEEK',
        examples: [
          "SELECT EXTRACT('YEAR', NOW()) as year;",
          "SELECT EXTRACT('QUARTER', NOW()) as quarter;"
        ]
      },
      {
        name: 'DATE_TRUNC(unit, date)',
        syntax: 'DATE_TRUNC(unit, date)',
        description: 'Schneidet ein Datum auf die angegebene Einheit ab. Units: YEAR, QUARTER, MONTH, WEEK, DAY, HOUR',
        examples: [
          "SELECT DATE_TRUNC('MONTH', NOW()) as start_of_month;",
          "SELECT DATE_TRUNC('YEAR', NOW()) as start_of_year;"
        ]
      },
      {
        name: 'EOMONTH(date [, offset])',
        syntax: 'EOMONTH(date [, month_offset])',
        description: 'Gibt das Ende des Monats zurück, optional mit Monats-Offset.',
        examples: [
          'SELECT EOMONTH(CURRENT_DATE()) as end_of_month;',
          'SELECT EOMONTH(CURRENT_DATE(), 1) as end_of_next_month;'
        ]
      },
      {
        name: 'ADD_MONTHS(date, months)',
        syntax: 'ADD_MONTHS(date, months)',
        description: 'Addiert oder subtrahiert Monate von einem Datum.',
        examples: [
          'SELECT ADD_MONTHS(CURRENT_DATE(), 3) as in_three_months;',
          'SELECT ADD_MONTHS(CURRENT_DATE(), -6) as six_months_ago;'
        ]
      },
      {
        name: 'IN_PERIOD(period, date)',
        syntax: 'IN_PERIOD(period_name, date)',
        description: 'Prüft, ob ein Datum in einem bestimmten Zeitraum liegt. Periods: TODAY, MTD, QTD, YTD, L12M, CURRENT_QUARTER, etc.',
        examples: [
          "SELECT IN_PERIOD('TODAY', CURRENT_DATE()) as is_today;",
          "SELECT IN_PERIOD('MTD', CURRENT_DATE()) as is_month_to_date;",
          "SELECT IN_PERIOD('YTD', '2024-06-15') as is_year_to_date;"
        ]
      }
    ]
  },
  string: {
    title: 'String Functions',
    functions: [
      {
        name: 'UPPER(str)',
        syntax: 'UPPER(string)',
        description: 'Konvertiert einen String in Großbuchstaben.',
        examples: ["SELECT UPPER('hello world') as uppercase;"]
      },
      {
        name: 'LOWER(str)',
        syntax: 'LOWER(string)',
        description: 'Konvertiert einen String in Kleinbuchstaben.',
        examples: ["SELECT LOWER('HELLO WORLD') as lowercase;"]
      },
      {
        name: 'CONCAT(str1, str2, ...)',
        syntax: 'CONCAT(string1, string2, ...)',
        description: 'Verkettet mehrere Strings.',
        examples: ["SELECT CONCAT('Hello', ' ', 'World') as result;"]
      },
      {
        name: 'LENGTH(str)',
        syntax: 'LENGTH(string)',
        description: 'Gibt die Länge eines Strings zurück.',
        examples: ["SELECT LENGTH('Hello World') as len;"]
      },
      {
        name: 'SUBSTRING(str, start, length)',
        syntax: 'SUBSTRING(string, start, length)',
        description: 'Extrahiert einen Teilstring.',
        examples: ["SELECT SUBSTRING('Hello World', 7, 5) as result;"]
      },
      {
        name: 'TRIM(str)',
        syntax: 'TRIM(string)',
        description: 'Entfernt Leerzeichen am Anfang und Ende.',
        examples: ["SELECT TRIM('  Hello  ') as trimmed;"]
      },
      {
        name: 'REPLACE(str, find, replace)',
        syntax: 'REPLACE(string, search, replacement)',
        description: 'Ersetzt alle Vorkommen eines Teilstrings.',
        examples: ["SELECT REPLACE('Hello World', 'World', 'Universe') as result;"]
      },
      {
        name: 'REGEXP_MATCH(str, pattern)',
        syntax: 'REGEXP_MATCH(string, regex_pattern)',
        description: 'Prüft, ob ein String einem Regex-Muster entspricht.',
        examples: [
          "SELECT REGEXP_MATCH('test@example.com', '.*@.*\\.com') as is_email;",
          "SELECT REGEXP_MATCH('12345', '^\\d+$') as is_numeric;"
        ]
      },
      {
        name: 'REGEXP_EXTRACT(str, pattern)',
        syntax: 'REGEXP_EXTRACT(string, regex_pattern)',
        description: 'Extrahiert den ersten Treffer eines Regex-Musters.',
        examples: [
          "SELECT REGEXP_EXTRACT('Order #12345', '#\\d+') as order_id;",
          "SELECT REGEXP_EXTRACT('Email: test@example.com', '\\w+@\\w+\\.\\w+') as email;"
        ]
      },
      {
        name: 'REGEXP_REPLACE(str, pattern, replacement)',
        syntax: 'REGEXP_REPLACE(string, regex_pattern, replacement)',
        description: 'Ersetzt alle Treffer eines Regex-Musters.',
        examples: [
          "SELECT REGEXP_REPLACE('2024-11-27', '-', '/') as formatted;",
          "SELECT REGEXP_REPLACE('Hello123World456', '\\d+', 'X') as result;"
        ]
      },
      {
        name: 'SPLIT_PART(str, delimiter, index)',
        syntax: 'SPLIT_PART(string, delimiter, part_number)',
        description: 'Teilt einen String und gibt den n-ten Teil zurück.',
        examples: ["SELECT SPLIT_PART('apple,banana,cherry', ',', 2) as second_fruit;"]
      }
    ]
  },
  numeric: {
    title: 'Numeric Functions',
    functions: [
      {
        name: 'ABS(number)',
        syntax: 'ABS(number)',
        description: 'Gibt den Absolutwert einer Zahl zurück.',
        examples: ['SELECT ABS(-42) as absolute;']
      },
      {
        name: 'ROUND(number, decimals)',
        syntax: 'ROUND(number, decimal_places)',
        description: 'Rundet eine Zahl auf die angegebene Anzahl von Dezimalstellen.',
        examples: ['SELECT ROUND(3.14159, 2) as rounded;']
      },
      {
        name: 'FLOOR(number)',
        syntax: 'FLOOR(number)',
        description: 'Rundet eine Zahl auf die nächste ganze Zahl ab.',
        examples: ['SELECT FLOOR(3.7) as result;']
      },
      {
        name: 'CEIL(number)',
        syntax: 'CEIL(number)',
        description: 'Rundet eine Zahl auf die nächste ganze Zahl auf.',
        examples: ['SELECT CEIL(3.2) as result;']
      },
      {
        name: 'POWER(base, exponent)',
        syntax: 'POWER(base, exponent)',
        description: 'Potenziert eine Zahl.',
        examples: ['SELECT POWER(2, 8) as result;']
      },
      {
        name: 'SQRT(number)',
        syntax: 'SQRT(number)',
        description: 'Berechnet die Quadratwurzel.',
        examples: ['SELECT SQRT(16) as result;']
      },
      {
        name: 'MOD(number, divisor)',
        syntax: 'MOD(dividend, divisor)',
        description: 'Gibt den Rest einer Division zurück.',
        examples: ['SELECT MOD(17, 5) as remainder;']
      },
      {
        name: 'PI()',
        syntax: 'PI()',
        description: 'Gibt die Zahl Pi zurück.',
        examples: ['SELECT PI() as pi_value;']
      },
      {
        name: 'SIN(angle)',
        syntax: 'SIN(radians)',
        description: 'Berechnet den Sinus eines Winkels (in Radiant).',
        examples: ['SELECT SIN(PI() / 2) as result;']
      },
      {
        name: 'COS(angle)',
        syntax: 'COS(radians)',
        description: 'Berechnet den Kosinus eines Winkels (in Radiant).',
        examples: ['SELECT COS(0) as result;']
      },
      {
        name: 'RANDOM()',
        syntax: 'RANDOM()',
        description: 'Gibt eine Zufallszahl zurück.',
        examples: ['SELECT RANDOM() as random_value;']
      }
    ]
  },
  array: {
    title: 'Array Functions',
    functions: [
      {
        name: 'SPLIT(str, delimiter)',
        syntax: 'SPLIT(string, delimiter)',
        description: 'Teilt einen String in ein Array auf.',
        examples: ["SELECT SPLIT('red,green,blue', ',') as colors;"]
      },
      {
        name: 'FIRST(array)',
        syntax: 'FIRST(array)',
        description: 'Gibt das erste Element eines Arrays zurück.',
        examples: ["SELECT FIRST(SPLIT('alpha,beta,gamma', ',')) as first_element;"]
      },
      {
        name: 'LAST(array)',
        syntax: 'LAST(array)',
        description: 'Gibt das letzte Element eines Arrays zurück.',
        examples: ["SELECT LAST(SPLIT('alpha,beta,gamma', ',')) as last_element;"]
      },
      {
        name: 'ARRAY_LENGTH(array)',
        syntax: 'ARRAY_LENGTH(array)',
        description: 'Gibt die Anzahl der Elemente in einem Array zurück.',
        examples: ["SELECT ARRAY_LENGTH(SPLIT('one,two,three', ',')) as array_size;"]
      },
      {
        name: 'ARRAY_CONTAINS(array, value)',
        syntax: 'ARRAY_CONTAINS(array, search_value)',
        description: 'Prüft, ob ein Wert in einem Array enthalten ist.',
        examples: ["SELECT ARRAY_CONTAINS(SPLIT('apple,banana,cherry', ','), 'banana') as has_banana;"]
      },
      {
        name: 'ARRAY_JOIN(array, delimiter)',
        syntax: 'ARRAY_JOIN(array, delimiter)',
        description: 'Verbindet Array-Elemente zu einem String.',
        examples: [
          "SELECT ARRAY_JOIN(SPLIT('hello,world', ','), ' ') as joined;",
          "SELECT ARRAY_JOIN(SPLIT('a,b,c', ','), ' - ') as joined_dash;"
        ]
      },
      {
        name: 'ARRAY_DISTINCT(array)',
        syntax: 'ARRAY_DISTINCT(array)',
        description: 'Entfernt Duplikate aus einem Array.',
        examples: ["SELECT ARRAY_DISTINCT(SPLIT('a,b,a,c,b,d', ',')) as unique_elements;"]
      },
      {
        name: 'ARRAY_SORT(array)',
        syntax: 'ARRAY_SORT(array)',
        description: 'Sortiert die Elemente eines Arrays.',
        examples: ["SELECT ARRAY_SORT(SPLIT('zebra,apple,mango,banana', ',')) as sorted;"]
      }
    ]
  },
  aggregate: {
    title: 'Aggregate Functions',
    functions: [
      {
        name: 'COUNT(*)',
        syntax: 'COUNT(*) or COUNT(column)',
        description: 'Zählt die Anzahl der Zeilen.',
        examples: [
          'CREATE TABLE test (id INT, value INT);',
          'INSERT INTO test VALUES (1, 10), (2, 20), (3, 30);',
          'SELECT COUNT(*) as total FROM test;'
        ]
      },
      {
        name: 'SUM(column)',
        syntax: 'SUM(column)',
        description: 'Summiert Werte einer Spalte.',
        examples: ['SELECT SUM(value) as total FROM test;']
      },
      {
        name: 'AVG(column)',
        syntax: 'AVG(column)',
        description: 'Berechnet den Durchschnitt.',
        examples: ['SELECT AVG(value) as average FROM test;']
      },
      {
        name: 'MIN(column)',
        syntax: 'MIN(column)',
        description: 'Findet den kleinsten Wert.',
        examples: ['SELECT MIN(value) as minimum FROM test;']
      },
      {
        name: 'MAX(column)',
        syntax: 'MAX(column)',
        description: 'Findet den größten Wert.',
        examples: ['SELECT MAX(value) as maximum FROM test;']
      },
      {
        name: 'MIN_BY(value_col, order_col)',
        syntax: 'MIN_BY(value_column, ordering_column)',
        description: 'Gibt den Wert aus der Zeile mit dem Minimum der Sortier-Spalte zurück.',
        examples: [
          'CREATE TABLE sales (product TEXT, date TEXT, amount INT);',
          "INSERT INTO sales VALUES ('Laptop', '2025-01-15', 1200), ('Mouse', '2025-03-20', 25);",
          'SELECT MIN_BY(product, date) as earliest_product FROM sales;'
        ]
      },
      {
        name: 'MAX_BY(value_col, order_col)',
        syntax: 'MAX_BY(value_column, ordering_column)',
        description: 'Gibt den Wert aus der Zeile mit dem Maximum der Sortier-Spalte zurück.',
        examples: [
          'SELECT MAX_BY(product, amount) as most_expensive FROM sales;'
        ]
      }
    ]
  },
  window: {
    title: 'Window Functions',
    functions: [
      {
        name: 'ROW_NUMBER() OVER',
        syntax: 'ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)',
        description: 'Vergibt fortlaufende Zeilennummern innerhalb von Partitionen.',
        examples: [
          'CREATE TABLE sales (product TEXT, category TEXT, amount INT);',
          "INSERT INTO sales VALUES ('Laptop', 'Electronics', 1200), ('Mouse', 'Electronics', 25), ('Desk', 'Furniture', 350);",
          'SELECT product, amount, ROW_NUMBER() OVER (ORDER BY amount) as row_num FROM sales;',
          'SELECT product, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) as row_in_cat FROM sales;'
        ]
      },
      {
        name: 'LAG() OVER',
        syntax: 'LAG(column, offset, default) OVER (ORDER BY ...)',
        description: 'Greift auf den Wert einer vorherigen Zeile zu.',
        examples: [
          'SELECT product, amount, LAG(amount, 1) OVER (ORDER BY amount) as prev_amount FROM sales;',
          'SELECT product, amount, LAG(amount, 1, 0) OVER (ORDER BY amount) as prev_or_zero FROM sales;'
        ]
      },
      {
        name: 'LEAD() OVER',
        syntax: 'LEAD(column, offset, default) OVER (ORDER BY ...)',
        description: 'Greift auf den Wert einer nachfolgenden Zeile zu.',
        examples: [
          'SELECT product, amount, LEAD(amount, 1) OVER (ORDER BY amount) as next_amount FROM sales;'
        ]
      },
      {
        name: 'FIRST_VALUE() OVER',
        syntax: 'FIRST_VALUE(column) OVER (PARTITION BY ... ORDER BY ...)',
        description: 'Gibt den ersten Wert im Window zurück.',
        examples: [
          'SELECT product, amount, FIRST_VALUE(product) OVER (ORDER BY amount) as cheapest FROM sales;',
          'SELECT product, category, FIRST_VALUE(product) OVER (PARTITION BY category ORDER BY amount) as cheapest_in_cat FROM sales;'
        ]
      },
      {
        name: 'LAST_VALUE() OVER',
        syntax: 'LAST_VALUE(column) OVER (PARTITION BY ... ORDER BY ...)',
        description: 'Gibt den letzten Wert im Window zurück.',
        examples: [
          'SELECT product, amount, LAST_VALUE(product) OVER (ORDER BY amount) as most_expensive_so_far FROM sales;'
        ]
      },
      {
        name: 'MOVING_SUM() OVER',
        syntax: 'MOVING_SUM(window_size, column) OVER (ORDER BY ...)',
        description: 'Berechnet eine gleitende Summe über ein Fenster.',
        examples: [
          'SELECT product, amount, MOVING_SUM(3, amount) OVER (ORDER BY amount) as rolling_sum FROM sales;'
        ]
      },
      {
        name: 'MOVING_AVG() OVER',
        syntax: 'MOVING_AVG(window_size, column) OVER (ORDER BY ...)',
        description: 'Berechnet einen gleitenden Durchschnitt über ein Fenster.',
        examples: [
          'SELECT product, amount, MOVING_AVG(3, amount) OVER (ORDER BY amount) as rolling_avg FROM sales;'
        ]
      }
    ]
  },
  conditional: {
    title: 'Conditional Functions',
    functions: [
      {
        name: 'COALESCE(val1, val2, ...)',
        syntax: 'COALESCE(value1, value2, ...)',
        description: 'Gibt den ersten Nicht-NULL-Wert zurück.',
        examples: ["SELECT COALESCE(NULL, NULL, 'default') as result;"]
      },
      {
        name: 'NULLIF(val1, val2)',
        syntax: 'NULLIF(value1, value2)',
        description: 'Gibt NULL zurück, wenn beide Werte gleich sind.',
        examples: [
          'SELECT NULLIF(5, 5) as result;',
          'SELECT NULLIF(5, 3) as result;'
        ]
      },
      {
        name: 'IF(condition, true_val, false_val)',
        syntax: 'IF(condition, value_if_true, value_if_false)',
        description: 'Gibt einen Wert basierend auf einer Bedingung zurück.',
        examples: ["SELECT IF(10 > 5, 'yes', 'no') as result;"]
      },
      {
        name: 'GREATEST(val1, val2, ...)',
        syntax: 'GREATEST(value1, value2, ...)',
        description: 'Gibt den größten Wert zurück.',
        examples: ['SELECT GREATEST(10, 25, 15, 30) as maximum;']
      },
      {
        name: 'LEAST(val1, val2, ...)',
        syntax: 'LEAST(value1, value2, ...)',
        description: 'Gibt den kleinsten Wert zurück.',
        examples: ['SELECT LEAST(10, 25, 15, 30) as minimum;']
      },
      {
        name: 'CASE WHEN',
        syntax: 'CASE WHEN condition THEN value ... ELSE value END',
        description: 'Conditional expression ähnlich if-else.',
        examples: [
          "SELECT CASE WHEN 10 > 5 THEN 'greater' WHEN 10 < 5 THEN 'less' ELSE 'equal' END as result;"
        ]
      }
    ]
  },
  crypto: {
    title: 'Cryptographic & Encoding Functions',
    functions: [
      {
        name: 'MD5(str)',
        syntax: 'MD5(string)',
        description: 'Berechnet den MD5-Hash.',
        examples: ["SELECT MD5('password') as hash;"]
      },
      {
        name: 'SHA256(str)',
        syntax: 'SHA256(string)',
        description: 'Berechnet den SHA-256-Hash.',
        examples: ["SELECT SHA256('password') as hash;"]
      },
      {
        name: 'BASE64(str)',
        syntax: 'BASE64(string)',
        description: 'Kodiert einen String in Base64.',
        examples: ["SELECT BASE64('Hello World') as encoded;"]
      },
      {
        name: 'BASE64_DECODE(str)',
        syntax: 'BASE64_DECODE(base64_string)',
        description: 'Dekodiert einen Base64-String.',
        examples: ["SELECT BASE64_DECODE('SGVsbG8gV29ybGQ=') as decoded;"]
      },
      {
        name: 'HEX(str)',
        syntax: 'HEX(string)',
        description: 'Konvertiert einen String in Hexadezimal.',
        examples: ["SELECT HEX('Hello') as hex;"]
      }
    ]
  },
  type: {
    title: 'Type Conversion Functions',
    functions: [
      {
        name: 'CAST(value AS type)',
        syntax: 'CAST(value AS type)',
        description: 'Konvertiert einen Wert in einen anderen Datentyp. Types: INTEGER, TEXT, FLOAT',
        examples: [
          "SELECT CAST('123' AS INTEGER) as number;",
          'SELECT CAST(3.14 AS INTEGER) as int_value;',
          'SELECT CAST(42 AS TEXT) as string;'
        ]
      },
      {
        name: 'TYPEOF(value)',
        syntax: 'TYPEOF(value)',
        description: 'Gibt den Datentyp eines Wertes zurück.',
        examples: [
          'SELECT TYPEOF(42) as type;',
          "SELECT TYPEOF('text') as type;",
          'SELECT TYPEOF(3.14) as type;'
        ]
      }
    ]
  },
  utility: {
    title: 'Utility Functions',
    functions: [
      {
        name: 'UUID()',
        syntax: 'UUID()',
        description: 'Generiert eine eindeutige UUID.',
        examples: ['SELECT UUID() as unique_id;']
      },
      {
        name: 'VERSION()',
        syntax: 'VERSION()',
        description: 'Gibt die tinySQL-Version zurück.',
        examples: ['SELECT VERSION() as version;']
      }
    ]
  }
};

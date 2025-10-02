//go:build !wasm

package main

// Package main validates all demo queries from the WASM browser example

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// setupTestDB creates a database with demo data
func setupTestDB() *storage.DB {
	db := storage.NewDB()
	ctx := context.Background()
	tenant := "default"

	// Setup queries from index_new.html
	setupQueries := []string{
		"CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT, email TEXT, active BOOL)",
		"CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, user_id INT, amount FLOAT, status TEXT, meta JSON)",
		"CREATE TABLE IF NOT EXISTS events (id INT PRIMARY KEY, user_id INT, ts TEXT, data JSON)",
		"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', TRUE)",
		"INSERT INTO users VALUES (2, 'Bob', NULL, TRUE)",
		"INSERT INTO users VALUES (3, 'Carol', 'carol@example.com', FALSE)",
		"INSERT INTO orders VALUES (101, 1, 100.50, 'PAID', '{\"device\":\"web\",\"items\":[{\"sku\":\"A\",\"qty\":1}]}')",
		"INSERT INTO orders VALUES (102, 1, 75.00, 'PAID', '{\"device\":\"app\",\"items\":[{\"sku\":\"B\",\"qty\":2}]}')",
		"INSERT INTO orders VALUES (103, 2, 200.00, 'PAID', '{\"device\":\"web\"}')",
		"INSERT INTO orders VALUES (104, 2, 20.00, 'CANCELED', NULL)",
		"INSERT INTO events VALUES (1, 1, '2023-01-01T10:00:00', '{\"type\":\"login\"}')",
		"INSERT INTO events VALUES (2, 1, '2023-01-01T10:15:00', '{\"type\":\"page_view\"}')",
		"INSERT INTO events VALUES (3, 2, '2023-01-01T11:00:00', '{\"type\":\"login\"}')",
		"INSERT INTO events VALUES (4, 2, '2023-01-01T14:00:00', '{\"type\":\"purchase\"}')",
		"INSERT INTO events VALUES (5, 1, '2023-01-02T09:00:00', '{\"type\":\"login\"}')",
	}

	for _, sql := range setupQueries {
		p := engine.NewParser(sql)
		stmt, err := p.ParseStatement()
		if err != nil {
			panic("Setup query failed to parse: " + sql + " - " + err.Error())
		}
		_, err = engine.Execute(ctx, db, tenant, stmt)
		if err != nil {
			panic("Setup query failed to execute: " + sql + " - " + err.Error())
		}
	}

	return db
}

// Demo queries from index.html
var demoQueries = map[string]string{
	// Date & Time Functions
	"datediff_group":   "SELECT user_id, DATEDIFF('HOURS', MIN(ts), MAX(ts)) AS hours_span FROM events GROUP BY user_id ORDER BY user_id",
	"datediff_days":    "SELECT user_id, MIN(ts) AS first_event, MAX(ts) AS last_event, DATEDIFF('DAYS', MIN(ts), MAX(ts)) AS days_active FROM events GROUP BY user_id HAVING COUNT(*) > 1",
	"datediff_minutes": "SELECT e1.user_id, e1.ts AS login_time, e2.ts AS next_event, DATEDIFF('MINUTES', e1.ts, e2.ts) AS minutes_diff FROM events e1 JOIN events e2 ON e1.user_id = e2.user_id AND e2.id > e1.id WHERE JSON_GET(e1.data, 'type') = 'login' ORDER BY e1.user_id, e1.ts",
	"events_by_date":   "SELECT LEFT(ts, 10) AS event_date, COUNT(*) AS event_count, COUNT(DISTINCT user_id) AS unique_users FROM events GROUP BY LEFT(ts, 10) ORDER BY event_date",

	// JSON Operations
	"json_get":           "SELECT id, JSON_GET(meta, 'device') AS device FROM orders ORDER BY id",
	"json_get_nested":    "SELECT id, JSON_GET(meta, 'device') AS device, JSON_GET(meta, 'items.0.sku') AS first_sku, JSON_GET(meta, 'items.0.qty') AS first_qty FROM orders WHERE meta IS NOT NULL ORDER BY id",
	"json_extract_items": "SELECT id, amount, JSON_GET(meta, 'device') AS device, CASE WHEN JSON_GET(meta, 'items.1.sku') IS NOT NULL THEN 'multi' ELSE 'single' END AS item_count FROM orders WHERE JSON_GET(meta, 'device') IS NOT NULL",
	"json_device_stats":  "SELECT JSON_GET(meta, 'device') AS device, COUNT(*) AS order_count, SUM(amount) AS total_revenue, AVG(amount) AS avg_order_value FROM orders WHERE JSON_GET(meta, 'device') IS NOT NULL GROUP BY JSON_GET(meta, 'device') ORDER BY total_revenue DESC",

	// Joins & Relations
	"join_agg":   "SELECT u.name, COUNT(o.id) AS orders, SUM(o.amount) AS revenue FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'PAID' GROUP BY u.name ORDER BY revenue DESC",
	"left_join":  "SELECT u.name, u.email, COUNT(o.id) AS order_count, SUM(o.amount) AS total_spent FROM users u LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'PAID' GROUP BY u.id, u.name, u.email ORDER BY total_spent DESC",
	"multi_join": "SELECT u.name, COUNT(DISTINCT o.id) AS orders, COUNT(DISTINCT e.id) AS events, MAX(e.ts) AS last_activity FROM users u LEFT JOIN orders o ON u.id = o.user_id LEFT JOIN events e ON u.id = e.user_id GROUP BY u.id, u.name ORDER BY last_activity DESC",
	"self_join":  "SELECT e1.user_id, JSON_GET(e1.data, 'type') AS first_event, JSON_GET(e2.data, 'type') AS second_event, e1.ts AS first_time, e2.ts AS second_time FROM events e1 JOIN events e2 ON e1.user_id = e2.user_id AND e2.id = e1.id + 1 ORDER BY e1.user_id, e1.ts",

	// Aggregation & Grouping
	"count_by_status": "SELECT status, COUNT(*) AS order_count, SUM(amount) AS total_amount, AVG(amount) AS avg_amount FROM orders GROUP BY status ORDER BY order_count DESC",
	"avg_order_value": "SELECT u.name, COUNT(o.id) AS order_count, AVG(o.amount) AS avg_order_value, MIN(o.amount) AS min_order, MAX(o.amount) AS max_order FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'PAID' GROUP BY u.id, u.name HAVING COUNT(o.id) >= 1 ORDER BY avg_order_value DESC",
	"sum_by_user":     "SELECT u.name, SUM(o.amount) AS total_revenue, COUNT(o.id) AS order_count FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'PAID' GROUP BY u.id, u.name ORDER BY total_revenue DESC",
	"min_max_orders":  "SELECT MIN(amount) AS smallest_order, MAX(amount) AS largest_order, AVG(amount) AS average_order, COUNT(*) AS total_orders FROM orders WHERE status = 'PAID'",
	"having_clause":   "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.amount) AS total_spent FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 ORDER BY order_count DESC",

	// Subqueries & Advanced
	"subquery_avg": "SELECT id, user_id, amount, status FROM orders WHERE amount > (SELECT AVG(amount) FROM orders WHERE status = 'PAID') ORDER BY amount DESC",
	// "exists_clause":     "SELECT u.name, u.email FROM users u WHERE EXISTS (SELECT 1 FROM events e WHERE e.user_id = u.id) ORDER BY u.name", // NOT SUPPORTED YET
	// "in_subquery":       "SELECT name, email FROM users WHERE active = TRUE AND id IN (SELECT DISTINCT user_id FROM orders WHERE status = 'PAID') ORDER BY name", // SUBQUERY IN IN NOT SUPPORTED
	// "correlated_subquery": "SELECT u.name, o.id, o.amount, o.status FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = (SELECT MIN(id) FROM orders o2 WHERE o2.user_id = u.id) ORDER BY o.amount DESC", // SUBQUERY IN WHERE NOT SUPPORTED

	// Filtering & Conditions
	"null_handling": "SELECT name, CASE WHEN email IS NULL THEN 'Keine Email' ELSE email END AS email_status, active FROM users WHERE email IS NULL OR email = '' ORDER BY name",
	"case_when":     "SELECT u.name, SUM(o.amount) AS total_spent, CASE WHEN SUM(o.amount) > 150 THEN 'Premium' WHEN SUM(o.amount) > 75 THEN 'Standard' ELSE 'Basic' END AS customer_tier FROM users u LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'PAID' GROUP BY u.id, u.name ORDER BY total_spent DESC",
	"like_pattern":  "SELECT name, email, active FROM users WHERE name LIKE '%a%' OR name LIKE 'C%' ORDER BY name",
	"between_dates": "SELECT user_id, ts, JSON_GET(data, 'type') AS event_type FROM events WHERE ts BETWEEN '2023-01-01' AND '2023-01-02T23:59:59' ORDER BY ts",

	// Analytics & Reporting
	"timeseries":       "SELECT user_id, ts, JSON_GET(data, 'type') AS event_type FROM events ORDER BY user_id, ts",
	"cohort_analysis":  "SELECT user_id, MIN(ts) AS first_seen, MAX(ts) AS last_seen, COUNT(*) AS total_events, DATEDIFF('HOURS', MIN(ts), MAX(ts)) AS session_hours FROM events GROUP BY user_id ORDER BY session_hours DESC",
	"revenue_analysis": "SELECT LEFT(CAST(o.id AS TEXT), 1) AS period, COUNT(*) AS orders, SUM(amount) AS revenue, AVG(amount) AS avg_order_value, COUNT(DISTINCT user_id) AS unique_customers FROM orders o WHERE status = 'PAID' GROUP BY LEFT(CAST(o.id AS TEXT), 1) ORDER BY period",
	"user_activity":    "SELECT JSON_GET(data, 'type') AS event_type, COUNT(*) AS frequency, COUNT(DISTINCT user_id) AS unique_users, MIN(ts) AS first_occurrence, MAX(ts) AS last_occurrence FROM events GROUP BY JSON_GET(data, 'type') ORDER BY frequency DESC",

	// Utility & Testing
	"distinct_values": "SELECT 'Order Status' AS category, status AS value, COUNT(*) AS count FROM orders GROUP BY status UNION ALL SELECT 'Event Types' AS category, JSON_GET(data, 'type') AS value, COUNT(*) AS count FROM events GROUP BY JSON_GET(data, 'type') ORDER BY category, count DESC",
	"limit_offset":    "SELECT id, user_id, amount, status FROM orders ORDER BY amount DESC LIMIT 3 OFFSET 1",
}

func TestAllDemoQueries(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()
	tenant := "default"

	failed := []string{}
	passed := 0

	for name, sql := range demoQueries {
		t.Run(name, func(t *testing.T) {
			// Skip commented queries
			if strings.HasPrefix(sql, "//") {
				t.Skip("Query not yet supported")
				return
			}

			p := engine.NewParser(sql)
			stmt, err := p.ParseStatement()
			if err != nil {
				t.Errorf("Parse error for %s: %v\nSQL: %s", name, err, sql)
				failed = append(failed, name+" (parse)")
				return
			}

			_, err = engine.Execute(ctx, db, tenant, stmt)
			if err != nil {
				t.Errorf("Execute error for %s: %v\nSQL: %s", name, err, sql)
				failed = append(failed, name+" (exec)")
				return
			}

			passed++
		})
	}

	t.Logf("✓ Passed: %d/%d queries", passed, len(demoQueries))
	if len(failed) > 0 {
		t.Logf("✗ Failed: %v", failed)
	}
}

// Test specific problematic queries
func TestProblematicQueries(t *testing.T) {
	db := setupTestDB()
	ctx := context.Background()
	tenant := "default"

	tests := []struct {
		name        string
		sql         string
		expectError bool
		reason      string
	}{
		{
			name:        "COALESCE in aggregate",
			sql:         "SELECT u.name, u.email, COALESCE(COUNT(o.id), 0) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name, u.email",
			expectError: false, // COALESCE now works correctly
			reason:      "COALESCE should work in SELECT with aggregates",
		},
		{
			name:        "Subquery in WHERE",
			sql:         "SELECT id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
			expectError: false,
			reason:      "Scalar subquery in WHERE should work",
		},
		{
			name:        "EXISTS clause",
			sql:         "SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM events e WHERE e.user_id = u.id)",
			expectError: true, // EXISTS not supported yet
			reason:      "EXISTS clause not implemented",
		},
		{
			name:        "IN with subquery",
			sql:         "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'PAID')",
			expectError: true, // Subquery in IN not supported
			reason:      "Subquery in IN clause not implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := engine.NewParser(tt.sql)
			stmt, err := p.ParseStatement()
			if err != nil {
				if !tt.expectError {
					t.Errorf("Unexpected parse error: %v\nReason: %s", err, tt.reason)
				} else {
					t.Logf("Expected parse error (not yet supported): %v", err)
				}
				return
			}

			_, err = engine.Execute(ctx, db, tenant, stmt)
			if err != nil {
				if !tt.expectError {
					t.Errorf("Unexpected execute error: %v\nReason: %s", err, tt.reason)
				} else {
					t.Logf("Expected execute error (not yet supported): %v", err)
				}
			} else if tt.expectError {
				t.Errorf("Expected error but query succeeded\nReason: %s", tt.reason)
			}
		})
	}
}

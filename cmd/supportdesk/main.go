// Command supportdesk is a compact support-search application built with
// tinySQL. It seeds a knowledge base and support tickets, then searches the
// knowledge base using tinySQL full-text search.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

type article struct {
	id       int
	title    string
	body     string
	category string
}

type ticket struct {
	id        int
	articleID int
	subject   string
	status    string
}

func main() {
	query := flag.String("query", "password", "Knowledge-base search query")
	web := flag.Bool("web", false, "Serve the support desk in a browser")
	addr := flag.String("addr", "127.0.0.1:8087", "HTTP listen address when -web is set; use :8087 to accept connections from other machines")
	dsn := flag.String("dsn", "file:supportdesk.db?autosave=1", "TinySQL DSN when -web is set")
	flag.Parse()
	if *web {
		if err := serveSupportDesk(context.Background(), *addr, *dsn); err != nil {
			fmt.Fprintln(os.Stderr, "supportdesk:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(context.Background(), *query, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "supportdesk:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, query string, out io.Writer) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return errors.New("query must not be empty")
	}

	db, err := tsqldriver.OpenInMemory("supportdesk")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := createSchema(ctx, db); err != nil {
		return err
	}
	if err := seed(ctx, db); err != nil {
		return err
	}

	var openTickets, auditEvents int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM open_tickets`).Scan(&openTickets); err != nil {
		return fmt.Errorf("count open tickets from view: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_events`).Scan(&auditEvents); err != nil {
		return fmt.Errorf("count trigger audit events: %w", err)
	}

	if _, err := fmt.Fprintln(out, "tinySQL support-desk example"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "Seeded %d knowledge-base articles and %d tickets.\n", len(seedArticles), len(seedTickets)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "Open tickets (view): %d | Audit events (trigger): %d\n\n", openTickets, auditEvents); err != nil {
		return err
	}

	if err := printCoverage(ctx, db, out); err != nil {
		return err
	}
	return printSearchResults(ctx, db, query, out)
}

func createSchema(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE articles (
			id INT PRIMARY KEY,
			title TEXT NOT NULL,
			body TEXT NOT NULL,
			category TEXT NOT NULL
		)`,
		`CREATE TABLE tickets (
			id INT PRIMARY KEY,
			article_id INT NOT NULL,
			subject TEXT NOT NULL,
			status TEXT NOT NULL
		)`,
		`CREATE TABLE ticket_events (ticket_id INT, event TEXT NOT NULL)`,
		`CREATE TRIGGER ticket_created AFTER INSERT ON tickets FOR EACH ROW BEGIN
			INSERT INTO ticket_events VALUES (NEW.id, 'created');
		END`,
		`CREATE VIEW open_tickets AS
			SELECT id, article_id, subject FROM tickets WHERE status = 'open'`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
}

var seedArticles = []article{
	{1, "Reset your password", "Use the reset link on the sign-in page. Password reset links expire after fifteen minutes.", "Account"},
	{2, "Invite a teammate", "Workspace administrators can send an invitation from the team settings page.", "Teams"},
	{3, "Export monthly invoices", "Open billing history and choose the month to download its PDF invoice.", "Billing"},
	{4, "Set up two-factor authentication", "Add an authenticator app in security settings to protect your account.", "Account"},
}

var seedTickets = []ticket{
	{1, 1, "Password reset email did not arrive", "open"},
	{2, 3, "Need an invoice for April", "closed"},
	{3, 4, "Authenticator code is rejected", "open"},
}

// seed inserts related records in one transaction. If any write fails, no
// partially populated support desk is left behind.
func seed(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin seed transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, a := range seedArticles {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO articles (id, title, body, category) VALUES (?, ?, ?, ?)`,
			a.id, a.title, a.body, a.category); err != nil {
			return fmt.Errorf("insert article %d: %w", a.id, err)
		}
	}
	for _, t := range seedTickets {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO tickets (id, article_id, subject, status) VALUES (?, ?, ?, ?)`,
			t.id, t.articleID, t.subject, t.status); err != nil {
			return fmt.Errorf("insert ticket %d: %w", t.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit seed transaction: %w", err)
	}
	return nil
}

func printCoverage(ctx context.Context, db *sql.DB, out io.Writer) error {
	// This CTE keeps the aggregation readable when the dashboard becomes more
	// complex (for example, by adding ticket counts per category).
	rows, err := db.QueryContext(ctx, `WITH category_counts AS (
		SELECT category, COUNT(*) AS articles
		FROM articles
		GROUP BY category
	)
	SELECT category, articles FROM category_counts ORDER BY category`)
	if err != nil {
		return fmt.Errorf("query category coverage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if _, err := fmt.Fprintln(out, "Knowledge-base coverage (CTE):"); err != nil {
		return err
	}
	for rows.Next() {
		var category string
		var count int
		if err := rows.Scan(&category, &count); err != nil {
			return fmt.Errorf("scan category coverage: %w", err)
		}
		if _, err := fmt.Fprintf(out, "- %s: %d article(s)\n", category, count); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate category coverage: %w", err)
	}
	_, err = fmt.Fprintln(out)
	return err
}

func printSearchResults(ctx context.Context, db *sql.DB, query string, out io.Writer) error {
	// FTS_SEARCH is a tinySQL table-valued function. The query remains a bound
	// parameter, just like ordinary application input.
	rows, err := db.QueryContext(ctx, `SELECT id, title, category, _fts_rank
		FROM FTS_SEARCH('articles', ?, 3)
		ORDER BY _fts_rank`, query)
	if err != nil {
		return fmt.Errorf("full-text search: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if _, err := fmt.Fprintf(out, "Full-text search for %q:\n", query); err != nil {
		return err
	}
	resultCount := 0
	for rows.Next() {
		var id, rank int
		var title, category string
		if err := rows.Scan(&id, &title, &category, &rank); err != nil {
			return fmt.Errorf("scan search result: %w", err)
		}
		resultCount++
		if _, err := fmt.Fprintf(out, "%d. %s [%s]\n", rank, title, category); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate search results: %w", err)
	}
	if resultCount == 0 {
		_, err = fmt.Fprintln(out, "No matching articles.")
	}
	return err
}

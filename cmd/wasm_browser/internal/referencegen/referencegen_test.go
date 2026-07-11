package referencegen

import (
	"strings"
	"testing"
)

func TestGenerateKeepsOnlyDecoratedHeadingsAndSourceOrder(t *testing.T) {
	source := `-- ============================================================
-- DATE FUNCTIONS
-- ============================================================

-- Current date
SELECT CURRENT_DATE();

-- COUNT
SELECT COUNT(*) FROM users;

-- Example 1: date range
SELECT DATE_ADD(CURRENT_DATE(), 1, 'DAY');

-- ============================================================
-- STRING FUNCTIONS
-- ============================================================

SELECT UPPER('tinySQL');

-- ============================================================
-- END OF EXAMPLES
-- ============================================================
`

	sections, err := Generate(strings.NewReader(source))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(sections) != 2 {
		t.Fatalf("expected two non-empty sections, got %#v", sections)
	}

	if sections[0].Section != "DATE FUNCTIONS" || sections[1].Section != "STRING FUNCTIONS" {
		t.Fatalf("expected source order to be preserved, got %#v", sections)
	}
	if got, want := len(sections[0].Examples), 3; got != want {
		t.Fatalf("expected %d date examples, got %d: %#v", want, got, sections[0].Examples)
	}
	if strings.Contains(sections[0].Section, "COUNT") || strings.Contains(sections[0].Section, "Example") {
		t.Fatalf("ordinary comments must not create sections: %#v", sections)
	}
}

func TestGenerateUsesGeneralForUndecoratedSQL(t *testing.T) {
	sections, err := Generate(strings.NewReader("-- introductory comment\nSELECT 1;\n"))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(sections) != 1 || sections[0].Section != "General" {
		t.Fatalf("expected a General section, got %#v", sections)
	}
}

func TestGenerateAcceptsHeadingWithoutClosingRule(t *testing.T) {
	source := `-- ============================================================
-- IO FUNCTIONS

SELECT GZIP('tinySQL');
`

	sections, err := Generate(strings.NewReader(source))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(sections) != 1 || sections[0].Section != "IO FUNCTIONS" {
		t.Fatalf("expected the incomplete heading to start a section, got %#v", sections)
	}
}

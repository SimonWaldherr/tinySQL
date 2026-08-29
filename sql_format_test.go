package tinysql

import (
	"strings"
	"testing"
)

func TestBeautifySQL(t *testing.T) {
	got := BeautifySQL("select id,name from users where note = 'from  x' and id=1")
	want := "SELECT id, name\nFROM users\nWHERE note = 'from  x'\nAND id = 1"
	if got != want {
		t.Fatalf("BeautifySQL() = %q, want %q", got, want)
	}
}

func TestMinifySQLPreservesProtectedTokens(t *testing.T) {
	got := MinifySQL("SELECT  'a  b'  -- keep this\n FROM  [user name]  WHERE id = 1")
	want := "SELECT 'a  b' -- keep this\nFROM [user name] WHERE id=1"
	if got != want {
		t.Fatalf("MinifySQL() = %q, want %q", got, want)
	}
}

// A line comment runs to end of line, so BeautifySQL has to terminate it.
// newline() is a no-op while atLineStart is set, and writing the comment did
// not clear that flag, so the closing newline never fired: "SELECT 1 -- note"
// followed by "FROM t" came out as "-- noteFROM t", putting the FROM clause
// inside the comment.
func TestBeautifySQLTerminatesLineComments(t *testing.T) {
	got := BeautifySQL("SELECT 1 -- note\nFROM t")
	if strings.Contains(got, "-- noteFROM") {
		t.Fatalf("FROM was absorbed into the comment: %q", got)
	}
	if !strings.Contains(got, "-- note\n") {
		t.Errorf("comment is not terminated by a newline: %q", got)
	}
	if _, err := ParseSQL(got); err != nil {
		t.Errorf("beautified output does not parse: %v\n%s", err, got)
	}
}

// The same for a block comment followed by more of the statement.
func TestBeautifySQLSeparatesBlockComments(t *testing.T) {
	got := BeautifySQL("SELECT a, /* mid */ b FROM t")
	if strings.Contains(got, "*/b") {
		t.Errorf("token ran straight onto the block comment: %q", got)
	}
	if _, err := ParseSQL(got); err != nil {
		t.Errorf("beautified output does not parse: %v\n%s", err, got)
	}
}

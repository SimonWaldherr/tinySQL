package tinysql

import "testing"

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

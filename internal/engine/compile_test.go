package engine

import "testing"

func TestQueryCache_Basic(t *testing.T) {
	qc := NewQueryCache(2)
	if qc == nil {
		t.Fatal("expected non-nil QueryCache")
	}

	stats := qc.Stats()
	if stats["maxSize"].(int) != 2 {
		t.Fatalf("expected maxSize 2, got %v", stats["maxSize"])
	}

	cq1, err := qc.Compile("SELECT 1")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	cq2, err := qc.Compile("SELECT 1")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if cq1 != cq2 {
		t.Fatalf("expected cached CompiledQuery pointer, got different instances")
	}

	if qc.Size() != 1 {
		t.Fatalf("expected size 1, got %d", qc.Size())
	}

	// Add more queries to trigger eviction when maxSize is exceeded
	if _, err := qc.Compile("SELECT 2"); err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if _, err := qc.Compile("SELECT 3"); err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if qc.Size() != 2 {
		t.Fatalf("expected size 2 after eviction, got %d", qc.Size())
	}
}

func TestQueryCache_MustCompilePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected MustCompile to panic on invalid SQL")
		}
	}()
	qc := NewQueryCache(1)
	qc.MustCompile("THIS IS INVALID SQL")
}

func TestQueryCache_ClearAndSize(t *testing.T) {
	qc := NewQueryCache(5)
	if _, err := qc.Compile("SELECT 1"); err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if _, err := qc.Compile("SELECT 2"); err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if qc.Size() == 0 {
		t.Fatalf("expected non-zero size before Clear")
	}

	qc.Clear()
	if qc.Size() != 0 {
		t.Fatalf("expected size 0 after Clear, got %d", qc.Size())
	}
}

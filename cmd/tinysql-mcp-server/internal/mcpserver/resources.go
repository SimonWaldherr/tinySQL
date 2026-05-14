package mcpserver

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// InsightStore is a thread-safe, in-memory store for analytical observations
// appended through the append_insight tool.
type InsightStore struct {
	mu       sync.RWMutex
	insights []insightEntry
}

type insightEntry struct {
	text      string
	createdAt time.Time
}

// Append adds a new insight to the store and returns its 1-based index.
func (s *InsightStore) Append(text string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insights = append(s.insights, insightEntry{
		text:      text,
		createdAt: time.Now().UTC(),
	})
	return len(s.insights)
}

// Memo returns the insight memo formatted as a Markdown document.
// If no insights have been recorded it returns the standard placeholder.
func (s *InsightStore) Memo() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.insights) == 0 {
		return "No business insights have been discovered yet."
	}
	var b strings.Builder
	b.WriteString("# Database Insights\n\n")
	b.WriteString(fmt.Sprintf("*%d observation(s) recorded.*\n\n", len(s.insights)))
	for i, e := range s.insights {
		b.WriteString(fmt.Sprintf("- **[%d]** (%s) %s\n", i+1, e.createdAt.Format("2006-01-02 15:04:05"), e.text))
	}
	b.WriteString("\n---\n")
	b.WriteString(fmt.Sprintf("*Last updated: %s*\n", time.Now().UTC().Format("2006-01-02 15:04:05")))
	return b.String()
}

// Count returns the number of insights stored.
func (s *InsightStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.insights)
}

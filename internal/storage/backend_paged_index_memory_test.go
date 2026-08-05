package storage

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage/pager"
)

func TestPagedIndexBackendSplitsMemoryBudgetAcrossCaches(t *testing.T) {
	const budget = 8 << 20
	pageBudget, poolBudget := splitPagedIndexMemoryBudget(budget)
	pageCacheBudget := (pageBudget / pager.DefaultPageSize) * pager.DefaultPageSize
	if total := pageCacheBudget + poolBudget; total > budget {
		t.Fatalf("paged backend cache budgets exceed limit: page=%d pool=%d total=%d limit=%d", pageBudget, poolBudget, total, budget)
	}
	if pageBudget <= poolBudget {
		t.Fatalf("pager should receive the larger serving budget: page=%d pool=%d", pageBudget, poolBudget)
	}
}

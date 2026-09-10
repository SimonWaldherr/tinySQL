package engine

import (
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"reflect"
	"strings"
	"time"
)

type SubscriptionStats struct {
	Refreshes, FullRefreshes, DeliveredChanges, Notifications, Coalesced uint64
	RefreshTime, DeliveryWait                                            time.Duration
}

func (s *QuerySubscription) Stats() SubscriptionStats {
	w := s.state.watch.Stats()
	return SubscriptionStats{Refreshes: s.state.refreshes.Load(), FullRefreshes: s.state.fullRefreshes.Load(), DeliveredChanges: s.delivered.Load() + 1, Notifications: w.Notifications, Coalesced: w.Coalesced, RefreshTime: time.Duration(s.state.refreshNanos.Load()), DeliveryWait: time.Duration(s.sendNanos.Load())}
}

// Unknown/dynamic functions may read other tables or depend on time; those
// subscriptions retain global wakeups. A whitelist makes future AST/function
// additions conservative rather than silently missing a dependency.
func subscriptionStaticTree(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return true
		}
		return subscriptionStaticTree(v.Elem())
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return true
		}
		if f, ok := v.Interface().(*FuncCall); ok {
			switch strings.ToUpper(f.Name) {
			case "COUNT", "SUM", "AVG", "MIN", "MAX", "ABS", "LOWER", "UPPER", "LENGTH", "COALESCE", "ROUND", "ROW_NUMBER", "RANK", "DENSE_RANK":
			default:
				return false
			}
		}
		return subscriptionStaticTree(v.Elem())
	}
	switch v.Kind() {
	case reflect.Struct:
		if from, ok := v.Interface().(FromItem); ok {
			if from.TableFunc != nil || isCatalogOrSysTableRef(from.Table) || isSQLiteSchemaTable(from.Table) {
				return false
			}
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() && !subscriptionStaticTree(v.Field(i)) {
				return false
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if !subscriptionStaticTree(v.Index(i)) {
				return false
			}
		}
	}
	return true
}
func subscriptionTableDependencies(db *storage.DB, tenant string, query *Select) []storage.TableRef {
	seen := make(map[storage.TableRef]bool)
	visiting := make(map[string]bool)
	var walk func(*Select, int) bool
	walk = func(sel *Select, depth int) bool {
		if depth > 16 || !subscriptionStaticTree(reflect.ValueOf(sel)) {
			return false
		}
		deps := selectDependencies(db.Catalog(), "", "", "", sel)
		for _, dep := range deps {
			name := dep.DependsOnName
			if dep.DependsOnSchema != "main" {
				name = dep.DependsOnSchema + "." + name
			}
			if table, err := db.Get(tenant, name); err == nil {
				seen[storage.TableRef{Tenant: tenant, Table: table.Name}] = true
				continue
			}
			key := dep.DependsOnSchema + "." + dep.DependsOnName
			if visiting[key] {
				return false
			}
			view, ok := db.Catalog().GetView(dep.DependsOnSchema, dep.DependsOnName)
			if !ok {
				return false
			}
			stmt, err := NewParser(view.SQLText).ParseStatement()
			if err != nil {
				return false
			}
			inner, ok := stmt.(*Select)
			if !ok {
				return false
			}
			visiting[key] = true
			if !walk(inner, depth+1) {
				return false
			}
			delete(visiting, key)
		}
		return true
	}
	if !walk(query, 0) {
		return nil
	}
	refs := make([]storage.TableRef, 0, len(seen))
	for ref := range seen {
		refs = append(refs, ref)
	}
	return refs
}

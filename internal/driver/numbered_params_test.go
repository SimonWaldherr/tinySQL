package driver

import (
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestNumberedParameterPlan(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		order []int
		count int
	}{
		{"SELECT $2, $1, $2", []int{1, 0, 1}, 2},
		{"SELECT :01, :1", []int{0, 0}, 1},
		{"SELECT $2, :1", []int{1, 0}, 2},
		{"SELECT '$99 ?', $1 AS \"$9\" -- $8 ?\n", []int{0}, 1},
		{"SELECT /* $99 ? */ $1", []int{0}, 1},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := buildPreparedQuery(tc.sql)
			if err != nil || p == nil {
				t.Fatalf("prepare: %v", err)
			}
			if p.inputCount != tc.count || !reflect.DeepEqual(p.argOrder, tc.order) {
				t.Fatalf("count=%d order=%v", p.inputCount, p.argOrder)
			}
			exec, err := p.acquire()
			if err != nil {
				t.Fatal(err)
			}
			args := make([]driver.NamedValue, tc.count)
			for i := range args {
				args[i].Value = i + 10
			}
			p.bind(exec, args)
			for i, ordinal := range tc.order {
				if exec.params[i].Val != ordinal+10 {
					t.Fatalf("slot %d = %v", i, exec.params[i].Val)
				}
			}
			p.release(exec)
			for i, literal := range exec.params {
				if literal.Val != p.markers[i] {
					t.Fatal("release retained argument")
				}
			}
			// Pool misses must rebuild the same mapping without relying on cached ASTs.
			fresh, err := p.newExecution()
			if err != nil {
				t.Fatal(err)
			}
			p.bind(fresh, args)
			for i, ordinal := range tc.order {
				if fresh.params[i].Val != ordinal+10 {
					t.Fatal("new execution mapping changed")
				}
			}
		})
	}
	for _, q := range []string{"SELECT $0", "SELECT $2", "SELECT $99999999999999999999999999", "SELECT ?, $1", "SELECT '__tinysql_prepared_param_0__', $1"} {
		if p, _ := buildPreparedQuery(q); p != nil {
			t.Fatalf("expected fallback: %s", q)
		}
	}
}

package main

import (
	"sort"
	"testing"
	"time"
)

func rowFor(id int64, name string) IncrementalRow {
	keyVals := []any{id}
	return IncrementalRow{
		Key:       computeRowKey(keyVals),
		KeyValues: keyVals,
		Columns:   []any{id, name},
	}
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()
	got = sortedCopy(got)
	want = sortedCopy(want)
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mismatch at %d: got %v, want %v", i, got, want)
		}
	}
}

func assertUpsertKeysEqual(t *testing.T, got []IncrementalRow, wantKeys []string) {
	t.Helper()
	gotKeys := make([]string, len(got))
	for i, r := range got {
		gotKeys[i] = r.Key
	}
	assertStringSlicesEqual(t, gotKeys, wantKeys)
}

// --- First run: prev is a zero-value state -> everything is an insert. ---

func TestPlanIncrementalSync_FirstRun_NoWatermark_AllInserts(t *testing.T) {
	prev := &TableSyncState{} // zero-value: no prior sync
	rows := []IncrementalRow{
		rowFor(1, "alice"),
		rowFor(2, "bob"),
		rowFor(3, "carol"),
	}
	currentKeys := []string{rows[0].Key, rows[1].Key, rows[2].Key}

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, rows, false)

	assertUpsertKeysEqual(t, toUpsert, currentKeys)
	if len(toDelete) != 0 {
		t.Fatalf("expected no deletes on first run, got %v", toDelete)
	}
	assertStringSlicesEqual(t, next.Keys, currentKeys)
	if len(next.RowHashes) != 3 {
		t.Fatalf("expected 3 row hashes persisted, got %d", len(next.RowHashes))
	}
}

func TestPlanIncrementalSync_FirstRun_Watermark_AllInserts(t *testing.T) {
	prev := &TableSyncState{} // zero-value: no prior sync
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	rows := []IncrementalRow{
		{Key: computeRowKey([]any{int64(1)}), KeyValues: []any{int64(1)}, Columns: []any{int64(1), "alice"}, Watermark: t1},
		{Key: computeRowKey([]any{int64(2)}), KeyValues: []any{int64(2)}, Columns: []any{int64(2), "bob"}, Watermark: t2},
	}
	currentKeys := []string{rows[0].Key, rows[1].Key}

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, rows, true)

	assertUpsertKeysEqual(t, toUpsert, currentKeys)
	if len(toDelete) != 0 {
		t.Fatalf("expected no deletes on first run, got %v", toDelete)
	}
	if next.Watermark == nil {
		t.Fatal("expected a watermark to be recorded")
	}
	got, err := next.Watermark.Value()
	if err != nil {
		t.Fatalf("Value(): %v", err)
	}
	gotTime, ok := got.(time.Time)
	if !ok || !gotTime.Equal(t2) {
		t.Fatalf("expected max watermark %v, got %v", t2, got)
	}
}

// --- Deletes: a key that disappears from currentKeys. ---

func TestPlanIncrementalSync_DisappearedKeyBecomesDelete(t *testing.T) {
	prev := &TableSyncState{
		Keys:      []string{"k1", "k2", "k3"},
		RowHashes: map[string]string{"k1": "h1", "k2": "h2", "k3": "h3"},
	}
	// k2 has vanished from the source.
	currentKeys := []string{"k1", "k3"}
	rows := []IncrementalRow{
		{Key: "k1", KeyValues: []any{"k1"}, Columns: []any{"k1", "same"}},
		{Key: "k3", KeyValues: []any{"k3"}, Columns: []any{"k3", "same"}},
	}

	_, toDelete, next := planIncrementalSync(prev, currentKeys, rows, false)

	assertStringSlicesEqual(t, toDelete, []string{"k2"})
	assertStringSlicesEqual(t, next.Keys, currentKeys)
}

// --- Watermark mode: only changedRows upsert; deletes still from full key scan. ---

func TestPlanIncrementalSync_Watermark_OnlyChangedRowsUpsert_DeletesFromFullScan(t *testing.T) {
	k1 := computeRowKey([]any{int64(1)})
	k2 := computeRowKey([]any{int64(2)})
	k3 := computeRowKey([]any{int64(3)})
	k4 := computeRowKey([]any{int64(4)})

	prev := &TableSyncState{
		Keys: []string{k1, k2, k3},
	}

	// Full key scan: k2 vanished, k4 is new.
	currentKeys := []string{k1, k3, k4}

	// Only the new row k4 passed the caller's watermark pre-filter.
	changedRows := []IncrementalRow{
		{Key: k4, KeyValues: []any{int64(4)}, Columns: []any{int64(4), "dave"}, Watermark: int64(100)},
	}

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, changedRows, true)

	assertUpsertKeysEqual(t, toUpsert, []string{k4})
	assertStringSlicesEqual(t, toDelete, []string{k2})
	assertStringSlicesEqual(t, next.Keys, currentKeys)
}

func TestPlanIncrementalSync_Watermark_EmptyChangedRowsKeepsPrevWatermark(t *testing.T) {
	prevWM, err := newWatermarkValue(int64(500))
	if err != nil {
		t.Fatalf("newWatermarkValue: %v", err)
	}
	prev := &TableSyncState{
		Keys:      []string{"k1"},
		Watermark: &prevWM,
	}
	currentKeys := []string{"k1"}

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, nil, true)

	if len(toUpsert) != 0 {
		t.Fatalf("expected no upserts, got %v", toUpsert)
	}
	if len(toDelete) != 0 {
		t.Fatalf("expected no deletes, got %v", toDelete)
	}
	if next.Watermark == nil || next.Watermark.Text != "500" {
		t.Fatalf("expected watermark to be preserved as 500, got %+v", next.Watermark)
	}
}

// --- No-watermark mode: unchanged rows are excluded, changed rows included. ---

func TestPlanIncrementalSync_NoWatermark_UnchangedRowExcluded(t *testing.T) {
	row := rowFor(1, "alice")
	prevHash := rowContentHash(row.Columns)

	prev := &TableSyncState{
		Keys:      []string{row.Key},
		RowHashes: map[string]string{row.Key: prevHash},
	}
	currentKeys := []string{row.Key}
	changedRows := []IncrementalRow{row} // full current row set; content is unchanged

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, changedRows, false)

	if len(toUpsert) != 0 {
		t.Fatalf("expected unchanged row to be excluded from upsert, got %v", toUpsert)
	}
	if len(toDelete) != 0 {
		t.Fatalf("expected no deletes, got %v", toDelete)
	}
	if next.RowHashes[row.Key] != prevHash {
		t.Fatalf("expected hash to be carried forward unchanged, got %q want %q", next.RowHashes[row.Key], prevHash)
	}
}

func TestPlanIncrementalSync_NoWatermark_ChangedRowIncluded(t *testing.T) {
	oldRow := rowFor(1, "alice")
	oldHash := rowContentHash(oldRow.Columns)

	prev := &TableSyncState{
		Keys:      []string{oldRow.Key},
		RowHashes: map[string]string{oldRow.Key: oldHash},
	}

	newRow := rowFor(1, "alice-updated") // same key, different content
	currentKeys := []string{newRow.Key}
	changedRows := []IncrementalRow{newRow}

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, changedRows, false)

	assertUpsertKeysEqual(t, toUpsert, []string{newRow.Key})
	if len(toDelete) != 0 {
		t.Fatalf("expected no deletes, got %v", toDelete)
	}
	newHash := rowContentHash(newRow.Columns)
	if next.RowHashes[newRow.Key] != newHash {
		t.Fatalf("expected updated hash %q, got %q", newHash, next.RowHashes[newRow.Key])
	}
	if newHash == oldHash {
		t.Fatalf("test setup invalid: old and new hashes should differ")
	}
}

func TestPlanIncrementalSync_NoWatermark_MixedChangedAndUnchanged(t *testing.T) {
	unchanged := rowFor(1, "alice")
	changedOld := rowFor(2, "bob")
	newRowForChanged := rowFor(2, "bob-updated")
	newRow := rowFor(3, "carol") // brand new key

	prev := &TableSyncState{
		Keys: []string{unchanged.Key, changedOld.Key},
		RowHashes: map[string]string{
			unchanged.Key:  rowContentHash(unchanged.Columns),
			changedOld.Key: rowContentHash(changedOld.Columns),
		},
	}

	currentKeys := []string{unchanged.Key, newRowForChanged.Key, newRow.Key}
	changedRows := []IncrementalRow{unchanged, newRowForChanged, newRow}

	toUpsert, toDelete, next := planIncrementalSync(prev, currentKeys, changedRows, false)

	assertUpsertKeysEqual(t, toUpsert, []string{newRowForChanged.Key, newRow.Key})
	if len(toDelete) != 0 {
		t.Fatalf("expected no deletes, got %v", toDelete)
	}
	if len(next.RowHashes) != 3 {
		t.Fatalf("expected 3 hashes in next state, got %d", len(next.RowHashes))
	}
}

// --- Composite-key delete round trip. ---

func TestCompositeKey_DecodeRowKeyRoundTrip(t *testing.T) {
	keyVals := []any{"tenant-42", int64(7), true}
	key := computeRowKey(keyVals)

	parts := decodeRowKey(key)
	if len(parts) != len(keyVals) {
		t.Fatalf("expected %d parts, got %d: %v", len(keyVals), len(parts), parts)
	}

	wantParts := make([]string, len(keyVals))
	for i, v := range keyVals {
		wantParts[i] = canonicalKeyPart(v)
	}
	for i := range wantParts {
		if parts[i] != wantParts[i] {
			t.Errorf("part %d: got %q, want %q", i, parts[i], wantParts[i])
		}
	}
}

func TestCompositeKey_DeleteAppliedViaKeyValuesFromPriorRow(t *testing.T) {
	// Simulate two runs. Run 1: composite-key row exists and is upserted,
	// establishing prev state. Run 2: the row vanishes -> delete. Verify
	// both delete-application contracts (decode the string, or carry
	// KeyValues alongside a present row) recover equivalent data.
	keyVals := []any{"tenant-42", int64(7)}
	row := IncrementalRow{
		Key:       computeRowKey(keyVals),
		KeyValues: keyVals,
		Columns:   []any{"tenant-42", int64(7), "payload"},
	}

	prevEmpty := &TableSyncState{}
	_, _, afterRun1 := planIncrementalSync(prevEmpty, []string{row.Key}, []IncrementalRow{row}, false)

	// Run 2: row is gone.
	toUpsert, toDelete, _ := planIncrementalSync(afterRun1, []string{}, []IncrementalRow{}, false)

	if len(toUpsert) != 0 {
		t.Fatalf("expected no upserts, got %v", toUpsert)
	}
	assertStringSlicesEqual(t, toDelete, []string{row.Key})

	// Contract 1: decode the opaque key string back into canonical parts.
	decoded := decodeRowKey(toDelete[0])
	wantParts := []string{canonicalKeyPart(keyVals[0]), canonicalKeyPart(keyVals[1])}
	if len(decoded) != 2 || decoded[0] != wantParts[0] || decoded[1] != wantParts[1] {
		t.Fatalf("decodeRowKey mismatch: got %v, want %v", decoded, wantParts)
	}

	// Contract 2: if the caller still has the row from when it was last
	// seen (e.g. cached from run 1), KeyValues recovers the original typed
	// values directly, with no decode needed.
	if row.KeyValues[0] != keyVals[0] || row.KeyValues[1] != keyVals[1] {
		t.Fatalf("KeyValues mismatch: got %v, want %v", row.KeyValues, keyVals)
	}
}

// --- Watermark comparison helper. ---

func TestCompareWatermarkValues(t *testing.T) {
	mk := func(v any) WatermarkValue {
		wv, err := newWatermarkValue(v)
		if err != nil {
			t.Fatalf("newWatermarkValue(%v): %v", v, err)
		}
		return wv
	}

	cases := []struct {
		name string
		a, b WatermarkValue
		want int
	}{
		{"int less", mk(int64(1)), mk(int64(2)), -1},
		{"int equal", mk(int64(5)), mk(int64(5)), 0},
		{"int greater", mk(int64(9)), mk(int64(2)), 1},
		{"float less", mk(2.5), mk(10.1), -1},
		{"string less", mk("a"), mk("b"), -1},
		{
			"time less",
			mk(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			mk(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)),
			-1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := compareWatermarkValues(c.a, c.b)
			if got != c.want {
				t.Errorf("compareWatermarkValues(%+v, %+v) = %d, want %d", c.a, c.b, got, c.want)
			}
			// Antisymmetry check.
			gotRev := compareWatermarkValues(c.b, c.a)
			if c.want != 0 && gotRev != -c.want {
				t.Errorf("compareWatermarkValues reversed = %d, want %d", gotRev, -c.want)
			}
		})
	}
}

func TestPlanIncrementalSync_NextKeysNotAliased(t *testing.T) {
	prev := &TableSyncState{}
	currentKeys := []string{"a", "b"}
	_, _, next := planIncrementalSync(prev, currentKeys, nil, false)

	next.Keys[0] = "mutated"
	if currentKeys[0] == "mutated" {
		t.Fatal("next.Keys aliases the caller's currentKeys slice")
	}
}

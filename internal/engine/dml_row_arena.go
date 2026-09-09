package engine

// dmlRowArena packs statement-owned row storage into small blocks. Each row
// gets a disjoint slice with capacity equal to its width, so append cannot
// overwrite its neighbour. Blocks are never pooled: retained rows and MVCC/WAL
// snapshots own their values for as long as they need them. A surviving row can
// retain at most one 64-row block after its neighbours have been deleted.
type dmlRowArena struct {
	remaining    []any
	rowsPerBlock int
}

func (a *dmlRowArena) row(width int) []any {
	if width == 0 || a.rowsPerBlock <= 1 {
		return make([]any, width)
	}
	if len(a.remaining) < width {
		a.remaining = make([]any, width*a.rowsPerBlock)
	}
	row := a.remaining[:width:width]
	a.remaining = a.remaining[width:]
	return row
}

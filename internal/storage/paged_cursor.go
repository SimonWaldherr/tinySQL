package storage

// PagedRowCursor advances through one immutable PagedIndex table or index
// interval in bounded decoded batches. It is intentionally single-consumer:
// ResultStream owns one producer goroutine, and retaining a mutex here would
// put it on the per-batch hot path for no useful concurrency.
//
// Each NextBatch call holds the pager read lock only while it decodes that
// batch. Callers must process/send the returned rows after NextBatch returns,
// so slow downstream consumers cannot retain a pager lock.
type PagedRowCursor struct {
	backend *PagedIndexBackend
	tenant  string
	table   string

	indexName string
	startKey  []byte
	endKey    []byte

	resumeKey  []byte
	resumeSkip int
	done       bool
}

// OpenPagedTableCursor starts an ordered table-row cursor. ok is false for a
// database without the ModePagedIndex backend.
func (db *DB) OpenPagedTableCursor(tenant, table string) (cursor *PagedRowCursor, ok bool, err error) {
	if db == nil {
		return nil, false, nil
	}
	backend, ok := db.backend.(*PagedIndexBackend)
	if !ok {
		return nil, false, nil
	}
	return &PagedRowCursor{backend: backend, tenant: tenant, table: table}, true, nil
}

// OpenPagedIndexRangeCursor starts an ordered index cursor over inclusive raw
// index-key bounds. The keys are copied because planner scratch buffers must
// not outlive plan construction. ok is false for non-paged storage.
func (db *DB) OpenPagedIndexRangeCursor(tenant, table, indexName string, startKey, endKey []byte) (cursor *PagedRowCursor, ok bool, err error) {
	if db == nil {
		return nil, false, nil
	}
	backend, ok := db.backend.(*PagedIndexBackend)
	if !ok {
		return nil, false, nil
	}
	return &PagedRowCursor{
		backend:   backend,
		tenant:    tenant,
		table:     table,
		indexName: indexName,
		startKey:  append([]byte(nil), startKey...),
		endKey:    append([]byte(nil), endKey...),
	}, true, nil
}

// NextBatch returns up to limit decoded rows and advances the cursor. A nil
// error with an empty result means EOF. limit must be positive.
func (c *PagedRowCursor) NextBatch(limit int) ([][]any, error) {
	if c == nil || c.done {
		return nil, nil
	}
	if c.indexName == "" {
		rows, nextKey, done, err := c.backend.page.ScanTableRowsBatch(c.tenant, c.table, c.resumeKey, limit)
		if err != nil {
			return nil, err
		}
		c.resumeKey = nextKey
		c.done = done
		return rows, nil
	}

	rows, nextKey, nextSkip, done, err := c.backend.page.ScanIndexRowsRangeBatch(
		c.tenant, c.table, c.indexName, c.startKey, c.endKey, c.resumeKey, c.resumeSkip, limit)
	if err != nil {
		return nil, err
	}
	c.resumeKey = nextKey
	c.resumeSkip = nextSkip
	c.done = done
	return rows, nil
}

// Done reports whether the cursor has reached EOF. It is valid after a
// successful NextBatch call.
func (c *PagedRowCursor) Done() bool {
	return c == nil || c.done
}

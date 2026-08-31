//go:build js && wasm

package main

import (
	"fmt"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// resultLimitInfo describes a browser-facing materialized result. The engine
// may have produced more rows, but a capped result is never kept in the WASM
// pager or made available for export.
type resultLimitInfo struct {
	Truncated    bool
	Reason       string
	RowsProduced int
	RowsRetained int
	ResultBytes  int64
}

// limitResultForBrowser keeps the legacy multi-statement path from retaining
// an unbounded final result after execution. The stream path applies the same
// limits while rows are produced; a multi-statement script still materializes
// its final statement inside the engine and is labelled accordingly in the UI.
func limitResultForBrowser(result *tinysql.ResultSet) (*tinysql.ResultSet, resultLimitInfo, error) {
	if result == nil {
		return nil, resultLimitInfo{}, nil
	}

	limited := &tinysql.ResultSet{
		Cols: append([]string(nil), result.Cols...),
		Rows: make([]tinysql.Row, 0, min(len(result.Rows), streamPreviewMaxRows)),
	}
	info := resultLimitInfo{RowsProduced: len(result.Rows)}
	for _, row := range result.Rows {
		if len(limited.Rows) >= streamPreviewMaxRows {
			info.Truncated = true
			info.Reason = "row"
			break
		}
		rowBytes, err := streamRowBytes(result.Cols, row)
		if err != nil {
			return nil, resultLimitInfo{}, fmt.Errorf("measure row: %w", err)
		}
		if info.ResultBytes+rowBytes > streamPreviewMaxBytes {
			info.Truncated = true
			info.Reason = "size"
			break
		}
		limited.Rows = append(limited.Rows, row)
		info.ResultBytes += rowBytes
	}
	info.RowsRetained = len(limited.Rows)
	if !info.Truncated {
		return result, info, nil
	}
	return limited, info, nil
}

func applyResultLimitPayload(payload map[string]interface{}, info resultLimitInfo) {
	if !info.Truncated {
		return
	}
	payload["previewOnly"] = true
	payload["truncated"] = true
	payload["truncationReason"] = info.Reason
	payload["resultLimitRows"] = streamPreviewMaxRows
	payload["resultLimitBytes"] = streamPreviewMaxBytes
	payload["rowsScanned"] = info.RowsProduced
	payload["rowsProduced"] = info.RowsProduced
	payload["rowsRetained"] = info.RowsRetained
	payload["resultBytes"] = info.ResultBytes
	payload["materialized"] = true
}

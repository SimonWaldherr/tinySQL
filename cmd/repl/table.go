package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
)

// writeResultTable formats each cell once while measuring column widths. Wait
// for the cursor to finish before writing so a late error never prints a
// partial table.
func writeResultTable(w io.Writer, rows *sql.Rows, cols []string, markdown, showCount bool) error {
	widths := make([]int, len(cols))
	cells := make([]any, len(cols))
	dest := make([]any, len(cols))
	positions := make(map[string]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
		dest[i] = &cells[i]
		positions[col] = i
	}
	// The original row maps kept the last value for a duplicate column name.
	sources := make([]int, len(cols))
	for i, col := range cols {
		sources[i] = positions[col]
	}

	var formatted []string
	count := 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		for i, index := range sources {
			value := cell(cells[index])
			widths[i] = max(widths[i], len(value))
			formatted = append(formatted, value)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}

	out := bufio.NewWriter(w)
	line := appendTableRow(nil, cols, widths, markdown)
	// bufio.Writer retains the first write error and returns it from Flush.
	_, _ = out.Write(line)
	line = appendTableDivider(line[:0], widths, markdown)
	_, _ = out.Write(line)
	for i := range count {
		start := i * len(cols)
		line = appendTableRow(line[:0], formatted[start:start+len(cols)], widths, markdown)
		_, _ = out.Write(line)
	}
	if showCount {
		_, _ = fmt.Fprintf(out, "(%d row(s))\n", count)
	}
	return out.Flush()
}

func appendTableRow(line []byte, cells []string, widths []int, markdown bool) []byte {
	if markdown {
		line = append(line, '|')
	}
	for i, value := range cells {
		if markdown {
			line = append(line, ' ')
		} else if i != 0 {
			line = append(line, ' ', ' ')
		}
		line = append(line, value...)
		for range widths[i] - len(value) {
			line = append(line, ' ')
		}
		if markdown {
			line = append(line, ' ', '|')
		}
	}
	return append(line, '\n')
}

func appendTableDivider(line []byte, widths []int, markdown bool) []byte {
	if markdown {
		line = append(line, '|')
	}
	for i, width := range widths {
		if markdown {
			width += 2
		} else if i != 0 {
			line = append(line, ' ', ' ')
		}
		for range width {
			line = append(line, '-')
		}
		if markdown {
			line = append(line, '|')
		}
	}
	return append(line, '\n')
}

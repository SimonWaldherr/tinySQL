// Package engine provides plaintext/fulltext utility functions and table-valued
// functions for RAG (Retrieval-Augmented Generation) workloads.
//
// TEXT_CHUNKS(text, chunk_size [, overlap [, unit]])
//
//	Splits text into overlapping chunks suitable for embedding and retrieval.
//
//	Parameters:
//	  text       – the source document text
//	  chunk_size – maximum tokens (words) per chunk (default 100)
//	  overlap    – number of tokens shared between adjacent chunks (default 20)
//	  unit       – 'words' (default) or 'chars'
//
//	Returns columns: chunk_index INT, chunk_text TEXT, start_pos INT, end_pos INT
//
// TEXT_WORD_COUNT(text)       – count of whitespace-separated words
// TEXT_CHAR_COUNT(text)       – count of Unicode characters
// TEXT_TRUNCATE(text, max_len [, ellipsis]) – truncate to max_len chars
package engine

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ─────────────────────────── TEXT_CHUNKS TVF ─────────────────────────────────

// TextChunksTableFunc implements TEXT_CHUNKS(text, chunk_size [, overlap [, unit]]).
type TextChunksTableFunc struct{}

func (f *TextChunksTableFunc) Name() string { return "TEXT_CHUNKS" }

func (f *TextChunksTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 2 || len(args) > 4 {
		return fmt.Errorf("TEXT_CHUNKS requires 2–4 arguments: (text, chunk_size [, overlap [, unit]])")
	}
	return nil
}

func (f *TextChunksTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	textVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("TEXT_CHUNKS text: %w", err)
	}
	if textVal == nil {
		return &ResultSet{Cols: []string{"chunk_index", "chunk_text", "start_pos", "end_pos"}, Rows: nil}, nil
	}
	text := fmt.Sprintf("%v", textVal)

	chunkSizeVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, fmt.Errorf("TEXT_CHUNKS chunk_size: %w", err)
	}
	chunkSize, err := toInt(chunkSizeVal)
	if err != nil || chunkSize <= 0 {
		return nil, fmt.Errorf("TEXT_CHUNKS: chunk_size must be a positive integer")
	}

	overlap := 0
	if len(args) >= 3 {
		ov, err := evalExpr(env, args[2], row)
		if err == nil && ov != nil {
			if n, err := toInt(ov); err == nil && n >= 0 {
				overlap = n
			}
		}
	}
	// Overlap must be strictly less than chunk_size to ensure forward progress.
	// If overlap >= chunk_size the step would be ≤ 0, producing an infinite loop;
	// we silently cap it at chunk_size-1 so callers with large overlaps still get
	// sensible (if dense) output rather than an error.
	if overlap >= chunkSize {
		overlap = chunkSize - 1
	}

	unit := "words"
	if len(args) >= 4 {
		uv, err := evalExpr(env, args[3], row)
		if err == nil && uv != nil {
			unit = strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", uv)))
		}
	}

	cols := []string{"chunk_index", "chunk_text", "start_pos", "end_pos"}

	var rows []Row
	if unit == "chars" {
		rows = textChunkByChars(text, chunkSize, overlap)
	} else {
		rows = textChunkByWords(text, chunkSize, overlap)
	}

	return &ResultSet{Cols: cols, Rows: rows}, nil
}

// textChunkByWords splits text into word-based chunks.
func textChunkByWords(text string, chunkSize, overlap int) []Row {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	step := chunkSize - overlap
	if step <= 0 {
		step = 1
	}

	var rows []Row
	idx := 0
	for start := 0; start < len(words); start += step {
		end := start + chunkSize
		if end > len(words) {
			end = len(words)
		}
		chunk := strings.Join(words[start:end], " ")
		rows = append(rows, Row{
			"chunk_index": idx,
			"chunk_text":  chunk,
			"start_pos":   start,
			"end_pos":     end,
		})
		idx++
		if end == len(words) {
			break
		}
	}
	return rows
}

// textChunkByChars splits text into character-based chunks.
func textChunkByChars(text string, chunkSize, overlap int) []Row {
	runes := []rune(text)
	if len(runes) == 0 {
		return nil
	}

	step := chunkSize - overlap
	if step <= 0 {
		step = 1
	}

	var rows []Row
	idx := 0
	for start := 0; start < len(runes); start += step {
		end := start + chunkSize
		if end > len(runes) {
			end = len(runes)
		}
		chunk := string(runes[start:end])
		rows = append(rows, Row{
			"chunk_index": idx,
			"chunk_text":  chunk,
			"start_pos":   start,
			"end_pos":     end,
		})
		idx++
		if end == len(runes) {
			break
		}
	}
	return rows
}

func init() {
	RegisterTableFunc(&TextChunksTableFunc{})
}

// ─────────────────────────── Scalar text helpers ─────────────────────────────

// evalTextWordCount returns the number of whitespace-separated words.
func evalTextWordCount(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("TEXT_WORD_COUNT expects 1 argument: (text)")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, nil
	}
	return len(strings.Fields(fmt.Sprintf("%v", v))), nil
}

// evalTextCharCount returns the number of Unicode code points in text.
func evalTextCharCount(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("TEXT_CHAR_COUNT expects 1 argument: (text)")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, nil
	}
	return utf8.RuneCountInString(fmt.Sprintf("%v", v)), nil
}

// evalTextTruncate truncates text to at most max_len characters.
// TEXT_TRUNCATE(text, max_len [, ellipsis])
func evalTextTruncate(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) < 2 || len(ex.Args) > 3 {
		return nil, fmt.Errorf("TEXT_TRUNCATE expects 2–3 arguments: (text, max_len [, ellipsis])")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	maxVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	maxLen, err := toInt(maxVal)
	if err != nil || maxLen < 0 {
		return nil, fmt.Errorf("TEXT_TRUNCATE: max_len must be a non-negative integer")
	}

	ellipsis := "..."
	if len(ex.Args) == 3 {
		ev, err := evalExpr(env, ex.Args[2], row)
		if err == nil && ev != nil {
			ellipsis = fmt.Sprintf("%v", ev)
		}
	}

	text := fmt.Sprintf("%v", v)
	runes := []rune(text)
	if len(runes) <= maxLen {
		return text, nil
	}

	cut := maxLen - utf8.RuneCountInString(ellipsis)
	if cut < 0 {
		cut = 0
	}
	return string(runes[:cut]) + ellipsis, nil
}

// getTextFunctions returns all plaintext scalar function handlers.
func getTextFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"TEXT_WORD_COUNT": evalTextWordCount,
		"TEXT_CHAR_COUNT": evalTextCharCount,
		"TEXT_TRUNCATE":   evalTextTruncate,
	}
}

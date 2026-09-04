//go:build !tinygo.wasm && !baremetal

package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"sync"
)

const htmlTemplateCacheEntries = 128

type boundedHTMLTemplateCache struct {
	mu    sync.RWMutex
	items map[string]*template.Template
	order []string
}

var (
	htmlTemplates = boundedHTMLTemplateCache{items: make(map[string]*template.Template)}
	htmlBuffers   = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

func (c *boundedHTMLTemplateCache) get(source string) (*template.Template, error) {
	c.mu.RLock()
	if cached := c.items[source]; cached != nil {
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	parsed, err := template.New("sql").Parse(source)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached := c.items[source]; cached != nil {
		return cached, nil
	}
	if len(c.order) == htmlTemplateCacheEntries {
		delete(c.items, c.order[0])
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
	}
	c.items[source] = parsed
	c.order = append(c.order, source)
	return parsed, nil
}

func evalHTMLEscape(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("HTML_ESCAPE expects 1 argument")
	}
	value, err := evalExpr(env, ex.Args[0], row)
	if err != nil || value == nil {
		return nil, err
	}
	return html.EscapeString(valueText(value)), nil
}

// evalHTMLTemplate safely renders a Go html/template with JSON data. Parsed
// templates are immutable and concurrency-safe, so a small bounded cache
// amortizes parsing for queries that render the same view for many rows.
func evalHTMLTemplate(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("HTML_TEMPLATE expects 2 arguments: (template, json_data)")
	}
	sourceValue, err := evalExpr(env, ex.Args[0], row)
	if err != nil || sourceValue == nil {
		return nil, err
	}
	dataValue, err := evalExpr(env, ex.Args[1], row)
	if err != nil || dataValue == nil {
		return nil, err
	}
	var data any
	if err := json.Unmarshal([]byte(valueText(dataValue)), &data); err != nil {
		return nil, fmt.Errorf("HTML_TEMPLATE data: %w", err)
	}
	parsed, err := htmlTemplates.get(valueText(sourceValue))
	if err != nil {
		return nil, fmt.Errorf("HTML_TEMPLATE parse: %w", err)
	}
	buffer := htmlBuffers.Get().(*bytes.Buffer)
	buffer.Reset()
	if err := parsed.Execute(buffer, data); err != nil {
		if buffer.Cap() <= 64<<10 {
			htmlBuffers.Put(buffer)
		}
		return nil, fmt.Errorf("HTML_TEMPLATE execute: %w", err)
	}
	result := string(buffer.Bytes())
	if buffer.Cap() <= 64<<10 {
		htmlBuffers.Put(buffer)
	}
	return result, nil
}

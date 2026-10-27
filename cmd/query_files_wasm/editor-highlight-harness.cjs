// Load the production editor highlighter in a small DOM-like harness. The
// benchmark accepts an older app.js path so it can compare working revisions.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function createHarness(appPath = path.join(__dirname, 'app.js')) {
    const source = fs.readFileSync(appPath, 'utf8');
    const slice = (start, end) => {
        const from = source.indexOf(start);
        const to = source.indexOf(end, from);
        if (from < 0 || to < 0) throw new Error(`Could not find ${start}`);
        return source.slice(from, to);
    };
    const handlers = new Map();
    const editor = {
        value: '', selectionStart: 0, scrollTop: 0, scrollLeft: 0,
        addEventListener(type, handler) {
            const list = handlers.get(type) || [];
            list.push(handler);
            handlers.set(type, list);
        },
    };
    function newHighlight() {
        return {
            writes: 0, scrollTop: 0, scrollLeft: 0, _html: '',
            set innerHTML(html) { this.writes++; this._html = html; },
            get innerHTML() { return this._html; },
        };
    }
    const elements = { queryEditor: editor, sqlHighlight: newHighlight(), editorLineCount: { textContent: '' } };
    const sandbox = { document: { getElementById: id => elements[id] }, scheduleEditorStateSave() {} };
    const src = [
        slice('const SQL_KEYWORDS = [', '// Safe references to WASM-exported functions'),
        slice('function escapeRegex(', 'function loadHistory()'),
        slice('function escapeHtml(', '// Escape XML special chars'),
        slice('function setupEditorSyntaxHighlighting()', 'function setupSqlAutocomplete()'),
        'globalThis.setup = setupEditorSyntaxHighlighting; globalThis.sync = syncEditorHighlight; globalThis.lineCount = updateEditorLineCount;',
    ].join('\n');
    vm.runInNewContext(src, sandbox);
    return { editor, elements, newHighlight, setup: sandbox.setup, sync: sandbox.sync, lineCount: sandbox.lineCount,
        emit(type) { for (const handler of handlers.get(type) || []) handler(); } };
}

module.exports = { createHarness };

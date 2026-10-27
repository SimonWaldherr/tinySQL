const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./editor-highlight-harness.cjs');

test('editing highlights once while keyup updates the cursor', () => {
    const h = createHarness();
    h.setup();
    const highlight = h.elements.sqlHighlight;
    assert.equal(highlight.writes, 1);
    h.editor.value = "SELECT 'a<b'\nFROM places";
    h.editor.selectionStart = 7;
    h.emit('input');
    assert.equal(highlight.writes, 2);
    assert.match(highlight.innerHTML, /sql-token keyword/);
    assert.match(highlight.innerHTML, /a&lt;b/);
    assert.equal(h.elements.editorLineCount.textContent, 'Ln 1, Col 8 • 2 lines');
    h.editor.selectionStart = h.editor.value.length;
    h.emit('keyup');
    assert.equal(highlight.writes, 2);
    assert.equal(h.elements.editorLineCount.textContent, 'Ln 2, Col 12 • 2 lines');
});

test('programmatic changes, scroll and replaced overlays stay in sync', () => {
    const h = createHarness();
    h.setup();
    h.editor.value = 'SELECT 2';
    h.editor.scrollTop = 25;
    h.editor.scrollLeft = 10;
    h.sync();
    const highlight = h.elements.sqlHighlight;
    assert.equal(highlight.writes, 2);
    assert.equal(highlight.scrollTop, 25);
    assert.equal(highlight.scrollLeft, 10);
    h.sync();
    assert.equal(highlight.writes, 2);
    h.elements.sqlHighlight = h.newHighlight();
    h.sync();
    assert.equal(h.elements.sqlHighlight.writes, 1);
    assert.equal(h.elements.sqlHighlight.innerHTML, highlight.innerHTML);
    h.editor.value = '';
    h.sync();
    assert.equal(h.elements.sqlHighlight.innerHTML, '');
});

test('cursor position follows text changes, line endings and UTF-16 offsets', () => {
    const h = createHarness();
    h.setup();
    for (const text of ['', 'one\n', 'α😀\r\nline\n\nend', '\n\n']) {
        h.editor.value = text;
        for (let pos = 0; pos <= text.length; pos++) {
            h.editor.selectionStart = pos;
            h.emit('keyup');
            const before = text.slice(0, pos);
            const line = before.split('\n').length;
            const col = pos - before.lastIndexOf('\n');
            const total = text.split('\n').length;
            assert.equal(h.elements.editorLineCount.textContent, `Ln ${line}, Col ${col} • ${total} line${total === 1 ? '' : 's'}`, `${JSON.stringify(text)} at ${pos}`);
        }
    }
});

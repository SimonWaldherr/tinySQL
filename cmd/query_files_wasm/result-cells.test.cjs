const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./result-cells.harness.cjs');

const { format, escape } = createHarness();
const legacyEscape = (value) => String(value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

test('HTML escaping preserves original behavior for diverse values', () => {
    const cases = ['', null, undefined, 0, false, 'plain text', '<script>alert("x&y")</script>',
        "&lt; '&#39; < >", 'München 😀', '\u0000\ud800'];
    // Adjacent and repeated special characters are particularly important:
    // escaping an ampersand must not re-escape replacement entities.
    const alphabet = ['&', '<', '>', '"', "'", 'a', 'Ü', '\n'];
    for (const a of alphabet) for (const b of alphabet) for (const c of alphabet) cases.push(a + b + c);
    for (const value of cases) assert.equal(escape(value), legacyEscape(value), `value ${String(value)}`);
});

test('result cells escape text, titles and GeoJSON while retaining types', () => {
    assert.equal(format(null), '<td class="null-value">NULL</td>');
    assert.equal(format(23), '<td class="number-value">23</td>');
    assert.equal(format(false), '<td class="boolean-value">false</td>');
    const hostile = `<img src=x onerror="alert('x')">&`;
    assert.equal(format(hostile), `<td>${legacyEscape(hostile)}</td>`);
    const long = `<script>${'x'.repeat(120)}&`;
    assert.equal(format(long), `<td class="truncated-cell" title="${legacyEscape(long)}">${legacyEscape(long.slice(0, 120))}…</td>`);
    const point = '{"type":"Point","coordinates":[11,48]}';
    assert.equal(format(point), `<td class="truncated-cell" title="${legacyEscape(point)}">Point(11.0000, 48.0000)</td>`);
    const polygon = '{"type":"<unsafe>","coordinates":[]}';
    assert.equal(format(polygon), `<td class="truncated-cell" title="${legacyEscape(polygon)}">&lt;unsafe&gt; geometry</td>`);
});

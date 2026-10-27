const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Run the production view code without booting the browser application.
const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('let visibleResultsCache = null;');
const end = source.indexOf('let resultFilterTimer = null;', start);
assert.ok(start >= 0 && end > start, 'result view functions are present');
const sandbox = { resultViewState: {}, currentResults: null };
vm.runInNewContext(`${source.slice(start, end)}\nglobalThis.visible = getVisibleResults;`, sandbox);

test('filtered and sorted views are reused without changing source rows', () => {
    const rows = [{name: 'Item 10'}, {name: 'Item 2'}, {name: null}, {name: 'Other'}];
    const source = { columns: ['name'], rows, duration: '1 ms' };
    sandbox.resultViewState = { filterText: ' ITEM ', sortColumn: 'name', sortDirection: 'asc' };
    const first = sandbox.visible(source);
    assert.deepEqual(Array.from(first.rows, row => row.name), ['Item 2', 'Item 10']);
    assert.deepEqual(rows.map(row => row.name), ['Item 10', 'Item 2', null, 'Other']);
    source.duration = '2 ms';
    const nextPage = sandbox.visible(source);
    assert.equal(nextPage.rows, first.rows);
    assert.equal(nextPage.duration, '2 ms');

    sandbox.resultViewState.sortDirection = 'desc';
    const descending = sandbox.visible(source);
    assert.notEqual(descending.rows, first.rows);
    assert.deepEqual(Array.from(descending.rows, row => row.name), ['Item 10', 'Item 2']);
    sandbox.resultViewState.filterText = 'other';
    assert.deepEqual(Array.from(sandbox.visible(source).rows, row => row.name), ['Other']);
});

test('stream append and replacement invalidate the cached view', () => {
    const source = { columns: ['name'], rows: [{name: 'z'}], duration: '' };
    sandbox.resultViewState = { filterText: '', sortColumn: 'name', sortDirection: 'asc' };
    const before = sandbox.visible(source);
    source.rows.push({name: 'a'});
    const appended = sandbox.visible(source);
    assert.notEqual(appended.rows, before.rows);
    assert.deepEqual(Array.from(appended.rows, row => row.name), ['a', 'z']);
    source.rows = [{name: 'b'}, {name: 'c'}];
    assert.deepEqual(Array.from(sandbox.visible(source).rows, row => row.name), ['b', 'c']);
    sandbox.resultViewState.filterText = 'b';
    assert.equal(sandbox.visible(source).rows.length, 1);
    source.columns = ['other'];
    assert.equal(sandbox.visible(source).rows.length, 0);
    sandbox.resultViewState = { filterText: '', sortColumn: '', sortDirection: 'asc' };
    assert.equal(sandbox.visible(source).rows, source.rows);
});

test('server pages and empty sources bypass the local derived-view cache', () => {
    const source = { columns: ['name'], rows: [{name: 'b'}, {name: 'a'}], serverPaged: true, filteredRowCount: 30 };
    sandbox.resultViewState = { filterText: 'none', sortColumn: 'name', sortDirection: 'asc' };
    const page = sandbox.visible(source);
    assert.deepEqual(Array.from(page.rows, row => row.name), ['b', 'a']);
    assert.notEqual(page.rows, source.rows);
    assert.equal(page.rowCount, 30);
    assert.equal(sandbox.visible(null), null);
});

test('local sort preserves numeric, text, null and stable tie ordering', () => {
    const values = [null, '10', 2, '2', 'Item 10', 'item 2', '', false, 0, 'Ä', 'a', undefined, ' 3 ', -1];
    const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
    const referenceCompare = (left, right) => {
        if (left === right) return 0;
        if (left == null) return 1;
        if (right == null) return -1;
        const leftNumber = typeof left === 'number' ? left : Number(left);
        const rightNumber = typeof right === 'number' ? right : Number(right);
        if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) return leftNumber - rightNumber;
        return collator.compare(String(left), String(right));
    };
    const rows = values.map((value, id) => ({ value, id }));
    for (const direction of ['asc', 'desc']) {
        sandbox.resultViewState = { filterText: '', sortColumn: 'value', sortDirection: direction };
        const expected = rows.slice().sort((a, b) => (direction === 'desc' ? -1 : 1) * referenceCompare(a.value, b.value));
        const result = sandbox.visible({columns: ['value'], rows});
        assert.deepEqual(Array.from(result.rows, row => row.id), expected.map(row => row.id));
        assert.deepEqual(rows.map(row => row.id), values.map((_, index) => index));
    }
});

test('pivot grid cannot change the cached view or its source rows', () => {
    const start = source.indexOf('function openInVanillaGrid()');
    const end = source.indexOf('// Reuse the derived view', start);
    assert.ok(start >= 0 && end > start);
    const rows = [{name: 'original'}, {name: 'second'}];
    const gridSandbox = {
        getVisibleResults: () => ({columns: ['name'], rows}),
        window: { renderVanillaGrid(result) { result.rows[0].name = 'changed'; result.rows.reverse(); } },
    };
    vm.runInNewContext(`${source.slice(start, end)}\nopenInVanillaGrid();`, gridSandbox);
    assert.deepEqual(rows.map(row => row.name), ['original', 'second']);
});

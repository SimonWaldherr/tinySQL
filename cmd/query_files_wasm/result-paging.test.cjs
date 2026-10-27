const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('async function renderResults(data)');
const end = source.indexOf('// Delegated click handling', start);
assert.ok(start >= 0 && end > start);

function deferred() {
    let resolve, reject;
    const promise = new Promise((ok, fail) => { resolve = ok; reject = fail; });
    return {promise, resolve, reject};
}

function fixture() {
    const container = {innerHTML: ''};
    const requests = [];
    const data = {rows: [{id: 0}], columns: ['id'], rowCount: 200, filteredRowCount: 200,
        serverPaged: true, pageKey: '', pageOffset: 0, duration: '1 ms'};
    const sandbox = {
        currentResults: data,
        resultViewState: {page: 1, pageSize: 100, filterText: '', sortColumn: '', sortDirection: 'asc'},
        RESULT_PAGE_SIZES: [50, 100, 250, 500], DEFAULT_RESULT_PAGE_SIZE: 100,
        document: {getElementById: id => id === 'resultsContainer' ? container : null},
        wasmApi: {getResultPage: () => { const d = deferred(); requests.push(d); return d.promise; }},
        window: {}, isWorkspaceTransitioning: () => false,
        setOpenVanillaGridEnabled() {}, setupResultsSortDelegation() {}, getSortIndicator: () => '',
        escapeHtml: String, formatCell: value => `<td>${value}</td>`,
        getVisibleResults: value => ({columns: value.columns, rows: value.rows}),
    };
    vm.runInNewContext(`${source.slice(start, end)}; globalThis.render = renderResults`, sandbox);
    return {sandbox, data, container, requests};
}

test('a stale paging failure does not replace the newer page', async () => {
    const {sandbox, data, container, requests} = fixture();
    const older = sandbox.render(data);
    sandbox.resultViewState.page = 2;
    const newer = sandbox.render(data);
    requests[1].resolve({success: true, rows: [{id: 101}], filteredRows: 200, pageOffset: 100});
    await newer;
    assert.match(container.innerHTML, /<td>101<\/td>/);
    requests[0].reject(new Error('old failure'));
    await older;
    assert.match(container.innerHTML, /<td>101<\/td>/);
    assert.doesNotMatch(container.innerHTML, /old failure/);
});

test('returning to the same page does not revive an older request', async () => {
    const {sandbox, data, container, requests} = fixture();
    const older = sandbox.render(data);
    sandbox.resultViewState.page = 2;
    const middle = sandbox.render(data);
    sandbox.resultViewState.page = 1;
    const newer = sandbox.render(data);
    requests[2].resolve({success: true, rows: [{id: 1}], filteredRows: 200, pageOffset: 0});
    await newer;
    requests[1].resolve({success: true, rows: [{id: 101}], filteredRows: 200, pageOffset: 100});
    await middle;
    requests[0].reject(new Error('stale same-page failure'));
    await older;
    assert.match(container.innerHTML, /<td>1<\/td>/);
    assert.doesNotMatch(container.innerHTML, /stale same-page failure|<td>101<\/td>/);
});

test('a current paging failure is still displayed', async () => {
    const {sandbox, data, container, requests} = fixture();
    const pending = sandbox.render(data);
    requests[0].reject(new Error('current failure'));
    await pending;
    assert.match(container.innerHTML, /current failure/);
});

test('a no-match result retains the filter for editing and can recover', async () => {
    const {sandbox, data, container, requests} = fixture();
    sandbox.resultViewState.filterText = 'nomatch';
    const empty = sandbox.render(data);
    requests[0].resolve({success: true, rows: [], filteredRows: 0, pageOffset: 0});
    await empty;
    assert.match(container.innerHTML, /No Matching Rows/);
    assert.match(container.innerHTML, /id="resultFilterInput"[^>]*value="nomatch"/);
    assert.match(container.innerHTML, /oninput="scheduleResultFilterUpdate\(\)"/);
    sandbox.resultViewState.filterText = 'match';
    const recovered = sandbox.render(data);
    requests[1].resolve({success: true, rows: [{id: 7}], filteredRows: 1, pageOffset: 0});
    await recovered;
    assert.match(container.innerHTML, /<td>7<\/td>/);
});

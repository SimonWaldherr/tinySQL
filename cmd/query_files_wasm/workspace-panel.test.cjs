const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createHarness } = require('./workspace-panel-harness.cjs');

const workspace = (id, name) => ({ id, name, updatedAt: '2026-10-27T10:00:00Z', sizeBytes: 100 });

test('autosave refresh updates the selector while the closed panel waits until opening', async () => {
    const h = createHarness();
    let records = [workspace('a', 'Alpha')];
    h.sandbox.workspaceStore.listWorkspaces = async () => records;
    await h.sandbox.refresh();
    assert.match(h.selector.innerHTML, /Alpha/);
    assert.equal(h.list.writes, 0);
    records = [workspace('a', 'Beta'), workspace('b', '<Other>')];
    await h.sandbox.refresh();
    assert.match(h.selector.innerHTML, /Beta/);
    assert.match(h.selector.innerHTML, /&lt;Other&gt;/);
    assert.equal(h.list.writes, 0);
    await h.sandbox.openPanel();
    assert.equal(h.panel.classList.contains('hidden'), false);
    assert.equal(h.list.writes, 1);
    assert.match(h.list.innerHTML, /Beta/);
    assert.match(h.list.innerHTML, /&lt;Other&gt;/);
});

test('visible panel tracks changes and refreshes again after closing', async () => {
    const h = createHarness();
    let records = [workspace('a', 'One')];
    h.sandbox.workspaceStore.listWorkspaces = async () => records;
    await h.sandbox.openPanel();
    records = [workspace('a', 'Two')];
    await h.sandbox.refresh();
    assert.match(h.list.innerHTML, /Two/);
    assert.equal(h.list.writes, 2);
    h.sandbox.closePanel();
    records = [workspace('a', 'Three')];
    await h.sandbox.refresh();
    assert.match(h.selector.innerHTML, /Three/);
    assert.match(h.list.innerHTML, /Two/);
    await h.sandbox.openPanel();
    assert.match(h.list.innerHTML, /Three/);
    assert.equal(h.list.writes, 3);
});

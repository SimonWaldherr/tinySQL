// Compare repeated post-autosave refreshes with an earlier app.js path.
// The mocked DOM counts HTML writes; actual IndexedDB and layout are excluded.
const { performance } = require('node:perf_hooks');
const { createHarness } = require('./workspace-panel-harness.cjs');

(async () => {
    for (const count of [1, 3, 20]) {
        const h = createHarness(process.argv[2]);
        const workspaces = Array.from({ length: count }, (_, i) => ({
            id: i === 0 ? 'a' : `ws-${i}`, name: `Workspace ${i}`,
            updatedAt: '2026-10-27T10:00:00Z', sizeBytes: i * 1024,
        }));
        h.sandbox.workspaceStore.listWorkspaces = async () => workspaces;
        for (let i = 0; i < 100; i++) await h.sandbox.refresh();
        const timings = [];
        for (let i = 0; i < 500; i++) {
            const started = performance.now();
            await h.sandbox.refresh();
            timings.push(performance.now() - started);
        }
        timings.sort((a, b) => a - b);
        console.log(`${count} workspaces: ${timings[250].toFixed(4)} ms median; ${h.list.writes} panel writes / 600 refreshes`);
    }
})().catch(error => { console.error(error); process.exitCode = 1; });

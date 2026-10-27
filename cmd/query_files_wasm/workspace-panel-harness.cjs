// Exercise the actual workspace selector and panel functions without a DOM.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function createHarness(appPath = path.join(__dirname, 'app.js')) {
    const source = fs.readFileSync(appPath, 'utf8');
    const start = source.indexOf('async function refreshWorkspaceSelector(');
    const end = source.indexOf('async function switchWorkspace(', start);
    if (start < 0 || end <= start) throw new Error('Workspace panel functions not found');
    function element() {
        return {
            value: '', disabled: false, writes: 0, _html: '',
            set innerHTML(value) { this._html = value; this.writes++; },
            get innerHTML() { return this._html; },
            setAttribute() {}, addEventListener() {}, focus() {},
        };
    }
    const selector = element(), list = element(), nameInput = element();
    let hidden = true;
    const panel = {
        classList: {
            contains(name) { return name === 'hidden' && hidden; },
            remove(name) { if (name === 'hidden') hidden = false; },
            add(name) { if (name === 'hidden') hidden = true; },
        },
        setAttribute() {},
    };
    const elements = { workspaceSelect: selector, workspaceList: list, workspacePanel: panel, workspaceNameInput: nameInput };
    const sandbox = {
        document: { getElementById: id => elements[id] || null },
        workspaceStore: { listWorkspaces: async () => [] },
        activeWorkspaceId: 'a',
        isWorkspaceTransitioning: () => false,
        formatRuntimeBytes: bytes => `${bytes} B`,
        escapeHtml: value => String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch])),
        showToast() {},
    };
    vm.runInNewContext(`${source.slice(start, end)}\nglobalThis.refresh = refreshWorkspaceSelector; globalThis.openPanel = openWorkspacePanel; globalThis.closePanel = closeWorkspacePanel;`, sandbox);
    return { sandbox, selector, list, panel };
}
module.exports = { createHarness };

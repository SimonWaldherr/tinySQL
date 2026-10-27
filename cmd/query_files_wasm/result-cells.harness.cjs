const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function createHarness(appPath = path.join(__dirname, 'app.js')) {
    const source = fs.readFileSync(appPath, 'utf8');
    const start = source.indexOf('function formatCell(');
    const end = source.indexOf('// Show upload dialog', start);
    const escapeStart = source.indexOf('function escapeHtml(');
    const escapeEnd = source.indexOf('// Escape XML', escapeStart);
    if (start < 0 || end <= start || escapeStart < 0 || escapeEnd <= escapeStart) {
        throw new Error('Result cell functions not found');
    }
    const sandbox = {};
    vm.runInNewContext(`${source.slice(start, end)}\n${source.slice(escapeStart, escapeEnd)}\nglobalThis.format = formatCell; globalThis.escape = escapeHtml;`, sandbox);
    return sandbox;
}

module.exports = { createHarness };

const { test: base, expect } = require('@playwright/test');
const CORRUPT_SNAPSHOT = 'not a tinySQL snapshot';

const test = base.extend({
    page: async ({ page, context, baseURL }, use) => {
        const errors = [];
        page.on('pageerror', error => errors.push(error.message));
        // CSV/JSON and the normal table view need no optional CDN libraries.
        // Block them so these tests also run without third-party services.
        await context.route('**/*', route => new URL(route.request().url()).origin === baseURL
            ? route.continue() : route.abort());
        await use(page);
        expect(errors, 'uncaught browser errors').toEqual([]);
    },
});

async function ready(page) {
    await expect(page.locator('#executeBtn')).toBeEnabled();
    await expect(page.locator('#workspaceSelect')).toBeEnabled();
    const launch = page.getByRole('button', { name: 'Open browser studio', exact: true });
    if (await launch.isVisible()) await launch.click();
}

test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Open browser studio', exact: true }).click();
    await ready(page);
});

async function importFiles(page, files) {
    await page.locator('#fileInput').setInputFiles(files.map(([name, text]) => ({
        name, mimeType: 'text/plain', buffer: Buffer.from(text),
    })));
    await expect(page.locator('#fileInput')).toHaveValue('');
    await expect(page.locator('#statusText')).toContainText('Imported');
}

async function query(page, sql, expected) {
    await page.getByRole('textbox', { name: 'SQL query editor' }).fill(sql);
    await page.getByRole('button', { name: 'Execute SQL query', exact: true }).click();
    await expect(page.locator('#resultsContainer .result-table tbody tr')).toHaveCount(expected.length);
    await expect(page.locator('#resultsContainer .result-table tbody td:not(.row-num-col)')).toHaveText(expected.flat());
    await expect(page.locator('#resultsContainer')).toHaveAttribute('aria-busy', 'false');
}

async function workspace(page, id) {
    return page.evaluate(async id => {
        const store = new TinySQLWorkspaceStorage.WorkspaceStorage();
        try {
            await store.open();
            return await store.getWorkspace(id);
        } finally {
            await store.close();
        }
    }, id);
}

async function saved(page, id, backend = 'indexeddb') {
    await expect.poll(async () => {
        const current = await workspace(page, id);
        return Boolean(current.currentGeneration && current.sizeBytes > 0 && current.backend === backend);
    }, { message: `workspace snapshot committed to ${backend}`, timeout: 30_000 }).toBe(true);
    return workspace(page, id);
}

async function corruptSnapshot(page, id) {
    return page.evaluate(async ({ id, corruptText }) => {
        const store = new TinySQLWorkspaceStorage.WorkspaceStorage();
        try {
            await store.open();
            const broken = await store.saveWorkspace(id, new TextEncoder().encode(corruptText), {
                format: 'tinysql-gob', keepGenerations: 3,
            });
            return broken.currentGeneration;
        } finally {
            await store.close();
        }
    }, { id, corruptText: CORRUPT_SNAPSHOT });
}

test('imports CSV and JSONL through the file input and queries their values', async ({ page }) => {
    await importFiles(page, [
        ['people.csv', 'id,name\r\n1,"Müller, Zoë"\r\n2,O\'Hara\r\n'],
        ['events.jsonl', '{"id":1,"note":"it\'s {color:blue}"}\n{"id":2,"note":"Grüße"}\n'],
    ]);
    await expect(page.locator('[data-table-name="people"]')).toBeVisible();
    await expect(page.locator('[data-table-name="events"]')).toBeVisible();
    await query(page, 'SELECT id, name FROM people ORDER BY id', [['1', 'Müller, Zoë'], ['2', "O'Hara"]]);
    await query(page, 'SELECT id, note FROM events ORDER BY id', [['1', "it's {color:blue}"], ['2', 'Grüße']]);
});

test('restores an autosaved IndexedDB snapshot after reload', async ({ page }) => {
    const id = await page.locator('#workspaceSelect').inputValue();
    await importFiles(page, [['records.csv', 'id,value\n1,persisted\n2,after reload\n']]);
    await saved(page, id);
    await page.reload();
    await ready(page);
    await expect(page.locator('#workspaceSelect')).toHaveValue(id);
    await query(page, 'SELECT id, value FROM records ORDER BY id', [['1', 'persisted'], ['2', 'after reload']]);
});

test('keeps workspaces isolated when switching immediately after import', async ({ page }) => {
    const first = await page.locator('#workspaceSelect').inputValue();
    await importFiles(page, [['records.csv', 'id,value\n11,first workspace\n']]);
    // Do not wait for autosave: creating/switching must flush pending writes.
    await page.getByRole('button', { name: 'Create workspace', exact: true }).click();
    await page.getByRole('textbox', { name: 'New workspace name' }).fill('Second workspace');
    await page.locator('#workspaceCreateButton').click();
    await expect(page.locator('#statusText')).toHaveText('Created workspace: Second workspace');
    const second = await page.locator('#workspaceSelect').inputValue();
    expect(second).not.toBe(first);
    await expect(page.locator('[data-table-name="records"]')).toHaveCount(0);
    await importFiles(page, [['records.csv', 'id,value\n22,second workspace\n']]);
    await page.getByRole('combobox', { name: 'Select workspace' }).selectOption(first);
    await expect(page.locator('#statusText')).toHaveText('Opened workspace: My workspace');
    await query(page, 'SELECT id, value FROM records', [['11', 'first workspace']]);
    await page.getByRole('combobox', { name: 'Select workspace' }).selectOption(second);
    await expect(page.locator('#statusText')).toHaveText('Opened workspace: Second workspace');
    await query(page, 'SELECT id, value FROM records', [['22', 'second workspace']]);
    await page.reload();
    await ready(page);
    await expect(page.locator('#workspaceSelect')).toHaveValue(second);
    await query(page, 'SELECT id, value FROM records', [['22', 'second workspace']]);
});

test('recovers an older valid snapshot when the newest generation is corrupt', async ({ page }) => {
    const id = await page.locator('#workspaceSelect').inputValue();
    await importFiles(page, [['records.csv', 'id,value\n7,recovered\n']]);
    const good = await saved(page, id);
    const broken = await corruptSnapshot(page, id);
    expect(broken).toBeGreaterThan(good.currentGeneration);
    await page.reload();
    await ready(page);
    await query(page, 'SELECT id, value FROM records', [['7', 'recovered']]);
    expect((await workspace(page, id)).currentGeneration).toBe(good.currentGeneration);
});

test('blocks edits after failed recovery and preserves the corrupt snapshot', async ({ page }) => {
    const id = await page.locator('#workspaceSelect').inputValue();
    const generation = await corruptSnapshot(page, id);
    await page.reload();
    await page.getByRole('button', { name: 'Open browser studio', exact: true }).click();
    await expect(page.locator('#statusText')).toContainText('Saved workspace could not be restored');
    await expect(page.locator('#executeBtn')).toBeDisabled();
    await page.getByRole('button', { name: 'Create workspace', exact: true }).click();
    await page.getByRole('textbox', { name: 'New workspace name' }).fill('Must not be created');
    await page.locator('#workspaceCreateButton').click();
    await expect(page.getByText('Workspaces are still initializing.', { exact: true })).toBeVisible();
    await expect(page.locator('#workspaceSelect option')).toHaveCount(1);
    expect((await workspace(page, id)).currentGeneration).toBe(generation);
    const bytes = await page.evaluate(async id => {
        const store = new TinySQLWorkspaceStorage.WorkspaceStorage();
        try {
            await store.open();
            return Array.from(new Uint8Array((await store.loadWorkspace(id)).snapshot));
        } finally {
            await store.close();
        }
    }, id);
    expect(bytes).toEqual(Array.from(Buffer.from(CORRUPT_SNAPSHOT)));
});

test('restores a large OPFS snapshot after reload', async ({ page }) => {
    test.setTimeout(120_000);
    const id = await page.locator('#workspaceSelect').inputValue();
    const length = 9 * 1024 * 1024;
    await importFiles(page, [['large_payload.csv', `id,body\n7,${'x'.repeat(length)}\n`]]);
    await saved(page, id, 'opfs');
    await page.reload();
    await ready(page);
    await query(page, 'SELECT id, LENGTH(body) AS size FROM large_payload', [['7', String(length)]]);
});

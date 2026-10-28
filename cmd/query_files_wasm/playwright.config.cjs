const { defineConfig, devices } = require('@playwright/test');

const port = Number(process.env.TINYSQL_BROWSER_PORT || 4173);
const baseURL = `http://127.0.0.1:${port}`;

module.exports = defineConfig({
    testDir: './tests/browser',
    testMatch: '*.spec.cjs',
    forbidOnly: Boolean(process.env.CI),
    workers: 1,
    retries: 0,
    timeout: 60_000,
    expect: { timeout: 15_000 },
    reporter: [['list'], ['html', { open: 'never' }]],
    use: {
        baseURL,
        trace: 'retain-on-failure',
        screenshot: 'only-on-failure',
    },
    projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
    webServer: {
        command: 'node tests/browser/server.cjs',
        url: baseURL,
        env: { TINYSQL_BROWSER_PORT: String(port) },
        reuseExistingServer: false,
        timeout: 180_000,
    },
});

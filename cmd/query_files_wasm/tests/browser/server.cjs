// Build fresh test assets outside the checkout and serve the real application.
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const { gzipSync } = require('node:zlib');

const root = path.resolve(__dirname, '../..');
const output = fs.mkdtempSync(path.join(os.tmpdir(), 'tinysql-browser-'));
process.on('exit', () => fs.rmSync(output, { recursive: true, force: true }));

const built = spawnSync('go', [
    'build', '-tags', 'no_http', '-trimpath', '-buildvcs=false',
    '-ldflags', '-s -w', '-o', path.join(output, 'query_files.wasm'), '.',
], { cwd: root, env: { ...process.env, GOOS: 'js', GOARCH: 'wasm' }, stdio: 'inherit' });
if (built.error) throw built.error;
if (built.status !== 0) process.exit(built.status || 1);
const goroot = spawnSync('go', ['env', 'GOROOT'], { cwd: root, encoding: 'utf8' });
if (goroot.error) throw goroot.error;
if (goroot.status !== 0) throw new Error(goroot.stderr);
fs.copyFileSync(path.join(goroot.stdout.trim(), 'lib/wasm/wasm_exec.js'), path.join(output, 'wasm_exec.js'));
fs.writeFileSync(path.join(output, 'query_files.wasm.gz'), gzipSync(fs.readFileSync(path.join(output, 'query_files.wasm'))));

const assets = new Map([
    ['/', ['index.html', 'text/html; charset=utf-8']],
    ['/index.html', ['index.html', 'text/html; charset=utf-8']],
    ...['app.js', 'workspace-storage.js', 'wasm-client.js', 'wasm-worker.js'].map(name => [
        `/${name}`, [name, 'text/javascript; charset=utf-8'],
    ]),
    ['/wasm_exec.js', [path.join(output, 'wasm_exec.js'), 'text/javascript; charset=utf-8']],
    ['/query_files.wasm', [path.join(output, 'query_files.wasm'), 'application/wasm']],
    ['/query_files.wasm.gz', [path.join(output, 'query_files.wasm.gz'), 'application/gzip']],
]);

const server = http.createServer((request, response) => {
    const asset = assets.get(new URL(request.url, 'http://localhost').pathname);
    if (!asset) {
        response.writeHead(404).end();
        return;
    }
    response.writeHead(200, { 'Content-Type': asset[1], 'Cache-Control': 'no-store' });
    fs.createReadStream(path.resolve(root, asset[0])).pipe(response);
});
server.listen(Number(process.env.TINYSQL_BROWSER_PORT || 4173), '127.0.0.1');
for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => server.close(() => process.exit(0)));
}

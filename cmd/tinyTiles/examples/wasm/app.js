/* global Go */
(function () {
  "use strict";

  const status = document.getElementById("status");
  const value = (id) => document.getElementById(id).value;
  const integer = (id) => {
    const raw = value(id).trim(), n = Number(raw);
    if (!raw || !Number.isSafeInteger(n)) throw new Error(`${id}: enter a whole number`);
    return n;
  };
  let ready = false, opened = false, busy = false;
  function controls() {
    for (const id of ["open", "close", "sync", "get"]) {
      document.getElementById(id).disabled = busy || !ready || (id === "open" ? opened : !opened);
    }
    for (const input of document.querySelectorAll("input")) input.disabled = busy;
  }
  async function action(message, operation) {
    if (busy || !ready) return;
    busy = true; controls(); write(message);
    const started = performance.now();
    try { const result = await operation(); write({ result, elapsed_ms: Math.round(performance.now() - started) }); }
    catch (error) { write({ error: String(error) }); }
    finally { busy = false; controls(); }
  }
  const write = (message) => { status.textContent = typeof message === "string" ? message : JSON.stringify(message, null, 2); };

  async function instantiate() {
    const go = new Go();
    const response = await fetch("tinytiles.wasm");
    if (!response.ok) throw new Error(`WASM download failed: HTTP ${response.status}`);
    const streaming = typeof WebAssembly.instantiateStreaming === "function" &&
      (response.headers.get("Content-Type") || "").split(";")[0].trim() === "application/wasm";
    const result = streaming
      ? await WebAssembly.instantiateStreaming(response, go.importObject)
      : await WebAssembly.instantiate(await response.arrayBuffer(), go.importObject);
    // A MIME fallback uses the same response, never a second WASM download.
    go.run(result.instance).catch((error) => {
      ready = false; controls(); write({ error: String(error) });
    });
    for (let attempt = 0; attempt < 200 && !window.tinyTiles; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    if (!window.tinyTiles) throw new Error("tinyTiles WASM API did not initialize");
    ready = true; controls();
    write({ ready: true, version: window.tinyTiles.version, message: "Open an IndexedDB cache, then synchronize a range." });
  }

  function request() {
    const z = integer("z"), xmin = integer("xmin"), xmax = integer("xmax");
    const ymin = integer("ymin"), ymax = integer("ymax"), concurrency = integer("concurrency");
    if (z < 0 || z > 30 || xmin < 0 || ymin < 0 || xmax < xmin || ymax < ymin || xmax >= 2 ** z || ymax >= 2 ** z) {
      throw new Error("Enter an ordered TMS range inside the selected zoom level.");
    }
    if (concurrency < 1 || concurrency > 32) throw new Error("Workers must be between 1 and 32.");
    if ((xmax - xmin + 1) * (ymax - ymin + 1) > 1024) throw new Error("This demo accepts at most 1,024 tiles per sync.");
    return {
      dataset: value("dataset").trim() || undefined,
      ranges: [{
        z: integer("z"),
        x_min: integer("xmin"),
        x_max: integer("xmax"),
        y_min: integer("ymin"),
        y_max: integer("ymax")
      }],
      concurrency: integer("concurrency"),
      prune_previous: false
    };
  }

  document.getElementById("open").addEventListener("click", () => action("Opening cache…", async () => {
    const name = value("cache-name").trim();
    if (!name) throw new Error("Enter an IndexedDB name.");
    const result = await window.tinyTiles.open(name); opened = true; return result;
  }));
  document.getElementById("close").addEventListener("click", () => action("Closing cache…", async () => {
    const result = await window.tinyTiles.close(); opened = false; return result;
  }));
  document.getElementById("sync").addEventListener("click", () => action("Synchronizing tiles…", async () => {
    const input = request();
    return window.tinyTiles.sync(value("manifest-url").trim(), input);
  }));
  document.getElementById("get").addEventListener("click", () => action("Reading first tile…", async () => {
    request();
    const dataset = value("dataset").trim();
    if (!dataset) throw new Error("Enter the dataset name to read a tile.");
    const tile = await window.tinyTiles.get(dataset, integer("z"), integer("xmin"), integer("ymin"));
    return { found: tile.found, revision: tile.revision, bytes: tile.data ? tile.data.byteLength : 0, contentType: tile.contentType, checksum: tile.checksum };
  }));

  controls();
  instantiate().catch((error) => { ready = false; controls(); write({ error: String(error) }); });
}());

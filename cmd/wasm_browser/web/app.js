    // WASM Integration
    let wasmReady = false;
    let dbConnected = false;
    let liveCatalogData = null;

    async function instantiateWasm(go) {
      // Prefer instantiateStreaming, but fall back to fetch+instantiate for Safari or wrong MIME
      const wasmURL = 'tinySQL.wasm';
      if (WebAssembly.instantiateStreaming) {
        try {
          return await WebAssembly.instantiateStreaming(fetch(wasmURL), go.importObject);
        } catch (e) {
          console.warn('instantiateStreaming failed, falling back to ArrayBuffer:', e);
        }
      }
      const resp = await fetch(wasmURL);
      const bytes = await resp.arrayBuffer();
      return await WebAssembly.instantiate(bytes, go.importObject);
    }

    async function initWasm() {
      try {
        const go = new Go();
        const result = await instantiateWasm(go);
        go.run(result.instance || result);

        // Wait for tinySQL to be available
        let attempts = 0;
        while (!window.tinySQL && attempts < 50) {
          await new Promise(resolve => setTimeout(resolve, 100));
          attempts++;
        }

        if (window.tinySQL) {
          wasmReady = true;
          console.log('[Reference] wasmReady set to true');
          // Auto-connect database
          const res = await window.tinySQL.open();
          console.log('[Reference] open() result:', res);
          // Parse JSON response if it's a string
          const result = typeof res === 'string' ? JSON.parse(res) : res;
          if (result && result.success) {
            dbConnected = true;
            console.log('[Reference] dbConnected set to true');
            await loadLiveCatalog();
          } else {
            console.error('[Reference] Database connection failed:', result);
          }
        } else {
          throw new Error('tinySQL API not available after 5 seconds');
        }
      } catch (error) {
        console.error('WASM loading failed:', error);
        showNotification('WASM konnte nicht geladen werden. Bitte Seite neu laden.', 'error');
      }
    }

    // ----- I18N support -----
    const i18n = {
      en: {
        title: 'tinySQL Function Reference',
        subtitle: 'Interactive reference and examples for tinySQL functions',
        back: 'Back to Demo',
        listTables: 'List Tables',
        describeTable: 'Describe Table',
        begin: 'BEGIN',
        commit: 'COMMIT',
        rollback: 'ROLLBACK',
        createView: 'Create View',
        listViews: 'List Views',
        showMeta: 'Show Meta',
        explain: 'Explain',
        tablePlaceholder: 'table name',
        explainPlaceholder: 'SELECT ...',
        tryButton: 'Try It Yourself',
        generatedOverview: 'Automatically generated function overview',
        example: 'example',
        examples: 'examples',
        liveCatalog: 'Live catalog',
        catalogSource: 'Complete runtime inventory from catalog.functions and sys.procedures.',
        functions: 'functions',
        proceduresCount: 'stored procedures',
        scalar: 'Scalar functions',
        aggregate: 'Aggregate functions',
        window: 'Window functions',
        table: 'Table-valued functions',
        procedures: 'Stored procedures',
        procedureDescription: 'Process-local procedures registered in the active tinySQL runtime.',
        wasmNotReady: 'WASM not ready. Please reload the page.'
      },
      de: {
        title: 'tinySQL Function Reference',
        subtitle: 'Interactive reference and examples for tinySQL functions',
        back: '⬅ Zurück zur Demo',
        listTables: 'List Tables',
        describeTable: 'Describe Table',
        begin: 'BEGIN',
        commit: 'COMMIT',
        rollback: 'ROLLBACK',
        createView: 'Create View',
        listViews: 'List Views',
        showMeta: 'Show Meta',
        explain: 'Explain',
        tablePlaceholder: 'table name',
        explainPlaceholder: 'SELECT ...',
        tryButton: '▶ Try It Yourself',
        generatedOverview: 'Automatisch erzeugte Funktionsübersicht',
        example: 'Beispiel',
        examples: 'Beispiele',
        liveCatalog: 'Laufzeitkatalog',
        catalogSource: 'Vollständige Laufzeit-Inventur aus catalog.functions und sys.procedures.',
        functions: 'Funktionen',
        proceduresCount: 'gespeicherte Prozeduren',
        scalar: 'Skalare Funktionen',
        aggregate: 'Aggregatfunktionen',
        window: 'Window-Funktionen',
        table: 'Tabellenfunktionen',
        procedures: 'Gespeicherte Prozeduren',
        procedureDescription: 'Prozesslokale Prozeduren, die in der aktiven tinySQL-Laufzeit registriert sind.',
        wasmNotReady: 'WASM nicht bereit. Bitte Seite neu laden.'
      }
    };

    function generatedExampleLabel(section, count, lang) {
      const map = i18n[lang] || i18n.de;
      return `${section} (${count} ${count === 1 ? map.example : map.examples})`;
    }

    function setLanguage(lang) {
      const map = i18n[lang] || i18n.de;
      document.querySelector('[data-i18n="title"]').textContent = map.title;
      document.querySelector('[data-i18n="subtitle"]').textContent = map.subtitle;
      document.querySelector('#backDemo').textContent = map.back;
      document.getElementById('btnListTables').textContent = map.listTables;
      document.getElementById('btnDescribeTable').textContent = map.describeTable;
      document.getElementById('btnBegin').textContent = map.begin;
      document.getElementById('btnCommit').textContent = map.commit;
      document.getElementById('btnRollback').textContent = map.rollback;
      document.getElementById('btnCreateView').textContent = map.createView;
      document.getElementById('btnListViews').textContent = map.listViews;
      document.getElementById('btnShowMeta').textContent = map.showMeta;
      document.getElementById('btnExplain').textContent = map.explain;
      document.getElementById('tblNameInput').placeholder = map.tablePlaceholder;
      document.getElementById('explainSql').placeholder = map.explainPlaceholder;
      // update try buttons
      document.querySelectorAll('.try-button').forEach(b => b.textContent = map.tryButton);
      document.querySelectorAll('[data-generated-overview]').forEach(h => h.textContent = map.generatedOverview);
      document.querySelectorAll('[data-example-count]').forEach(summary => {
        summary.textContent = generatedExampleLabel(
          summary.dataset.section,
          Number(summary.dataset.exampleCount),
          lang
        );
      });
      if (liveCatalogData) renderLiveCatalog();
    }

    document.getElementById('langSelect').addEventListener('change', (e) => {
      const v = e.target.value;
      localStorage.setItem('tinysql-lang', v);
      setLanguage(v);
    });

    // initialize language from localStorage, browser or default to de
    const savedLang = localStorage.getItem('tinysql-lang');
    const browserLang = savedLang || ((navigator.language || 'de').startsWith('en') ? 'en' : 'de');
    document.getElementById('langSelect').value = browserLang;
    setLanguage(browserLang);

    // Notification helper (non-blocking)
    function showNotification(message, type = 'info', timeout = 6000) {
      const container = document.getElementById('toastContainer');
      if (!container) return;
      const note = document.createElement('div');
      note.className = 'notification ' + type;
      note.style.background = type === 'error' ? '#fee2e2' : type === 'success' ? '#ecfccb' : '#eff6ff';
      note.style.border = '1px solid rgba(0,0,0,0.06)';
      note.style.color = '#111827';
      note.style.padding = '0.6rem 0.9rem';
      note.style.marginBottom = '0.5rem';
      note.style.borderRadius = '6px';
      note.style.boxShadow = '0 4px 12px rgba(0,0,0,0.06)';
      note.textContent = message;
      container.appendChild(note);
      setTimeout(() => {
        note.style.opacity = '0';
        setTimeout(() => container.removeChild(note), 400);
      }, timeout);
    }

    // ----- Schema / Explain handlers -----
    document.getElementById('btnListTables').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      const res = await window.tinySQL.listTables();
      const parsed = typeof res === 'string' ? JSON.parse(res) : res;
      if (parsed.error) {
        showNotification(parsed.error, 'error');
        return;
      }
      const list = parsed.tables || [];
      const html = '<div class="function-card"><h3>Tables</h3><div>' + list.map(t => `<div>${t}</div>`).join('') + '</div></div>';
      document.getElementById('content').insertAdjacentHTML('afterbegin', html);
      // update schema browser
      renderSchemaList(list);
    });

    document.getElementById('btnDescribeTable').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      const tbl = document.getElementById('tblNameInput').value.trim();
      if (!tbl) return showNotification('Please provide a table name', 'error');
      const res = await window.tinySQL.describeTable(tbl);
      const parsed = typeof res === 'string' ? JSON.parse(res) : res;
      if (parsed.error) { showNotification(parsed.error, 'error'); return; }
      const cols = parsed.columns || [];
      let html = `<div class="function-card"><h3>Table: ${escapeHtml(parsed.table)}</h3><div>Rows: ${parsed.rows}</div><table style="margin-top:0.5rem;width:100%"><thead><tr><th>Name</th><th>Type</th><th>Primary</th></tr></thead><tbody>`;
      cols.forEach(c => html += `<tr><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.type)}</td><td>${c.primary ? 'YES' : ''}</td></tr>`);
      html += '</tbody></table></div>';
      document.getElementById('content').insertAdjacentHTML('afterbegin', html);
      // highlight in schema browser
      highlightSchemaTable(parsed.table);
    });

    document.getElementById('btnExplain').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      const sql = document.getElementById('explainSql').value.trim();
      if (!sql) return showNotification('Please provide SQL to explain', 'error');
      const res = await window.tinySQL.explain(sql);
      const parsed = typeof res === 'string' ? JSON.parse(res) : res;
      if (parsed.error) { showNotification(parsed.error, 'error'); return; }
      const plan = parsed.plan || [];
      let html = `<div class="function-card"><h3>Explain</h3><div class="explain-steps">`;
      plan.forEach((p, i) => {
        html += `<details class="explain-step"><summary>Step ${i+1}: ${escapeHtml(p.operation)} (${escapeHtml(p.object)})</summary><div style="padding:0.5rem 0;">Cost: ${escapeHtml(p.cost)}<br/>${escapeHtml(p.details)}</div></details>`;
      });
      html += '</div></div>';
      document.getElementById('content').insertAdjacentHTML('afterbegin', html);
    });

    // Transaction controls
    document.getElementById('btnBegin').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      try {
        const res = await window.tinySQL.exec('BEGIN;');
        const parsed = typeof res === 'string' ? JSON.parse(res) : res;
        if (parsed && parsed.error) throw new Error(parsed.error);
        showNotification('Transaction begun', 'success', 2500);
      } catch (err) {
        showNotification('BEGIN failed: ' + (err.message || err), 'error', 5000);
      }
    });

    document.getElementById('btnCommit').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      try {
        const res = await window.tinySQL.exec('COMMIT;');
        const parsed = typeof res === 'string' ? JSON.parse(res) : res;
        if (parsed && parsed.error) throw new Error(parsed.error);
        showNotification('Transaction committed', 'success', 2500);
      } catch (err) {
        showNotification('COMMIT failed: ' + (err.message || err), 'error', 5000);
      }
    });

    document.getElementById('btnRollback').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      try {
        const res = await window.tinySQL.exec('ROLLBACK;');
        const parsed = typeof res === 'string' ? JSON.parse(res) : res;
        if (parsed && parsed.error) throw new Error(parsed.error);
        showNotification('Transaction rolled back', 'success', 2500);
      } catch (err) {
        showNotification('ROLLBACK failed: ' + (err.message || err), 'error', 5000);
      }
    });

    // Create view - reuse modal editor for composing the CREATE VIEW statement
    document.getElementById('btnCreateView').addEventListener('click', () => {
      const template = `CREATE VIEW view_name AS\nSELECT * FROM <table> WHERE <condition>;`;
      openTryModal(template);
      // hint the user to use Execute Statement button
      modalResult.innerHTML = '<div style="color: #2563eb;">Edit the CREATE VIEW statement and click "Execute Statement".</div>';
    });

    // List views - try to query sqlite_master (fallback message if unsupported)
    document.getElementById('btnListViews').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      try {
        // common meta table for many SQL engines
        const res = await window.tinySQL.query(`SELECT name, sql FROM sqlite_master WHERE type='view';`);
        const parsed = typeof res === 'string' ? JSON.parse(res) : res;
        if (parsed.error) { showNotification(parsed.error, 'error'); return; }
        const html = '<div class="function-card"><h3>Views</h3>' + renderTable(parsed) + '</div>';
        document.getElementById('content').insertAdjacentHTML('afterbegin', html);
      } catch (err) {
        showNotification('Listing views failed: ' + (err.message || err), 'error');
      }
    });

    // Show meta: basic counts for tables and views and a quick tables list
    document.getElementById('btnShowMeta').addEventListener('click', async () => {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      try {
        // count tables
        const tablesRes = await window.tinySQL.query(`SELECT COUNT(*) as table_count FROM sqlite_master WHERE type='table';`);
        const tablesParsed = typeof tablesRes === 'string' ? JSON.parse(tablesRes) : tablesRes;
        const viewsRes = await window.tinySQL.query(`SELECT COUNT(*) as view_count FROM sqlite_master WHERE type='view';`);
        const viewsParsed = typeof viewsRes === 'string' ? JSON.parse(viewsRes) : viewsRes;

        const listRes = await window.tinySQL.query(`SELECT name FROM sqlite_master WHERE type='table' LIMIT 100;`);
        const listParsed = typeof listRes === 'string' ? JSON.parse(listRes) : listRes;

        let html = `<div class="function-card"><h3>Metadata</h3><div style="display:flex;gap:1rem;flex-wrap:wrap;">`;
        html += `<div>Tables: ${tablesParsed.rows && tablesParsed.rows[0] ? tablesParsed.rows[0][0] : '0'}</div><div>Views: ${viewsParsed.rows && viewsParsed.rows[0] ? viewsParsed.rows[0][0] : '0'}</div></div>`;
        if (listParsed && listParsed.rows && listParsed.rows.length > 0) {
          html += '<div style="margin-top:0.5rem;"><strong>Tables:</strong>' + listParsed.rows.map(r => `<div>${escapeHtml(r[0])}</div>`).join('') + '</div>';
        }
        html += '</div>';
        document.getElementById('content').insertAdjacentHTML('afterbegin', html);
      } catch (err) {
        showNotification('Show meta failed: ' + (err.message || err), 'error');
      }
    });

    // Render functions
    function renderContent() {
      const content = document.getElementById('content');
      let html = '';

      for (const [categoryId, categoryData] of Object.entries(functionData)) {
        html += `<section class="category" id="${categoryId}">`;
        html += `<h2 class="category-title">${categoryData.title}</h2>`;

        for (const func of categoryData.functions) {
          html += `<div class="function-card">`;
          html += `<h3 class="function-name">${func.name}</h3>`;
          html += `<p class="function-description">${func.description}</p>`;
          html += `<div class="function-syntax">${func.syntax}</div>`;

          if (func.examples && func.examples.length > 0) {
            html += `<div class="examples-section">`;
            html += `<div class="example-title">📖 Beispiele:</div>`;

            func.examples.forEach((example, idx) => {
              html += `<div class="example-item">`;
              html += `<div class="example-code">${escapeHtml(example)}</div>`;
              html += `<div class="example-toolbar"><button class="try-button" data-sql="${escapeHtml(example)}">▶ Try It</button><button class="copy-button" data-sql="${escapeHtml(example)}">Copy</button></div>`;
              html += `</div>`;
            });

            html += `</div>`;
          }

          html += `</div>`;
        }

        html += `</section>`;
      }

      content.innerHTML = html;

      // Attach event listeners to try and copy buttons
      document.querySelectorAll('.try-button').forEach(btn => {
        btn.addEventListener('click', (e) => {
          const sql = e.currentTarget.getAttribute('data-sql');
          openTryModal(sql);
        });
      });
      document.querySelectorAll('.copy-button').forEach(btn => {
        btn.addEventListener('click', (e) => {
          const sql = e.currentTarget.getAttribute('data-sql');
          copyToClipboard(sql);
        });
      });
    }

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    // Modal functionality
    const modal = document.getElementById('tryModal');
    const modalEditor = document.getElementById('modalEditor');
    const modalResult = document.getElementById('modalResult');
    const closeBtn = document.getElementById('closeModal');
    const executeQueryBtn = document.getElementById('executeQuery');
    const executeExecBtn = document.getElementById('executeExec');

    function openTryModal(sql) {
      modalEditor.value = sql;
      modalResult.innerHTML = 'Bereit zum Ausführen...';
      modal.classList.add('active');
    }

    closeBtn.addEventListener('click', () => {
      modal.classList.remove('active');
    });

    modal.addEventListener('click', (e) => {
      if (e.target === modal) {
        modal.classList.remove('active');
      }
    });

    executeQueryBtn.addEventListener('click', async () => {
      const sql = modalEditor.value.trim();
      if (!sql) {
        modalResult.innerHTML = '<div style="color: red;">Bitte SQL-Query eingeben.</div>';
        return;
      }

      console.log('[Reference] executeQuery - wasmReady:', wasmReady, 'dbConnected:', dbConnected);
      if (!wasmReady || !dbConnected) {
        modalResult.innerHTML = '<div style="color: red;">WASM nicht bereit. Bitte Seite neu laden.</div>';
        return;
      }

      try {
        const res = await window.tinySQL.query(sql);
        const result = typeof res === 'string' ? JSON.parse(res) : res;
        if (result.error) {
          modalResult.innerHTML = `<div style="color: red;">Error: ${escapeHtml(result.error)}</div>`;
        } else {
          modalResult.innerHTML = renderTable(result);
        }
      } catch (error) {
        modalResult.innerHTML = `<div style="color: red;">Error: ${escapeHtml(error.toString())}</div>`;
      }
    });

    executeExecBtn.addEventListener('click', async () => {
      const sqlRaw = modalEditor.value || '';
      const sql = sqlRaw.trim();
      if (!sql) {
        modalResult.innerHTML = '<div style="color: red;">Bitte SQL-Statement eingeben.</div>';
        return;
      }

      if (!wasmReady || !dbConnected) {
        modalResult.innerHTML = '<div style="color: red;">WASM nicht bereit. Bitte Seite neu laden.</div>';
        return;
      }

      // Split into statements by semicolon and execute sequentially.
      // Keep last SELECT result for rendering if present.
      const parts = sqlRaw.split(';').map(s => s.trim()).filter(s => s.length > 0);
      if (parts.length === 0) {
        modalResult.innerHTML = '<div style="color: red;">Keine gültigen Anweisungen gefunden.</div>';
        return;
      }

      modalResult.innerHTML = '<div>Executing...</div>';
      let lastQueryResult = null;
      try {
        for (const stmt of parts) {
          // choose query vs exec based on starting keyword
          const start = stmt.split(/\s+/)[0].toLowerCase();
          if (start === 'select' || start === 'with') {
            const r = await window.tinySQL.query(stmt + ';');
            const parsed = typeof r === 'string' ? JSON.parse(r) : r;
            if (parsed && parsed.error) throw new Error(parsed.error);
            lastQueryResult = parsed;
          } else {
            const r = await window.tinySQL.exec(stmt + ';');
            const parsed = typeof r === 'string' ? JSON.parse(r) : r;
            if (parsed && parsed.error) throw new Error(parsed.error);
          }
        }

        if (lastQueryResult) {
          modalResult.innerHTML = renderTable(lastQueryResult);
        } else {
          modalResult.innerHTML = '<div style="color: green;">✓ Statements erfolgreich ausgeführt!</div>';
        }
      } catch (error) {
        modalResult.innerHTML = `<div style="color: red;">Error: ${escapeHtml(error.message || String(error))}</div>`;
      }
    });

    // modal copy button
    document.getElementById('copyModal').addEventListener('click', () => {
      const text = modalEditor.value || '';
      copyToClipboard(text);
    });

    // keyboard shortcut: Cmd/Ctrl+Enter to execute query from modal
    modalEditor.addEventListener('keydown', (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        executeQueryBtn.click();
      }
    });

    function renderTable(result) {
      if (!result.columns || result.columns.length === 0) {
        return '<div>Keine Daten zurückgegeben.</div>';
      }

      let html = '<table class="result-table"><thead><tr>';
      for (const col of result.columns) {
        html += `<th>${escapeHtml(col)}</th>`;
      }
      html += '</tr></thead><tbody>';

      if (result.rows && result.rows.length > 0) {
        for (const row of result.rows) {
          html += '<tr>';
          for (let i = 0; i < result.columns.length; i++) {
            const cell = row[i];
            html += `<td>${formatValue(cell)}</td>`;
          }
          html += '</tr>';
        }
      }

      html += '</tbody></table>';
      return html;
    }

    function formatValue(val) {
      if (val === null || val === undefined) return 'NULL';
      if (typeof val === 'string') return escapeHtml(val);
      if (typeof val === 'object') return escapeHtml(JSON.stringify(val));
      return escapeHtml(String(val));
    }

    // Search functionality
    const searchInput = document.getElementById('searchInput');
    searchInput.addEventListener('input', (e) => {
      const query = e.target.value.toLowerCase();
      const cards = document.querySelectorAll('.function-card');

      cards.forEach(card => {
        const name = card.querySelector('.function-name')?.textContent.toLowerCase() || '';
        const desc = card.querySelector('.function-description')?.textContent.toLowerCase() || '';
        const catalogNames = Array.from(card.querySelectorAll('.catalog-function-name'))
          .some(item => item.textContent.toLowerCase().includes(query));

        if (name.includes(query) || desc.includes(query) || catalogNames) {
          card.style.display = 'block';
        } else {
          card.style.display = 'none';
        }
      });
    });

    // Navigation highlighting
    const navLinks = document.querySelectorAll('.nav-link');
    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const id = entry.target.id;
          navLinks.forEach(link => {
            if (link.getAttribute('href') === `#${id}`) {
              link.classList.add('active');
            } else {
              link.classList.remove('active');
            }
          });
        }
      });
    }, { rootMargin: '-50% 0px -50% 0px' });

    // Initialize
    initWasm();
    renderContent();

    // compute header offset and update CSS variable so sticky elements don't overlap
    (function adjustHeaderOffset(){
      const hdr = document.querySelector('header');
      if (!hdr) return;
      const h = hdr.offsetHeight || 120;
      // add a small gap
      const offset = h + 12;
      document.documentElement.style.setProperty('--header-offset', offset + 'px');
    })();

    // Load generated examples (if the generator has produced `function_examples.json`)
    let generatedExamples = null;

    function queryRowsAsObjects(result) {
      const columns = Array.isArray(result.columns) ? result.columns : [];
      const rows = Array.isArray(result.rows) ? result.rows : [];
      return rows.map(row => Object.fromEntries(columns.map((column, index) => [
        String(column).toLowerCase(), row[index]
      ])));
    }

    function liveCatalogTitle(functionCount, procedureCount, lang) {
      const map = i18n[lang] || i18n.de;
      return `${map.liveCatalog}: ${functionCount} ${map.functions} · ${procedureCount} ${map.proceduresCount}`;
    }

    function catalogTypeLabel(type, lang) {
      const map = i18n[lang] || i18n.de;
      return map[String(type).toLowerCase()] || type;
    }

    function appendCatalogGroup(container, title, description, names) {
      const details = document.createElement('details');
      details.className = 'function-card catalog-group';
      const summary = document.createElement('summary');
      summary.className = 'function-name';
      summary.textContent = `${title} (${names.length})`;
      details.appendChild(summary);

      const desc = document.createElement('p');
      desc.className = 'function-description';
      desc.textContent = description;
      details.appendChild(desc);

      if (names.length > 0) {
        const list = document.createElement('ul');
        list.className = 'catalog-function-list';
        names.forEach(name => {
          const item = document.createElement('li');
          item.className = 'catalog-function-name';
          item.textContent = name;
          list.appendChild(item);
        });
        details.appendChild(list);
      }
      container.appendChild(details);
    }

    function renderLiveCatalog() {
      if (!liveCatalogData) return;
      const content = document.getElementById('content');
      const lang = document.getElementById('langSelect').value;
      const map = i18n[lang] || i18n.de;
      const previous = document.getElementById('live-catalog');
      if (previous) previous.remove();

      const container = document.createElement('section');
      container.id = 'live-catalog';
      container.className = 'category';
      const heading = document.createElement('h2');
      heading.className = 'category-title';
      heading.textContent = liveCatalogTitle(liveCatalogData.functions.length, liveCatalogData.procedures.length, lang);
      container.appendChild(heading);
      const source = document.createElement('p');
      source.className = 'catalog-source';
      source.textContent = map.catalogSource;
      container.appendChild(source);

      const byType = new Map();
      liveCatalogData.functions.forEach(fn => {
        const type = String(fn.function_type || 'SCALAR').toUpperCase();
        if (!byType.has(type)) byType.set(type, []);
        byType.get(type).push(String(fn.name));
      });
      const typeOrder = ['SCALAR', 'AGGREGATE', 'WINDOW', 'TABLE'];
      const typeRank = type => {
        const index = typeOrder.indexOf(type);
        return index >= 0 ? index : typeOrder.length;
      };
      [...byType.keys()]
        .sort((a, b) => typeRank(a) - typeRank(b) || a.localeCompare(b))
        .forEach(type => {
          const names = byType.get(type).sort((a, b) => a.localeCompare(b));
          appendCatalogGroup(container, catalogTypeLabel(type, lang), `${type} · ${names.length}`, names);
        });

      appendCatalogGroup(
        container,
        map.procedures,
        map.procedureDescription,
        liveCatalogData.procedures.map(proc => String(proc.name)).sort((a, b) => a.localeCompare(b))
      );

      content.insertBefore(container, document.getElementById('generated-examples') || content.firstChild);
      observer.observe(container);
    }

    async function loadLiveCatalog() {
      try {
        const [functionResponse, procedureResponse] = await Promise.all([
          window.tinySQL.query('SELECT name, function_type FROM catalog.functions ORDER BY function_type, name'),
          window.tinySQL.query('SELECT name FROM sys.procedures ORDER BY name')
        ]);
        const functionResult = typeof functionResponse === 'string' ? JSON.parse(functionResponse) : functionResponse;
        const procedureResult = typeof procedureResponse === 'string' ? JSON.parse(procedureResponse) : procedureResponse;
        if (functionResult.error) throw new Error(functionResult.error);
        if (procedureResult.error) throw new Error(procedureResult.error);

        liveCatalogData = {
          functions: queryRowsAsObjects(functionResult),
          procedures: queryRowsAsObjects(procedureResult)
        };
        renderLiveCatalog();
      } catch (err) {
        console.warn('[Reference] Live catalog unavailable', err);
      }
    }

    async function loadGeneratedExamples() {
      try {
        const res = await fetch('function_examples.json');
        if (!res.ok) return;
        const data = await res.json();
        if (!Array.isArray(data) || data.length === 0) return;
        generatedExamples = data;
        const container = document.createElement('section');
        container.id = 'generated-examples';
        container.className = 'category';
        const h = document.createElement('h2');
        h.className = 'category-title';
        h.dataset.generatedOverview = 'true';
        h.textContent = i18n[document.getElementById('langSelect').value].generatedOverview;
        container.appendChild(h);

        data.forEach(sec => {
          const details = document.createElement('details');
          details.className = 'function-card';
          details.open = false;
          const summary = document.createElement('summary');
          summary.className = 'function-name';
          const count = Array.isArray(sec.examples) ? sec.examples.length : 0;
          summary.dataset.section = sec.section;
          summary.dataset.exampleCount = String(count);
          summary.textContent = generatedExampleLabel(sec.section, count, document.getElementById('langSelect').value);
          details.appendChild(summary);

          if (Array.isArray(sec.examples)) {
            sec.examples.forEach(ex => {
              const item = document.createElement('div');
              item.className = 'example-item';
              const pre = document.createElement('pre');
              pre.className = 'example-code';
              pre.textContent = ex;
              const toolbar = document.createElement('div');
              toolbar.className = 'example-toolbar';
              const tryBtn = document.createElement('button');
              tryBtn.className = 'try-button';
              tryBtn.setAttribute('data-sql', ex);
              tryBtn.textContent = '▶ Try It';
              tryBtn.addEventListener('click', () => openTryModal(ex));
              const copyBtn = document.createElement('button');
              copyBtn.className = 'copy-button';
              copyBtn.setAttribute('data-sql', ex);
              copyBtn.textContent = 'Copy';
              copyBtn.addEventListener('click', () => copyToClipboard(ex));
              toolbar.appendChild(tryBtn);
              toolbar.appendChild(copyBtn);
              item.appendChild(pre);
              item.appendChild(toolbar);
              details.appendChild(item);
            });
          }

          container.appendChild(details);
        });

        const content = document.getElementById('content');
        // Keep the live catalog first; the generated SQL examples follow it.
        const liveCatalog = document.getElementById('live-catalog');
        if (liveCatalog) {
          content.insertBefore(container, liveCatalog.nextSibling);
        } else {
          content.prepend(container);
        }
      } catch (err) {
        console.warn('[Reference] No generated examples available', err);
      }
    }

    // attempt to load generated examples asynchronously
    loadGeneratedExamples();

    // Demo loader: run CREATE/INSERT statements from generated examples
    async function loadDemoTables() {
      if (!wasmReady || !dbConnected) return showNotification(i18n[document.getElementById('langSelect').value].wasmNotReady, 'error');
      try {
        let data = generatedExamples;
        if (!data) {
          const res = await fetch('function_examples.json');
          if (!res.ok) return showNotification('No demo examples available', 'error');
          data = await res.json();
        }

        // collect statements that look like CREATE or INSERT
        const stmts = [];
        data.forEach(sec => {
          const name = (sec.section || '').toLowerCase();
          if (name.includes('create sample') || name.includes('window') || name.includes('demo')) {
            (sec.examples || []).forEach(ex => {
              // split into individual statements by semicolon
              ex.split(';').forEach(s => {
                const t = s.trim();
                if (!t) return;
                // only keep create/insert statements
                if (/^create\s+/i.test(t) || /^insert\s+/i.test(t)) {
                  stmts.push(t + ';');
                }
              });
            });
          }
        });

        if (stmts.length === 0) return showNotification('No demo CREATE/INSERT statements found', 'error');

        // execute within a transaction
        await window.tinySQL.exec('BEGIN;');
        for (const s of stmts) {
          const r = await window.tinySQL.exec(s);
          const parsed = typeof r === 'string' ? JSON.parse(r) : r;
          if (parsed && parsed.error) throw new Error(parsed.error);
        }
        await window.tinySQL.exec('COMMIT;');
        showNotification('Demo tables loaded', 'success', 4000);
        // refresh schema browser if present
        if (typeof renderSchemaList === 'function') {
          const res = await window.tinySQL.listTables();
          const parsed = typeof res === 'string' ? JSON.parse(res) : res;
          renderSchemaList(parsed.tables || []);
        }
      } catch (err) {
        try { await window.tinySQL.exec('ROLLBACK;'); } catch(e){}
        showNotification('Failed to load demo: ' + (err.message || err), 'error', 8000);
      }
    }

    document.getElementById('btnLoadDemo').addEventListener('click', () => {
      loadDemoTables();
    });

    // Copy helper for examples
    async function copyToClipboard(text) {
      try {
        await navigator.clipboard.writeText(text);
        showNotification('Copied SQL to clipboard', 'success', 2500);
      } catch (err) {
        showNotification('Copy failed', 'error', 4000);
      }
    }

    // Observe sections for navigation
    setTimeout(() => {
      document.querySelectorAll('.category').forEach(section => {
        observer.observe(section);
      });
    }, 100);

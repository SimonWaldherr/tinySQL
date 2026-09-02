// AccessWeb – client-side helpers.
//
// Everything lives in this file rather than in inline <script> blocks and
// onclick="" attributes: the app sends a Content-Security-Policy without
// 'unsafe-inline', so inline code never executed and the SQL editor, the
// column builder and the drop/delete confirmations were all dead. Behaviour is
// wired up here through data-action attributes and event delegation instead.

(function () {
  'use strict';

  // ── Sidebar ──────────────────────────────────────────────────────────────
  // Highlight the active table link based on the current URL path.
  var path = window.location.pathname;
  document.querySelectorAll('.sidebar .table-link').forEach(function (a) {
    if (a.getAttribute('href') === path) {
      a.classList.add('active');
    }
  });

  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function byId(id) {
    return document.getElementById(id);
  }

  // ── SQL editor (query page) ──────────────────────────────────────────────

  function renderError(msg) {
    byId('statusMsg').textContent = '';
    byId('resultArea').innerHTML =
      '<div class="alert alert-danger mt-1 py-2"><i class="bi bi-exclamation-triangle-fill me-2"></i>' +
      escHtml(msg) + '</div>';
  }

  function renderResult(data) {
    var status = byId('statusMsg');
    var area = byId('resultArea');

    if (data.error) {
      status.textContent = '';
      area.innerHTML = '<div class="alert alert-danger mt-1 py-2"><i class="bi bi-exclamation-triangle-fill me-2"></i>' +
        escHtml(data.error) + '</div>';
      return;
    }

    var ms = data.elapsed_ms || 0;
    if (data.columns && data.columns.length > 0) {
      var rows = data.rows || [];
      status.textContent = rows.length + ' row(s) · ' + ms + ' ms';
      var html = '<div class="card mt-1"><div class="table-responsive">' +
        '<table class="table table-sm table-hover result-table mb-0"><thead><tr>';
      data.columns.forEach(function (c) { html += '<th>' + escHtml(c) + '</th>'; });
      html += '</tr></thead><tbody>';
      rows.forEach(function (row) {
        html += '<tr>';
        row.forEach(function (cell) {
          html += '<td>' + (cell === '' ? '<span class="null-cell">null</span>' : escHtml(cell)) + '</td>';
        });
        html += '</tr>';
      });
      html += '</tbody></table></div></div>';
      area.innerHTML = html;
    } else {
      status.textContent = '';
      area.innerHTML = '<div class="alert alert-success mt-1 py-2"><i class="bi bi-check-circle-fill me-2"></i>' +
        'OK — ' + (data.affected || 0) + ' row(s) affected · ' + ms + ' ms</div>';
    }
  }

  function runQuery() {
    var input = byId('sqlInput');
    var sql = input ? input.value.trim() : '';
    if (!sql) return;
    var btn = byId('runBtn');
    byId('statusMsg').textContent = 'Running…';
    btn.disabled = true;

    fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: sql })
    })
      .then(function (r) { return r.json(); })
      .then(function (data) { btn.disabled = false; renderResult(data); })
      .catch(function (err) { btn.disabled = false; renderError(err.toString()); });
  }

  function clearEditor() {
    byId('sqlInput').value = '';
    byId('resultArea').innerHTML = '';
    byId('statusMsg').textContent = '';
    byId('sqlInput').focus();
  }

  function exportQuery(format) {
    var sql = byId('sqlInput').value.trim();
    if (!sql) return;
    var status = byId('statusMsg');
    status.textContent = 'Exporting…';

    fetch('/api/export', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: sql, format: format })
    })
      .then(function (r) {
        if (!r.ok) {
          return r.text().then(function (text) { throw new Error(text || r.statusText); });
        }
        return r.blob();
      })
      .then(function (blob) {
        status.textContent = '';
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = 'query.' + format;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
      })
      .catch(function (err) {
        status.textContent = '';
        renderError(err.message || err.toString());
      });
  }

  var sqlInput = byId('sqlInput');
  if (sqlInput) {
    sqlInput.addEventListener('keydown', function (e) {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        runQuery();
      }
    });
  }

  // ── Column builder (create-table page) ───────────────────────────────────

  function addField() {
    var list = byId('fieldList');
    var row = document.createElement('div');
    row.className = 'field-row mb-2';
    row.innerHTML =
      '<input type="text" class="form-control form-control-sm" name="col_name" placeholder="column name" required>' +
      '<select class="form-select form-select-sm type-sel" name="col_type">' +
      '<option value="TEXT" selected>TEXT</option><option value="INT">INT</option>' +
      '<option value="FLOAT">FLOAT</option><option value="BOOL">BOOL</option>' +
      '</select>' +
      '<button type="button" class="btn btn-outline-danger btn-sm btn-remove" data-action="remove-field" title="Remove column">' +
      '<i class="bi bi-dash"></i></button>';
    list.appendChild(row);
    row.querySelector('input').focus();
  }

  function removeField(btn) {
    // Keep at least one column row so the form stays submittable.
    if (document.querySelectorAll('#fieldList .field-row').length <= 1) return;
    btn.closest('.field-row').remove();
  }

  // ── Delegated actions ────────────────────────────────────────────────────

  document.addEventListener('click', function (e) {
    var el = e.target.closest('[data-action]');
    if (!el) return;

    switch (el.dataset.action) {
      case 'run-query':
        runQuery();
        break;
      case 'clear-editor':
        clearEditor();
        break;
      case 'export-query':
        exportQuery(el.dataset.format);
        break;
      case 'add-field':
        addField();
        break;
      case 'remove-field':
        removeField(el);
        break;
      case 'drop-table':
        if (window.confirm('Drop table "' + el.dataset.table + '"? This cannot be undone.')) {
          byId('dropForm').submit();
        }
        break;
    }
  });

  // Forms carrying data-confirm ask before they submit.
  document.addEventListener('submit', function (e) {
    var message = e.target.dataset ? e.target.dataset.confirm : null;
    if (message && !window.confirm(message)) {
      e.preventDefault();
    }
  });
})();

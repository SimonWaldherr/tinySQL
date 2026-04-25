(() => {
  'use strict';

  /* ── Bootstrap form validation ─────────────────────────────────── */
  for (const form of document.querySelectorAll('.needs-validation')) {
    form.addEventListener('submit', event => {
      if (!form.checkValidity()) {
        event.preventDefault();
        event.stopPropagation();
      }
      form.classList.add('was-validated');
    }, false);
  }

  /* ── Admin quick-login button ───────────────────────────────────── */
  const adminBtn = document.getElementById('btn-admin-login');
  if (adminBtn) {
    adminBtn.addEventListener('click', () => {
      const u = adminBtn.dataset.adminUser;
      const p = adminBtn.dataset.adminPass;
      const userField = document.getElementById('username');
      const passField = document.getElementById('password');
      if (userField && passField) {
        userField.value = u;
        passField.value = p;
        userField.closest('form').requestSubmit();
      }
    });
  }

  /* ── Form builder ───────────────────────────────────────────────── */
  const list      = document.getElementById('field-list');
  const addBtn    = document.getElementById('btn-add-field');
  const countBadge = document.getElementById('field-count');
  const emptyHint = document.getElementById('empty-hint');
  const tmpl      = document.getElementById('field-card-template');
  if (!list || !addBtn || !tmpl) return;

  let fieldIndex = 0;

  const TYPE_ICONS = {
    text:       'bi-fonts',
    textarea:   'bi-textarea-t',
    select:     'bi-menu-button',
    checkboxes: 'bi-check2-square',
    date:       'bi-calendar3',
    number:     'bi-123',
    email:      'bi-envelope',
    password:   'bi-key',
  };

  function updateCount() {
    const n = list.querySelectorAll('.field-card').length;
    countBadge.textContent = n;
    emptyHint.classList.toggle('d-none', n > 0);
  }

  function renumber() {
    list.querySelectorAll('.field-card').forEach((card, i) => {
      card.querySelector('.field-num').textContent = i + 1;
      // keep required checkbox value in sync with visual position
      const reqCb = card.querySelector('input[type=checkbox][name=field_required]');
      if (reqCb) reqCb.value = i;
    });
    updateCount();
  }

  function applyTypeChange(card, value) {
    const icon = card.querySelector('.field-type-icon i');
    const optionsRow = card.querySelector('.field-options-row');
    const defaultCol = card.querySelector('.field-default-col');
    const hasOptions = value === 'select' || value === 'checkboxes';

    icon.className = `bi ${TYPE_ICONS[value] || 'bi-fonts'}`;
    optionsRow.classList.toggle('d-none', !hasOptions);

    // hide default value for select/checkboxes (less useful)
    defaultCol.classList.toggle('d-none', hasOptions);
  }

  function addField() {
    const clone = tmpl.content.cloneNode(true);
    const card  = clone.querySelector('.field-card');
    const idx   = fieldIndex++;

    // fix the for/id pair on the required switch
    const reqCb    = card.querySelector('input[type=checkbox][name=field_required]');
    const reqLabel = card.querySelector('label[for^="req-"]');
    const newId    = `req-${idx}`;
    reqCb.id       = newId;
    reqCb.value    = idx;
    if (reqLabel) reqLabel.setAttribute('for', newId);

    // type change → update icon + toggle options row
    const typeSelect = card.querySelector('.field-type-select');
    typeSelect.addEventListener('change', () => applyTypeChange(card, typeSelect.value));

    // remove button
    card.querySelector('.btn-remove-field').addEventListener('click', () => {
      card.remove();
      renumber();
    });

    list.appendChild(clone);
    renumber();
    card.querySelector('.field-label').focus();
  }

  addBtn.addEventListener('click', addField);

  // start with one field already present
  addField();

  /* ── Drag-to-reorder (optional, progressive enhancement) ───────── */
  let dragged = null;
  list.addEventListener('dragstart', e => {
    dragged = e.target.closest('.field-card');
    if (dragged) { dragged.style.opacity = '.4'; e.dataTransfer.effectAllowed = 'move'; }
  });
  list.addEventListener('dragend', () => {
    if (dragged) { dragged.style.opacity = ''; dragged = null; }
    renumber();
  });
  list.addEventListener('dragover', e => {
    e.preventDefault();
    const over = e.target.closest('.field-card');
    if (over && dragged && over !== dragged) {
      const rect = over.getBoundingClientRect();
      const after = e.clientY > rect.top + rect.height / 2;
      list.insertBefore(dragged, after ? over.nextSibling : over);
    }
  });
  // make cards draggable after they are added
  new MutationObserver(() => {
    list.querySelectorAll('.field-card:not([draggable])').forEach(c => c.setAttribute('draggable', 'true'));
  }).observe(list, { childList: true });
})();

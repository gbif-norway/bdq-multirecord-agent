import { CONFIG } from './config.js';
import { AGG, TG2, TOPS, FALLBACK_TOPS, getClassColor } from './data-structures.js';
import { $, fmtInt, byValDesc, escapeHtml, truncate } from './utils.js';

// UI rendering functions
export function renderKPIs() {
  $('#kpiRecords').textContent = fmtInt(AGG.recordIds.size);
  $('#kpiTests').textContent = fmtInt(AGG.testIds.size);
  $('#kpiTotal').textContent = fmtInt(AGG.totals.rows);
  $('#kpiAmended').textContent = fmtInt(AGG.totals.amended);
  
  // Calculate validation percentage: COMPLIANT / (NOT_COMPLIANT + COMPLIANT)
  const validationTotal = AGG.totals.validationCompliant + AGG.totals.validationNotCompliant;
  const validationRate = validationTotal ? Math.round(1000 * (AGG.totals.validationCompliant / validationTotal)) / 10 : 0;
  $('#kpiValidated').textContent = validationTotal ? `${validationRate}%` : '–';
  
  $('#kpiIssues').textContent = fmtInt(AGG.totals.potentialIssues);
}

export function renderIEChips(value, title) {
  const items = (value || '').split(/[,;]+/).map(s => s.trim()).filter(Boolean).slice(0, 3);
  if (!items.length) return '';
  return `<span class="pill">${title}</span> ` + items.map(v => `<span>${escapeHtml(v)}</span>`).join(' ');
}

export function renderTopValues(label) {
  const byField = TOPS.get(label) || FALLBACK_TOPS.get(label);
  if (!byField) return '';
  const parts = [];
  for (const [field, byVal] of byField.entries()) {
    // top 3
    const arr = Array.from(byVal.entries()).map(([value, count]) => ({ value, count }))
      .sort((a, b) => b.count - a.count).slice(0, CONFIG.MAX_TOP_VALUES_DISPLAY);
    if (!arr.length) continue;
    const vals = arr.map(x => `<span class="val">${escapeHtml(x.value)} (${fmtInt(x.count)})</span>`).join(' ');
    parts.push(`<div><span class="field">${escapeHtml(field)}:</span> ${vals}</div>`);
  }
  return parts.length ? `<div class="tops">${parts.join('')}</div>` : '';
}

export function listNeedsAttention() {
  // Build arrays of tests with counts
  const validations = [];
  const amendments = [];
  const issues = [];
  
  for (const [label, per] of AGG.perTest.entries()) {
    const tg2 = TG2.byLabel.get(label) || {};
    const nice = tg2['prefLabel'] || label;
    const acted = (tg2['InformationElement:ActedUpon'] || '').trim();
    const consulted = (tg2['InformationElement:Consulted'] || '').trim();
    const desc = (tg2['Description'] || '').trim();
    const obj = { label, nice, desc, acted, consulted, type: per.type, counts: per.counts, tg2 };
    const c = per.counts || {};
    
    if (per.type === 'Validation' && (c['NOT_COMPLIANT'] || 0) > 0) validations.push({ ...obj, value: c['NOT_COMPLIANT'] });
    if (per.type === 'Amendment' && (c['AMENDED'] || 0) > 0) amendments.push({ ...obj, value: c['AMENDED'] });
    if (per.type === 'Issue' && (c['POTENTIAL_ISSUE'] || 0) > 0) issues.push({ ...obj, value: c['POTENTIAL_ISSUE'] });
  }
  
  validations.sort(byValDesc); 
  amendments.sort(byValDesc); 
  issues.sort(byValDesc);
  
  $('#countValidations').textContent = fmtInt(validations.length);
  $('#countAmendments').textContent = fmtInt(amendments.length);
  $('#countIssues').textContent = fmtInt(issues.length);

  function mkItem(entry) {
    const el = document.createElement('div');
    el.className = 'list-item color-left';
    const cls = (entry.tg2 && entry.tg2['IE Class']) ? entry.tg2['IE Class'] : 'Unknown';
    const color = getClassColor(cls);
    el.style.borderLeftColor = color;
    el.innerHTML = `
      <div class="item-title">
        <div><strong>${escapeHtml(entry.nice)}</strong> <span class="muted">(${escapeHtml(entry.label)})</span></div>
        <span class="badge">${fmtInt(entry.value)}</span>
      </div>
      <div class="muted" style="margin-top:6px;">${escapeHtml(truncate(entry.desc, CONFIG.MAX_DESCRIPTION_LENGTH))}</div>
      <div class="chips" style="margin-top:8px;">
        ${renderIEChips(entry.acted, 'Acted upon')}
        ${renderIEChips(entry.consulted, 'Consulted')}
      </div>
      ${renderTopValues(entry.label)}
    `;
    el.addEventListener('click', () => showTestDetails(entry.label));
    return el;
  }
  
  const lv = $('#listValidations'); 
  lv.innerHTML = ''; 
  validations.forEach(v => lv.appendChild(mkItem(v)));
  
  const la = $('#listAmendments');  
  la.innerHTML = ''; 
  amendments.forEach(v => la.appendChild(mkItem(v)));
  
  const li = $('#listIssues');      
  li.innerHTML = ''; 
  issues.forEach(v => li.appendChild(mkItem(v)));
}

// Modal functions
export function openModalContent(title, html) {
  $('#modalTitle').textContent = title;
  $('#modalContent').innerHTML = html;
  $('#modal').classList.add('show');
}

export function closeModal() {
  $('#modal').classList.remove('show');
}

export function showTestDetails(label) {
  const per = AGG.perTest.get(label) || { counts: {} };
  const tg2 = TG2.byLabel.get(label) || {};
  const nice = tg2['prefLabel'] || label;
  const desc = tg2['Description'] || '';
  const notes = tg2['Notes'] || '';
  const acted = tg2['InformationElement:ActedUpon'] || '';
  const consulted = tg2['InformationElement:Consulted'] || '';

  const lines = [];
  const keys = ['COMPLIANT', 'NOT_COMPLIANT', 'NOT_AMENDED', 'AMENDED', 'NOT_ISSUE', 'POTENTIAL_ISSUE', 'EXTERNAL_PREREQUISITES_NOT_MET', 'INTERNAL_PREREQUISITES_NOT_MET'];
  for (const k of keys) if ((per.counts[k] || 0) > 0) lines.push(`<span class="count-pill"><strong>${k}:</strong> ${fmtInt(per.counts[k])}</span>`);
  const countsHtml = lines.length ? `<div class="counts">${lines.join('')}</div>` : '<div class="muted">No results counted.</div>';
  
  // top values (up to 5) per field
  let topsHtml = '';
  const byField = TOPS.get(label) || FALLBACK_TOPS.get(label);
  if (byField) {
    const blocks = [];
    for (const [field, byVal] of byField.entries()) {
      const arr = Array.from(byVal.entries()).map(([value, count]) => ({ value, count }))
        .sort((a, b) => b.count - a.count).slice(0, CONFIG.MAX_TOP_VALUES);
      if (!arr.length) continue;
      const vals = arr.map(x => `<span class="val">${escapeHtml(x.value)} (${fmtInt(x.count)})</span>`).join(' ');
      blocks.push(`<div style="margin-top:6px;"><span class="field">${escapeHtml(field)}:</span> ${vals}</div>`);
    }
    if (blocks.length) topsHtml = `<div class="section-block">${blocks.join('')}</div>`;
  }

  const actedList = (acted || '').split(/[,;]+/).map(s => s.trim()).filter(Boolean);
  const colsLine = actedList.length ? `Columns: [${actedList.join(', ')}]` : '';
  const classPill = (tg2['IE Class']) ? `<span class="pill-soft" style="background:${getClassColor(tg2['IE Class'])}">${escapeHtml(tg2['IE Class'])}</span>` : '';
  const msg = `
    <div>
      <h3 class="desc" style="font-size:18px; font-weight:600; margin:0 0 6px 0;">${escapeHtml(desc)}</h3>
      ${notes ? `<p class="desc" style="margin:6px 0 10px 0;">${escapeHtml(notes)}</p>` : ''}
      ${colsLine ? `<div style="margin:6px 0;">${escapeHtml(colsLine)}</div>` : ''}
      ${classPill ? `<div style="margin:6px 0 12px 0;">${classPill}</div>` : ''}
      ${countsHtml}
      ${topsHtml || '<div class="muted" style="margin-top:8px;">No common values found.</div>'}
    </div>`;
  openModalContent(`${escapeHtml(nice)} ( ${escapeHtml(label)} )`, msg);
}

// Build test catalogue table with Tabulator
export function buildCatalogue() {
  const testsPresent = new Set(AGG.testIds);
  const rows = [];
  for (const [label, tg2] of TG2.byLabel.entries()) {
    if (!testsPresent.has(label)) continue; // only those we ran
    rows.push({
      label,
      prefLabel: tg2['prefLabel'] || '',
      ieClass: tg2['IE Class'] || '',
      acted: tg2['InformationElement:ActedUpon'] || '',
      consulted: tg2['InformationElement:Consulted'] || '',
      expected: tg2['ExpectedResponse'] || '',
      description: tg2['Description'] || '',
      notes: tg2['Notes'] || '',
      examples: tg2['Examples'] || '',
    });
  }
  const table = new Tabulator('#catalogue', {
    data: rows,
    layout: 'fitColumns',
    height: `${CONFIG.TABLE_HEIGHT}px`,
    pagination: true,
    paginationSize: CONFIG.PAGINATION_SIZE,
    movableColumns: true,
    placeholder: CONFIG.ERROR_MESSAGES.NO_TESTS_FOUND,
    columns: [
      { title: 'Label', field: 'label', width: 230, headerFilter: 'input' },
      { title: 'Name', field: 'prefLabel', widthGrow: 2, headerFilter: 'input' },
      { title: 'IE Class', field: 'ieClass', width: 180, headerFilter: 'input' },
      { title: 'Acted upon', field: 'acted', width: 200 },
      { title: 'Consulted', field: 'consulted', width: 200 },
      { title: 'Expected response', field: 'expected', widthGrow: 2 },
      { title: 'Description', field: 'description', widthGrow: 3 },
      { title: 'Notes', field: 'notes', widthGrow: 2 },
    ],
    rowClick: (e, row) => showTestDetails(row.getData().label),
  });
  return table;
}

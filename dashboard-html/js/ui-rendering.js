import { CONFIG } from './config.js';
import { AGG, TG2, TOPS, FALLBACK_TOPS, AMENDMENTS, TOP_GROUPED, getClassColor } from './data-structures.js';
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

export function renderAmendments() {
  const amendments = [];
  
  // Get all amendment tests from AGG.perTest that have actual amendments
  for (const [label, per] of AGG.perTest.entries()) {
    if (per.type !== 'Amendment') continue;
    
    const amendmentData = AMENDMENTS.get(label) || { original: new Map(), amended: new Map(), total: 0 };
    
    // Only show tests that have actual amendments (AMENDED only)
    const hasAmendments = (per.counts && (per.counts['AMENDED'] || 0) > 0);
    if (!hasAmendments) continue;
    
    const tg2 = TG2.byLabel.get(label) || {};
    const nice = tg2['prefLabel'] || label;
    const desc = (tg2['Description'] || '').trim();
    
    amendments.push({ 
      label, 
      nice, 
      desc, 
      total: amendmentData.total,
      original: amendmentData.original,
      amended: amendmentData.amended,
      tg2 
    });
  }
  
  amendments.sort((a, b) => b.total - a.total);
  
  function mkAmendmentItem(entry) {
    const el = document.createElement('div');
    el.className = 'list-item color-left';
    const cls = (entry.tg2 && entry.tg2['IE Class']) ? entry.tg2['IE Class'] : 'Unknown';
    const color = getClassColor(cls);
    el.style.borderLeftColor = color;
    
    // Create data affected table
    const tableRows = [];
    const allValues = new Set([...entry.original.keys(), ...entry.amended.keys()]);
    
    for (const value of allValues) {
      const originalCount = entry.original.get(value) || 0;
      const amendedCount = entry.amended.get(value) || 0;
      const totalCount = originalCount + amendedCount;
      
      if (totalCount > 0) {
        // Show original value, amended value (if different), and count
        const amendedValue = entry.amended.get(value) || value;
        const isAmended = amendedValue !== value;
        
        tableRows.push(`
          <tr>
            <td>${escapeHtml(value)}</td>
            <td>${isAmended ? escapeHtml(amendedValue) : '<span class="muted">No change</span>'}</td>
            <td>${fmtInt(totalCount)}</td>
          </tr>
        `);
      }
    }
    
    const dataTable = tableRows.length > 0 ? `
      <div style="margin-top: 12px;">
        <h4 style="margin: 0 0 8px 0; font-size: 14px; font-weight: 600;">Data affected</h4>
        <table class="amendment-table">
          <thead>
            <tr>
              <th>Original</th>
              <th>Amendment</th>
              <th>Number of rows affected</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows.join('')}
          </tbody>
        </table>
      </div>
    ` : '<div class="muted" style="margin-top: 12px;">No amendment data available.</div>';
    
    // Get total records tested for this amendment test
    const perTest = AGG.perTest.get(entry.label) || { counts: {} };
    const totalTested = Object.values(perTest.counts).reduce((sum, count) => sum + (count || 0), 0);
    
    el.innerHTML = `
      <div class="item-title">
        <div><strong>${escapeHtml(entry.nice)}</strong> <span class="muted">(${escapeHtml(entry.label)})</span></div>
        <span class="badge">${fmtInt(totalTested)}</span>
      </div>
      <div class="muted" style="margin-top:6px;">${escapeHtml(truncate(entry.desc, CONFIG.MAX_DESCRIPTION_LENGTH))}</div>
      ${dataTable}
    `;
    el.addEventListener('click', () => showTestDetails(entry.label));
    return el;
  }
  
  const la = $('#listAmendments');  
  la.innerHTML = ''; 
  
  if (amendments.length === 0) {
    la.innerHTML = '<div class="muted" style="text-align: center; padding: 20px;">None of the BDQ Tests generated any auto amendments which could be applied to your dataset.</div>';
  } else {
    amendments.forEach(v => la.appendChild(mkAmendmentItem(v)));
  }
}

export function renderValidations() {
  const validations = [];
  
  // Get all validation tests from AGG.perTest that have NOT_COMPLIANT results
  for (const [label, per] of AGG.perTest.entries()) {
    if (per.type !== 'Validation') continue;
    
    // Only show tests that have NOT_COMPLIANT results
    const hasNotCompliant = (per.counts && (per.counts['NOT_COMPLIANT'] || 0) > 0);
    if (!hasNotCompliant) continue;
    
    const tg2 = TG2.byLabel.get(label) || {};
    const nice = tg2['prefLabel'] || label;
    const desc = (tg2['Description'] || '').trim();
    
    // Get top grouped data for this validation test
    const topGroupedData = [];
    for (const [groupKey, count] of TOP_GROUPED.validations.entries()) {
      if (groupKey.endsWith(`|${label}`)) {
        const [actedUpon, consulted] = groupKey.split('|');
        topGroupedData.push({ actedUpon, consulted, count });
      }
    }
    topGroupedData.sort((a, b) => b.count - a.count);
    
    validations.push({ 
      label, 
      nice, 
      desc, 
      notCompliantCount: per.counts['NOT_COMPLIANT'] || 0,
      topGroupedData,
      tg2 
    });
  }
  
  validations.sort((a, b) => b.notCompliantCount - a.notCompliantCount);
  
  function mkValidationItem(entry) {
    const el = document.createElement('div');
    el.className = 'list-item color-left';
    const cls = (entry.tg2 && entry.tg2['IE Class']) ? entry.tg2['IE Class'] : 'Unknown';
    const color = getClassColor(cls);
    el.style.borderLeftColor = color;
    
    // Create top grouped data display
    const topGroupedHtml = entry.topGroupedData.length > 0 ? `
      <div style="margin-top: 12px;">
        <h4 style="margin: 0 0 8px 0; font-size: 14px; font-weight: 600;">Most common issues:</h4>
        <div style="font-size: 12px;">
          ${entry.topGroupedData.slice(0, 5).map(item => 
            `<div style="margin: 4px 0;">
              <span class="badge" style="font-size: 11px;">${fmtInt(item.count)}</span>
              ${escapeHtml(item.actedUpon)} ${item.consulted ? `(${escapeHtml(item.consulted)})` : ''}
            </div>`
          ).join('')}
        </div>
      </div>
    ` : '';
    
    el.innerHTML = `
      <div class="item-title">
        <div><strong>${escapeHtml(entry.nice)}</strong> <span class="muted">(${escapeHtml(entry.label)})</span></div>
        <span class="badge">${fmtInt(entry.notCompliantCount)}</span>
      </div>
      <div class="muted" style="margin-top:6px;">${escapeHtml(truncate(entry.desc, CONFIG.MAX_DESCRIPTION_LENGTH))}</div>
      ${topGroupedHtml}
    `;
    el.addEventListener('click', () => showTestDetails(entry.label));
    return el;
  }
  
  const lv = $('#listValidations');  
  lv.innerHTML = ''; 
  
  if (validations.length === 0) {
    lv.innerHTML = '<div class="muted" style="text-align: center; padding: 20px;">All validation tests passed successfully.</div>';
  } else {
    validations.forEach(v => lv.appendChild(mkValidationItem(v)));
  }
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


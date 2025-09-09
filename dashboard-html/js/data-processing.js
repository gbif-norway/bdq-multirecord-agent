import { CONFIG } from './config.js';
import { TG2, AGG, IE_CLASS_COUNTS, ATTENTION, TOPS, FALLBACK_TOPS, getPerTest, addAttention, getIeClassCounts } from './data-structures.js';
import { normalizeId, normalizeValue, inc } from './utils.js';
import { withErrorHandling, validateUrl, AppError } from './error-handling.js';

// Data processing functions
export function passByType(testType, outcome) {
  if (outcome === 'EXTERNAL_PREREQUISITES_NOT_MET' || outcome === 'INTERNAL_PREREQUISITES_NOT_MET') return false;
  if (testType === 'Validation') return outcome === 'COMPLIANT';
  if (testType === 'Amendment') return outcome === 'NOT_AMENDED';
  if (testType === 'Issue') return outcome === 'NOT_ISSUE';
  // If unknown type, conservatively treat the canonical pass words
  return ['COMPLIANT', 'NOT_AMENDED', 'NOT_ISSUE'].includes(outcome);
}

export function needsAttentionBucket(testType, outcome) {
  if (testType === 'Validation' && outcome === 'NOT_COMPLIANT') return 'NOT_COMPLIANT';
  if (testType === 'Amendment' && outcome === 'AMENDED') return 'AMENDED';
  if (testType === 'Issue' && outcome === 'POTENTIAL_ISSUE') return 'POTENTIAL_ISSUE';
  return null;
}

export function getOutcome(row) {
  let out = (row['result'] || '').trim();
  if (!CONFIG.CANONICAL_OUTCOMES.includes(out)) {
    const s = (row['status'] || '').trim();
    if (CONFIG.CANONICAL_OUTCOMES.includes(s)) out = s;
  }
  return out;
}

export function detectRecordIdHeader(headers) {
  const lc = headers.map(h => h.toLowerCase());
  // Prefer explicit dwc:occurrenceID first
  const idxExactOcc = headers.indexOf('dwc:occurrenceID');
  if (idxExactOcc >= 0) return headers[idxExactOcc];
  const idxExactTax = headers.indexOf('dwc:taxonID');
  if (idxExactTax >= 0) return headers[idxExactTax];
  // Prefer namespaced columns first (dwc:occurrenceID etc.)
  let idx = lc.findIndex(h => /:(occurrenceid|taxonid)$/.test(h));
  if (idx >= 0) return headers[idx];
  // Then unprefixed Darwin Core local names
  idx = lc.findIndex(h => /^(occurrenceid|taxonid)$/.test(h));
  if (idx >= 0) return headers[idx];
  // Fallback to first column
  return headers[0];
}

export function detectRecordIdHeaderFromData(headers, sampleRows, knownIdsSet) {
  if (!headers || !sampleRows || sampleRows.length === 0 || !knownIdsSet || knownIdsSet.size === 0) return detectRecordIdHeader(headers);
  // Name-based hint
  let bestHeader = detectRecordIdHeader(headers);
  let bestScore = -1;
  for (const h of headers) {
    let score = 0;
    for (const r of sampleRows) {
      const v = r[h];
      if (v !== undefined && v !== null) {
        const s = String(v).trim();
        if (s && knownIdsSet.has(s)) score++;
      }
    }
    if (score > bestScore) { 
      bestScore = score; 
      bestHeader = h; 
    }
  }
  return bestHeader;
}

export function mapTokensToHeaders(tokenString, headers) {
  if (!tokenString) return [];
  const headerSet = new Set(headers);
  const lookup = new Map(headers.map(h => [h.toLowerCase(), h]));
  const tokens = tokenString.split(/[,;]+/).map(s => s.trim()).filter(Boolean);
  const mapped = [];
  for (const tok of tokens) {
    const bare = tok.includes(':') ? tok.split(':').slice(1).join(':') : tok;
    const candidates = [tok, bare];
    for (const c of candidates) {
      const lc = c.toLowerCase();
      if (lookup.has(lc)) { 
        mapped.push(lookup.get(lc)); 
        break; 
      }
      // common DwC core uses localName without prefix
      if (lookup.has(bare.toLowerCase())) { 
        mapped.push(lookup.get(bare.toLowerCase())); 
        break; 
      }
    }
  }
  // de-duplicate
  return Array.from(new Set(mapped));
}

export function bumpTop(label, field, value) {
  if (!TOPS.has(label)) TOPS.set(label, new Map());
  const byField = TOPS.get(label) || FALLBACK_TOPS.get(label);
  if (!byField.has(field)) byField.set(field, new Map());
  const byVal = byField.get(field);
  byVal.set(value, (byVal.get(value) || 0) + 1);
}

export function bumpFallbackTop(label, fieldToken, value) {
  if (!FALLBACK_TOPS.has(label)) FALLBACK_TOPS.set(label, new Map());
  const byField = FALLBACK_TOPS.get(label);
  if (!byField.has(fieldToken)) byField.set(fieldToken, new Map());
  const byVal = byField.get(fieldToken);
  byVal.set(value, (byVal.get(value) || 0) + 1);
}

export function extractValuesFromComment(comment) {
  const vals = [];
  if (!comment) return vals;
  const re = /\[([^\]]+)\]/g;
  let m;
  while ((m = re.exec(comment)) !== null) {
    const v = normalizeValue(m[1]);
    if (v) vals.push(v);
  }
  return vals;
}

// Data loading functions
export async function loadTG2(url) {
  return withErrorHandling(async () => {
    validateUrl(url, 'TG2 URL');
    
    return new Promise((resolve, reject) => {
      Papa.parse(url, {
        download: true,
        header: true,
        dynamicTyping: false,
        skipEmptyLines: true,
        complete: (res) => {
          try {
            if (!res.data || res.data.length === 0) {
              throw new AppError('TG2 file is empty or invalid', 'EMPTY_TG2_FILE');
            }
            
            TG2.rows = res.data;
            TG2.byLabel.clear();
            for (const row of TG2.rows) {
              const label = row['Label'] || row['term_localName'] || row['prefLabel'];
              if (!label) continue;
              TG2.byLabel.set(label, row);
            }
            
            if (TG2.byLabel.size === 0) {
              throw new AppError('No valid test labels found in TG2 file', 'NO_VALID_LABELS');
            }
            
            resolve(TG2);
          } catch (e) { 
            reject(e); 
          }
        },
        error: (error) => {
          reject(new AppError(`Failed to load TG2 file: ${error.message}`, 'TG2_LOAD_ERROR', { url, error }));
        },
      });
    });
  }, 'Loading TG2 tests');
}

export async function loadResults(url) {
  return new Promise((resolve, reject) => {
    let recIdHeader = null;
    Papa.parse(url, {
      download: true,
      header: true,
      worker: true,
      skipEmptyLines: true,
      chunkSize: CONFIG.CHUNK_SIZE,
      chunk: (chunk) => {
        if (!recIdHeader) {
          recIdHeader = detectRecordIdHeader(chunk.meta.fields || Object.keys(chunk.data[0] || {}));
        }
        for (const row of chunk.data) {
          AGG.totals.rows++;
          const recordId = normalizeId(row[recIdHeader]);
          if (recordId) AGG.recordIds.add(recordId);
          const label = row['test_id'] || row['Label'];
          if (!label) continue;
          AGG.testIds.add(label);
          const testType = row['test_type'] || (TG2.byLabel.get(label) || {})['Type'] || 'Unknown';
          const outcome = getOutcome(row);
          const resultStr = (row['result'] || '').trim();
          const per = getPerTest(label);
          if (!per.type && testType) per.type = testType;
          inc(per.counts, outcome);

          if (outcome === 'EXTERNAL_PREREQUISITES_NOT_MET' || outcome === 'INTERNAL_PREREQUISITES_NOT_MET') {
            AGG.totals.prerequisites++;
          } else {
            AGG.totals.considered++;
            if (passByType(testType, outcome)) AGG.totals.pass++;
          }
          if (testType === 'Amendment' && (outcome === 'AMENDED' || outcome === 'FILLED_IN')) AGG.totals.amended++;
          
          // Track validation-specific metrics
          if (testType === 'Validation') {
            if (outcome === 'COMPLIANT') AGG.totals.validationCompliant++;
            if (outcome === 'NOT_COMPLIANT') AGG.totals.validationNotCompliant++;
          }
          
          // Track potential issues
          if (testType === 'Issue' && outcome === 'POTENTIAL_ISSUE') AGG.totals.potentialIssues++;

          const tg2 = TG2.byLabel.get(label);
          const ieClass = tg2 ? (tg2['IE Class'] || 'Unknown') : 'Unknown';
          const bucket = needsAttentionBucket(testType, outcome);
          if (bucket) {
            const counts = getIeClassCounts(ieClass);
            inc(counts, bucket);
            addAttention(label, recordId);
            // Fallback: also attempt to extract the acted-upon value from the comment
            const actedTokens = (tg2 && tg2['InformationElement:ActedUpon']) ? tg2['InformationElement:ActedUpon'].split(/[,;]+/).map(s => s.trim()).filter(Boolean) : [];
            const values = extractValuesFromComment(row['comment'] || '');
            if (values.length && actedTokens.length) {
              for (const token of actedTokens) for (const v of values) bumpFallbackTop(label, token, v);
            }
            // Also parse proposed key=value pairs from the result column for Amendments
            if (testType === 'Amendment' && actedTokens.length) {
              const parts = String(resultStr).split('|');
              for (const p of parts) {
                const idx = p.indexOf('=');
                if (idx > 0) {
                  const key = p.slice(0, idx).trim();
                  const value = normalizeValue(p.slice(idx + 1));
                  if (actedTokens.includes(key)) bumpFallbackTop(label, key, value);
                }
              }
            }
          }
        }
      },
      complete: () => resolve(AGG),
      error: reject,
    });
  });
}

export async function loadCoreData(url) {
  return new Promise((resolve, reject) => {
    let recIdHeader = null;
    let headers = null;
    let mappedFieldsByTest = null; // label -> [header]
    let matchedRows = 0;
    const attentionTotal = ATTENTION.byRecord.size;
    Papa.parse(url, {
      download: true,
      header: true,
      worker: true,
      skipEmptyLines: true,
      chunkSize: CONFIG.CHUNK_SIZE,
      chunk: (chunk) => {
        if (!headers) {
          headers = chunk.meta.fields || Object.keys(chunk.data[0] || {});
          // Prefer strict dwc:occurrenceID for the join, otherwise fallback to overlap detection
          recIdHeader = headers.includes('dwc:occurrenceID') ? 'dwc:occurrenceID' : detectRecordIdHeaderFromData(headers, chunk.data.slice(0, 200), AGG.recordIds);
          // Map acted-upon per test to actual headers, but only for tests with attention
          mappedFieldsByTest = new Map();
          for (const [label, ids] of ATTENTION.byTest.entries()) {
            if (!ids || ids.size === 0) continue;
            const tg2 = TG2.byLabel.get(label) || {};
            const fields = mapTokensToHeaders(tg2['InformationElement:ActedUpon'] || '', headers);
            if (fields.length) mappedFieldsByTest.set(label, fields);
          }
        }
        for (const row of chunk.data) {
          const recId = normalizeId(row[recIdHeader]);
          if (!recId || !ATTENTION.byRecord.has(recId)) continue;
          matchedRows++;
          const labels = ATTENTION.byRecord.get(recId);
          for (const label of labels) {
            const fields = mappedFieldsByTest.get(label);
            if (!fields || !fields.length) continue;
            for (const f of fields) {
              const val = normalizeValue(row[f]);
              bumpTop(label, f, val);
            }
          }
        }
      },
      complete: () => resolve(true),
      error: reject,
    });
  });
}

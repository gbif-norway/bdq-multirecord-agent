import { CONFIG } from './config.js';

// Utility helpers
export const $ = (q) => document.querySelector(q);
export const $$ = (q) => Array.from(document.querySelectorAll(q));
export const fmtInt = (n) => n.toLocaleString();
export const byValDesc = (a, b) => b.value - a.value;
export const sleep = (ms) => new Promise(r => setTimeout(r, ms));

export function showToast(msg, opts = {}) {
  const t = $('#toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  setTimeout(() => t.classList.add('hidden'), opts.ms || CONFIG.TOAST_DURATION);
}

export function getParam(name) {
  const u = new URL(window.location.href);
  return u.searchParams.get(name);
}

export function prettifyFileName(name) {
  try {
    const leaf = name.split('/').pop();
    return leaf.replace(/_/g, ' ').replace(/-/g, ' ');
  } catch { 
    return name; 
  }
}

export function isAbsoluteUrl(s) { 
  return /^https?:\/\//i.test(s); 
}

export function escapeHtml(s = '') { 
  return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', '\'': '&#39;' }[c])); 
}

export function truncate(s = '', n = CONFIG.MAX_DESCRIPTION_LENGTH) { 
  return s.length > n ? s.slice(0, n - 1) + '…' : s; 
}

export function normalizeValue(v) {
  if (v === null || v === undefined) return '(empty)';
  let s = String(v).trim();
  if (!s) return '(empty)';
  if (s.length > CONFIG.MAX_VALUE_LENGTH) s = s.slice(0, CONFIG.MAX_VALUE_LENGTH - 3) + '…';
  return s;
}

export function normalizeId(v) {
  if (v === null || v === undefined) return '';
  return String(v).trim();
}

export function inc(obj, key, by = 1) { 
  obj[key] = (obj[key] || 0) + by; 
}

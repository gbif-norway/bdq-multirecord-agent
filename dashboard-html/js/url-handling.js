import { isAbsoluteUrl, prettifyFileName } from './utils.js';

// URL handling functions
export function computeFileNameTitle(csvUrl) {
  try {
    const leaf = csvUrl.split('?')[0].split('#')[0].split('/').pop();
    const withoutExt = leaf.replace(/\.csv$/i, '');
    return prettifyFileName(withoutExt);
  } catch { 
    return prettifyFileName(csvUrl); 
  }
}

export function guessResultsUrlFromParam(param) {
  if (!param) return null;
  if (isAbsoluteUrl(param)) return param;
  // Assume results/ relative to this page
  const base = new URL(window.location.href);
  const url = new URL(param, base);
  // If param does not include a slash, prefix results/
  if (!param.includes('/')) {
    url.pathname = (base.pathname.replace(/[^\/]*$/, '')) + 'results/' + param;
  }
  return url.toString();
}

export function guessDataUrlFromParam(param) {
  if (!param) return null;
  if (isAbsoluteUrl(param)) return param;
  const base = new URL(window.location.href);
  const url = new URL(param, base);
  if (!param.includes('/')) {
    url.pathname = (base.pathname.replace(/[^\/]*$/, '')) + 'results/' + param;
  }
  return url.toString();
}

export function guessTG2Url() {
  // Prefer same folder TG2_tests.csv
  const base = new URL(window.location.href);
  const candidate = new URL('TG2_tests.csv', base).toString();
  return candidate;
}

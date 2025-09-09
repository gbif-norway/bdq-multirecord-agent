// Import modules
import { CONFIG } from './js/config.js';
import { $, showToast, getParam } from './js/utils.js';
import { resetDataStructures } from './js/data-structures.js';
import { loadTG2, loadResults, loadCoreData } from './js/data-processing.js';
import { buildNeedsChart } from './js/charts.js';
import { renderKPIs, renderAmendments, renderValidations, closeModal } from './js/ui-rendering.js';
import { computeFileNameTitle, guessResultsUrlFromParam, guessDataUrlFromParam, guessTG2Url } from './js/url-handling.js';
import { withErrorHandling, handleError } from './js/error-handling.js';

// Main application functions
async function main() {
  // From query param
  const resultsParam = getParam('results');
  const originalParam = getParam('original');
  const resultsUrl = guessResultsUrlFromParam(resultsParam || '');
  const originalUrl = originalParam ? guessDataUrlFromParam(originalParam) : null;
  if (resultsUrl) run(resultsUrl, originalUrl);
}

async function run(csvUrl, dataUrl) {
  return withErrorHandling(async () => {
    $('#fileName').textContent = 'Loading…';
    showToast('Loading TG2 tests…');
    await loadTG2(guessTG2Url());
    showToast('Parsing results…');
    
    // Reset state
    resetDataStructures();
    
    await loadResults(csvUrl);

    // Update header filename
    $('#fileName').textContent = 'for ' + computeFileNameTitle(csvUrl);

    // KPIs
    renderKPIs();
    // Chart
    buildNeedsChart();
    // Amendments list
    renderAmendments();
    // Validations list
    renderValidations();
    
    // Attempt to load core data for top values
    if (dataUrl) {
      try {
        showToast('Analyzing common values…');
        await loadCoreData(dataUrl);
        // re-render sections to include tops
        renderAmendments();
        renderValidations();
      } catch (e) {
        console.warn('Core data not available or failed:', e);
        showToast('Core data not loaded; common values unavailable.', { ms: 3000 });
      }
    }
    showToast('Report ready');
  }, 'Loading report').catch(error => {
    $('#fileName').textContent = 'Load error';
    throw error; // Re-throw to be handled by the error handler
  });
}


// Event listeners
window.addEventListener('DOMContentLoaded', main);

// Modal: close handlers
window.addEventListener('click', (e) => {
  const m = document.getElementById('modal');
  if (m && e.target === m) closeModal();
});

window.addEventListener('keydown', (e) => { 
  if (e.key === 'Escape') closeModal(); 
});

document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'modalClose') closeModal();
});
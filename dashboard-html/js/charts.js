import { CONFIG } from './config.js';
import { IE_CLASS_COUNTS, getClassColor } from './data-structures.js';
import { escapeHtml } from './utils.js';

// Chart building
let needsChartRef = null;

export function buildNeedsChart() {
  const ctx = document.getElementById('needsChart');
  const classes = Array.from(IE_CLASS_COUNTS.keys());
  const stacks = classes.map(cls => IE_CLASS_COUNTS.get(cls));
  const notCompliant = stacks.map(s => s.NOT_COMPLIANT || 0);
  const amended = stacks.map(s => s.AMENDED || 0);
  const potential = stacks.map(s => s.POTENTIAL_ISSUE || 0);
  
  // Create legend chips for IE Class colors
  const legendHost = document.getElementById('classLegend');
  if (legendHost) {
    const items = classes.map(cls => {
      const col = getClassColor(cls);
      return `<span class="legend-chip"><span class="legend-dot" style="background:${col}"></span>${escapeHtml(cls)}</span>`;
    }).join(' ');
    legendHost.innerHTML = items || '<div class="muted">No classes to show.</div>';
  }

  const data = {
    labels: classes,
    datasets: [
      { 
        label: 'Validation: NOT_COMPLIANT', 
        data: notCompliant, 
        backgroundColor: CONFIG.CHART_COLORS.NOT_COMPLIANT.bg, 
        borderColor: CONFIG.CHART_COLORS.NOT_COMPLIANT.border, 
        borderWidth: 1, 
        stack: 'stack0' 
      },
      { 
        label: 'Amendment: AMENDED', 
        data: amended, 
        backgroundColor: CONFIG.CHART_COLORS.AMENDED.bg, 
        borderColor: CONFIG.CHART_COLORS.AMENDED.border, 
        borderWidth: 1, 
        stack: 'stack0' 
      },
      { 
        label: 'Issue: POTENTIAL_ISSUE', 
        data: potential, 
        backgroundColor: CONFIG.CHART_COLORS.POTENTIAL_ISSUE.bg, 
        borderColor: CONFIG.CHART_COLORS.POTENTIAL_ISSUE.border, 
        borderWidth: 1, 
        stack: 'stack0' 
      },
    ]
  };
  
  if (needsChartRef) try { needsChartRef.destroy(); } catch {}
  needsChartRef = new Chart(ctx, {
    type: 'bar',
    data,
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: { 
        x: { stacked: true, ticks: { color: '#0f172a' }, grid: { color: 'rgba(148,163,184,0.25)' } }, 
        y: { stacked: true, beginAtZero: true, ticks: { color: '#0f172a' }, grid: { color: 'rgba(148,163,184,0.25)' } } 
      },
      plugins: {
        legend: { position: 'bottom', labels: { color: '#0f172a' } },
        tooltip: { mode: 'index', intersect: false },
        title: { display: true, text: 'Attention needed by data area', color: '#0f172a' }
      }
    }
  });
  return needsChartRef;
}

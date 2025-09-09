// Configuration constants
export const CONFIG = {
  // Performance settings
  CHUNK_SIZE: 1024 * 1024, // 1MB chunks for CSV parsing
  TOAST_DURATION: 2600,
  
  // UI dimensions
  CHART_HEIGHT: 360,
  TABLE_HEIGHT: 420,
  PAGINATION_SIZE: 12,
  
  // Data display limits
  MAX_TOP_VALUES: 5,
  MAX_TOP_VALUES_DISPLAY: 3,
  MAX_DESCRIPTION_LENGTH: 220,
  MAX_VALUE_LENGTH: 120,
  
  // Visual styling
  CLASS_PALETTE: ['#C7D2FE', '#FBCFE8', '#BAE6FD', '#A7F3D0', '#FDE68A', '#E9D5FF'],
  
  // Data validation
  CANONICAL_OUTCOMES: [
    'COMPLIANT', 
    'NOT_COMPLIANT', 
    'AMENDED', 
    'NOT_AMENDED', 
    'POTENTIAL_ISSUE', 
    'NOT_ISSUE', 
    'EXTERNAL_PREREQUISITES_NOT_MET', 
    'INTERNAL_PREREQUISITES_NOT_MET'
  ],
  
  // Test types
  TEST_TYPES: {
    VALIDATION: 'Validation',
    AMENDMENT: 'Amendment',
    ISSUE: 'Issue'
  },
  
  // Chart colors
  CHART_COLORS: {
    NOT_COMPLIANT: { bg: 'rgba(251,207,232,0.85)', border: '#f472b6' },
    AMENDED: { bg: 'rgba(253,230,138,0.85)', border: '#f59e0b' },
    POTENTIAL_ISSUE: { bg: 'rgba(186,230,253,0.85)', border: '#38bdf8' }
  },
  
  // External dependencies
  EXTERNAL_LIBS: {
    TABULATOR: 'https://unpkg.com/tabulator-tables@5.5.2/dist/js/tabulator.min.js',
    CHART_JS: 'https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js',
    PAPA_PARSE: 'https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js'
  },
  
  // Default file names
  DEFAULT_FILES: {
    TG2_TESTS: 'TG2_tests.csv'
  },
  
  // Error messages
  ERROR_MESSAGES: {
    LOAD_FAILED: 'Failed to load report. See console.',
    CORE_DATA_UNAVAILABLE: 'Core data not loaded; common values unavailable.',
    NO_TESTS_FOUND: 'No tests found in results.',
    NO_CLASSES_TO_SHOW: 'No classes to show.'
  }
};

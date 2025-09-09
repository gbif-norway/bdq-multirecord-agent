import { CONFIG } from './config.js';

// Data holders and management
export const TG2 = {
  byLabel: new Map(), // Label (test_id) -> test info row
  rows: [], // all rows for catalogue
};

export const AGG = {
  recordIds: new Set(),
  testIds: new Set(),
  totals: { rows: 0, prerequisites: 0, pass: 0, considered: 0, amended: 0, validationCompliant: 0, validationNotCompliant: 0, potentialIssues: 0 },
  perTest: new Map(), // label -> { counts: { NOT_COMPLIANT, AMENDED, POTENTIAL_ISSUE, COMPLIANT, NOT_AMENDED, NOT_ISSUE, EXTERNAL_PREREQUISITES_NOT_MET, INTERNAL_PREREQUISITES_NOT_MET, other }, type: 'Validation'|'Amendment'|'Issue'|unknown }
};

export const IE_CLASS_COUNTS = new Map(); // class -> { NOT_COMPLIANT, AMENDED, POTENTIAL_ISSUE }
export const CLASS_COLORS = new Map();

// Track which records are in a needs-attention state for each test
export const ATTENTION = {
  byTest: new Map(),     // label -> Set(recordId)
  byRecord: new Map(),   // recordId -> Set(label)
};

// Top values per test per field from the original data file
export const TOPS = new Map(); // label -> Map(fieldHeader -> Map(value -> count))
// Fallback values from results comments when core fields are unavailable
export const FALLBACK_TOPS = new Map(); // label -> Map(fieldToken -> Map(value -> count))

// Amendment data tracking
export const AMENDMENTS = new Map(); // label -> { original: Map(value -> count), amended: Map(value -> count), total: number }

// Top grouped data tracking (similar to Python _get_top_grouped)
export const TOP_GROUPED = {
  amendments: new Map(), // groupKey -> count
  validations: new Map(), // groupKey -> count
  issues: new Map(), // groupKey -> count
  filledIn: new Map() // groupKey -> count
};

// Data management functions
export function getPerTest(label) {
  if (!AGG.perTest.has(label)) {
    AGG.perTest.set(label, { counts: {}, type: undefined });
  }
  return AGG.perTest.get(label);
}

export function addAttention(label, recordId) {
  if (!recordId) return;
  if (!ATTENTION.byTest.has(label)) ATTENTION.byTest.set(label, new Set());
  ATTENTION.byTest.get(label).add(recordId);
  if (!ATTENTION.byRecord.has(recordId)) ATTENTION.byRecord.set(recordId, new Set());
  ATTENTION.byRecord.get(recordId).add(label);
}

export function getIeClassCounts(cls) {
  if (!IE_CLASS_COUNTS.has(cls)) {
    IE_CLASS_COUNTS.set(cls, { NOT_COMPLIANT: 0, AMENDED: 0, POTENTIAL_ISSUE: 0 });
  }
  return IE_CLASS_COUNTS.get(cls);
}

export function getClassColor(cls) {
  if (!CLASS_COLORS.has(cls)) {
    const idx = CLASS_COLORS.size % CONFIG.CLASS_PALETTE.length;
    CLASS_COLORS.set(cls, CONFIG.CLASS_PALETTE[idx]);
  }
  return CLASS_COLORS.get(cls);
}

export function getAmendmentData(label) {
  if (!AMENDMENTS.has(label)) {
    AMENDMENTS.set(label, { original: new Map(), amended: new Map(), total: 0 });
  }
  return AMENDMENTS.get(label);
}

export function resetDataStructures() {
  AGG.recordIds.clear(); 
  AGG.testIds.clear(); 
  AGG.perTest.clear(); 
  IE_CLASS_COUNTS.clear();
  ATTENTION.byTest.clear(); 
  ATTENTION.byRecord.clear(); 
  TOPS.clear();
  FALLBACK_TOPS.clear();
  AMENDMENTS.clear();
  TOP_GROUPED.amendments.clear();
  TOP_GROUPED.validations.clear();
  TOP_GROUPED.issues.clear();
  TOP_GROUPED.filledIn.clear();
  AGG.totals = { rows: 0, prerequisites: 0, pass: 0, considered: 0, amended: 0, validationCompliant: 0, validationNotCompliant: 0, potentialIssues: 0 };
}

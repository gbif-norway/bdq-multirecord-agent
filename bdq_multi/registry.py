import csv
import json
import os
from typing import Dict, List, Optional, Tuple


def load_registry(csv_path: str = "TG2_multirecord_measure_tests.csv") -> List[Dict[str, str]]:
    """Load the TG2 multirecord registry CSV into a list of dicts.

    Returns a list of rows; if the file is missing, returns an empty list.
    """
    if not os.path.exists(csv_path):
        return []
    rows: List[Dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def load_mapping(map_path: str) -> Dict[str, Dict]:
    """Load a simple JSON mapping of registry Label -> {name, params}.

    If missing, returns an empty mapping.
    """
    if not os.path.exists(map_path):
        return {}
    with open(map_path, encoding="utf-8") as fh:
        return json.load(fh)


def measures_from_registry(
    registry_csv: str,
    mapping_json: Optional[str] = None,
    include_labels: Optional[List[str]] = None,
) -> List[Dict]:
    """Build measure configs by auto-mapping the registry and optional overrides.

    Strategy:
    - For each row in the registry, derive the target single-record label from the
      "InformationElement:ActedUpon" column (e.g., bdq:VALIDATION_COUNTRYCODE_STANDARD.Response).
    - If the registry Label or prefLabel suggests Counting Compliance, create
      AggregateFromSingleLabel with count_result=COMPLIANT.
    - If it suggests Quality Assurance (QA), create QaAllCompliantOrPrereq.
    - Apply overrides from mapping_json if present (Label -> {name, params}).
    """
    reg = load_registry(registry_csv)
    overrides: Dict[str, Dict] = load_mapping(mapping_json) if mapping_json else {}

    def derive_target(row: Dict[str, str]) -> Optional[str]:
        acted = row.get("InformationElement:ActedUpon") or ""
        # Expect like: bdq:VALIDATION_X.Response
        if ":" in acted:
            acted = acted.split(":", 1)[1]
        if acted.endswith(".Response"):
            acted = acted[: -len(".Response")]
        return acted or None

    def classify(row: Dict[str, str]) -> Optional[str]:
        label = row.get("Label", "") or ""
        pref = row.get("prefLabel", "") or ""
        if "COUNT" in label or "Counting Compliance" in pref:
            return "count"
        if "QA_" in label or "QualityAssurance" in pref:
            return "qa"
        # Fallback: use ExpectedResponse wording
        er = row.get("ExpectedResponse", "")
        if "Count" in er:
            return "count"
        if "COMPLETE" in er and "NOT_COMPLETE" in er:
            return "qa"
        return None

    out: List[Dict] = []
    for row in reg:
        lbl = row.get("Label")
        if not lbl:
            continue
        if include_labels is not None and lbl not in include_labels:
            continue

        # Overrides by label
        if lbl in overrides:
            spec = overrides[lbl]
            if isinstance(spec, dict) and "name" in spec:
                out.append({"name": spec["name"], "params": spec.get("params") or {}})
            continue

        target = derive_target(row)
        kind = classify(row)
        if not target or not kind:
            continue

        guid = row.get("term_localName") or row.get("iri") or row.get("term_iri")
        if kind == "count":
            out.append({
                "name": "AggregateFromSingleLabel",
                "params": {"target_label": target, "count_result": "COMPLIANT"},
                "label": lbl,
                "guid": guid,
            })
        elif kind == "qa":
            out.append({
                "name": "QaAllCompliantOrPrereq",
                "params": {"target_label": target},
                "label": lbl,
                "guid": guid,
            })

    return out

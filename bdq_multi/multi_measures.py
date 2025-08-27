import argparse
import csv
import datetime as dt
import gzip
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from . import __name__ as _pkg_name  # noqa: F401
from .registry import measures_from_registry


class BdqResponse(object):
    def __init__(
        self,
        label: str,
        status: str,
        result: Union[int, float, str],
        comment: Optional[str] = None,
        qualifier: Optional[Dict[str, Any]] = None,
        guid: Optional[str] = None,
    ) -> None:
        self.label = label
        self.status = status
        self.result = result
        self.comment = comment
        self.qualifier = qualifier
        self.guid = guid

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "label": self.label,
            "status": self.status,
            "result": self.result,
        }
        if self.comment is not None:
            out["comment"] = self.comment
        if self.qualifier is not None:
            out["qualifier"] = self.qualifier
        if self.guid is not None:
            out["guid"] = self.guid
        return out


class MultiRecordMeasure:
    """Base class for multi-record measures."""

    label: str = ""

    def __init__(self) -> None:
        self._prepared = False
        self._label_override: Optional[str] = None
        self._guid: Optional[str] = None

    def prepare(self, params: Optional[Dict[str, Any]]) -> None:  # noqa: ARG002
        self._prepared = True
        if params:
            # Allow reserved override fields
            if "label" in params:
                self._label_override = str(params["label"]) or None
            if "label_override" in params:
                self._label_override = str(params["label_override"]) or self._label_override
            if "guid" in params:
                self._guid = str(params["guid"]) or None

    def consume_row(self, row: Dict[str, Any]) -> None:  # pragma: no cover - override in subclasses
        del row

    # Optional for measures that aggregate single-record results
    def consume_single(self, row: Dict[str, Any]) -> None:  # pragma: no cover - override in subclasses
        del row

    def finish(self) -> BdqResponse:  # pragma: no cover - override in subclasses
        lbl = self._label_override or self.label or self.__class__.__name__
        return BdqResponse(label=lbl, status="RUN_HAS_RESULT", result=0, guid=self._guid)


class OccurrenceIdDuplicatesScan(MultiRecordMeasure):
    label = "OccurrenceIdDuplicates"

    def __init__(self) -> None:
        super().__init__()
        self.seen: Set[str] = set()
        self.duplicates = 0

    def consume_row(self, row: Dict[str, Any]) -> None:
        oid = (row.get("occurrenceID") or "").strip()
        if not oid:
            return
        if oid in self.seen:
            self.duplicates += 1
        else:
            self.seen.add(oid)

    def finish(self) -> BdqResponse:
        lbl = self._label_override or self.label
        return BdqResponse(label=lbl, status="RUN_HAS_RESULT", result=int(self.duplicates), guid=self._guid)


class CoordinateDuplicatesScan(MultiRecordMeasure):
    label = "CoordinateDuplicates"

    def __init__(self) -> None:
        super().__init__()
        self.round_decimals = 2
        self.seen: Set[Tuple[int, int]] = set()
        self.duplicates = 0

    def prepare(self, params: Optional[Dict[str, Any]]) -> None:
        super().prepare(params)
        if params and isinstance(params.get("round_decimals"), int):
            self.round_decimals = int(params["round_decimals"])

    def consume_row(self, row: Dict[str, Any]) -> None:
        lat_s = row.get("decimalLatitude")
        lon_s = row.get("decimalLongitude")
        try:
            lat = float(lat_s) if lat_s not in (None, "") else None
            lon = float(lon_s) if lon_s not in (None, "") else None
        except (TypeError, ValueError):
            return
        if lat is None or lon is None:
            return
        f = 10**self.round_decimals
        key = (int(round(lat * f)), int(round(lon * f)))
        if key in self.seen:
            self.duplicates += 1
        else:
            self.seen.add(key)

    def finish(self) -> BdqResponse:
        lbl = self._label_override or self.label
        return BdqResponse(label=lbl, status="RUN_HAS_RESULT", result=int(self.duplicates), guid=self._guid)


class AggregateFromSingleLabel(MultiRecordMeasure):
    """Count single-record results with a given label and result."""

    label = "AggregateFromSingleLabel"

    def __init__(self) -> None:
        super().__init__()
        self.target_label: Optional[str] = None
        self.count_result: Optional[str] = None
        self.count = 0

    def prepare(self, params: Optional[Dict[str, Any]]) -> None:
        super().prepare(params)
        params = params or {}
        self.target_label = params.get("target_label")
        self.count_result = params.get("count_result")

    def consume_single(self, row: Dict[str, Any]) -> None:
        # Accept common column names: label/Label and result/Result
        label = row.get("label") or row.get("Label") or row.get("test") or row.get("Test")
        result = row.get("result") or row.get("Result")
        if self.target_label and label != self.target_label:
            return
        if self.count_result is None or result == self.count_result:
            self.count += 1

    def finish(self) -> BdqResponse:
        qualifier = {"target_label": self.target_label, "count_result": self.count_result}
        lbl = self._label_override or self.label
        if self.target_label:
            # Only suffix if not overridden
            if self._label_override is None:
                lbl = f"{lbl}:{self.target_label}"
        return BdqResponse(label=lbl, status="RUN_HAS_RESULT", result=int(self.count), qualifier=qualifier, guid=self._guid)


class QaAllCompliantOrPrereq(MultiRecordMeasure):
    """Emit COMPLETE if all singles of a target label are COMPLIANT or have status INTERNAL_PREREQUISITES_NOT_MET; else NOT_COMPLETE."""

    label = "QaAllCompliantOrPrereq"

    def __init__(self) -> None:
        super().__init__()
        self.target_label: Optional[str] = None
        self.bad = 0
        self.total = 0

    def prepare(self, params: Optional[Dict[str, Any]]) -> None:
        super().prepare(params)
        params = params or {}
        self.target_label = params.get("target_label")

    def consume_single(self, row: Dict[str, Any]) -> None:
        label = row.get("label") or row.get("Label") or row.get("test") or row.get("Test")
        if self.target_label and label != self.target_label:
            return
        result = row.get("result") or row.get("Result")
        status = (
            row.get("status")
            or row.get("Status")
            or row.get("response_status")
            or row.get("Response.status")
            or row.get("ResponseStatus")
        )
        self.total += 1
        if str(result) == "COMPLIANT" or str(status) == "INTERNAL_PREREQUISITES_NOT_MET":
            return
        self.bad += 1

    def finish(self) -> BdqResponse:
        lbl = self._label_override or self.label
        if self.target_label:
            if self._label_override is None:
                lbl = f"{lbl}:{self.target_label}"
        result = "COMPLETE" if self.bad == 0 else "NOT_COMPLETE"
        qualifier = {"target_label": self.target_label, "non_compliant": self.bad, "total": self.total}
        return BdqResponse(label=lbl, status="RUN_HAS_RESULT", result=result, qualifier=qualifier, guid=self._guid)


def build_measures(config: Optional[Union[str, List[Dict[str, Any]]]]) -> List[MultiRecordMeasure]:
    if config is None:
        measures: List[MultiRecordMeasure] = [OccurrenceIdDuplicatesScan(), CoordinateDuplicatesScan()]
        for m in measures:
            m.prepare({})
        return measures

    if isinstance(config, str):
        try:
            parsed = json.loads(config)
        except json.JSONDecodeError:
            raise ValueError("measures must be JSON list of {name, params}")
    else:
        parsed = config

    if not isinstance(parsed, list):
        raise ValueError("measures must be a list")

    class_map = {
        "OccurrenceIdDuplicatesScan": OccurrenceIdDuplicatesScan,
        "CoordinateDuplicatesScan": CoordinateDuplicatesScan,
        "AggregateFromSingleLabel": AggregateFromSingleLabel,
        "QaAllCompliantOrPrereq": QaAllCompliantOrPrereq,
    }

    out: List[MultiRecordMeasure] = []
    for item in parsed:
        if not isinstance(item, dict) or "name" not in item:
            raise ValueError("each measure must be an object with a 'name'")
        name = item["name"]
        params = item.get("params") or {}
        cls = class_map.get(name)
        if cls is None:
            raise ValueError(f"unknown measure: {name}")
        inst = cls()
        inst.prepare(params)
        # Apply top-level overrides
        if "label" in item:
            inst._label_override = str(item["label"]) or inst._label_override
        if "guid" in item:
            inst._guid = str(item["guid"]) or inst._guid
        out.append(inst)
    return out


def stream_dataset_csv(path: str, measures: Iterable[MultiRecordMeasure]) -> None:
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            for m in measures:
                m.consume_row(row)


def stream_single_results_csv(path: str, measures: Iterable[MultiRecordMeasure]) -> None:
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            for m in measures:
                # Not all measures handle single-record aggregation
                if hasattr(m, "consume_single"):
                    m.consume_single(row)  # type: ignore[attr-defined]


def write_outputs(out_dir: str, responses: List[BdqResponse]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    # Write JSON Lines
    mjl = os.path.join(out_dir, "measures.jsonl")
    with open(mjl, "w", encoding="utf-8") as fh:
        for r in responses:
            fh.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")

    # Write summary
    summary = {r.label: r.result for r in responses}
    summary["timestamp"] = dt.datetime.utcnow().isoformat() + "Z"
    with open(os.path.join(out_dir, "measures_summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    # Also emit gz versions for convenience
    for name in ("measures.jsonl", "measures_summary.json"):
        src = os.path.join(out_dir, name)
        with open(src, "rb") as fi, gzip.open(src + ".gz", "wb") as fo:
            fo.writelines(fi)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run multi-record BDQ measures")
    parser.add_argument("--dataset-csv", help="Path to dataset CSV", default=None)
    parser.add_argument("--test-results-csv", help="Path to single-record results CSV", default=None)
    parser.add_argument("--out-dir", help="Output directory", default="out")
    parser.add_argument(
        "--measures",
        help="JSON list of measures, e.g. [{\"name\":\"OccurrenceIdDuplicatesScan\"}]",
        default=None,
    )
    parser.add_argument("--use-registry", action="store_true", help="Build measures from TG2 registry + mapping")
    parser.add_argument("--registry-csv", default="TG2_multirecord_measure_tests.csv", help="Registry CSV path")
    parser.add_argument(
        "--registry-map",
        default=os.path.join(os.path.dirname(__file__), "registry_map.json"),
        help="JSON mapping of registry Label to measure class + params",
    )
    args = parser.parse_args(argv)

    measures_cfg = None
    if args.use_registry and args.measures is None:
        # Derive measures from registry + mapping
        reg_cfg = measures_from_registry(args.registry_csv, args.registry_map)
        measures_cfg = reg_cfg
    else:
        measures_cfg = args.measures

    measures = build_measures(measures_cfg)

    if args.dataset_csv:
        stream_dataset_csv(args.dataset_csv, measures)
    if args.test_results_csv:
        stream_single_results_csv(args.test_results_csv, measures)

    write_outputs(args.out_dir, [m.finish() for m in measures])

    summary_path = os.path.join(args.out_dir, "measures_summary.json")
    print(f"Wrote {summary_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

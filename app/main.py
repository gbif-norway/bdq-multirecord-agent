import base64
import csv
import io
import json
import os
import re
import sys
import traceback
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import chardet
import requests
from flask import Flask, Response, request
from imapclient import IMAPClient
import smtplib
from dotenv import load_dotenv


# Load .env for local development
load_dotenv()

app = Flask(__name__)


# Config
BDQ_API_BASE = os.getenv("BDQ_API_BASE", "https://bdq-api-638241344017.europe-west1.run.app")
GMAIL_POLL_LABEL = os.getenv("GMAIL_POLL_LABEL", "")
GMAIL_FROM_FILTER = os.getenv("GMAIL_FROM_FILTER", "")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
HTTP_TIMEOUT_S = int(os.getenv("HTTP_TIMEOUT_S", "30"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
REPLY_FROM = os.getenv("REPLY_FROM", os.getenv("SMTP_USER", ""))
MAX_ATTACHMENT_MB = int(os.getenv("MAX_ATTACHMENT_MB", "25"))

# IMAP/SMTP
IMAP_HOST = os.getenv("IMAP_HOST", "imap.gmail.com")
IMAP_USER = os.getenv("IMAP_USER", "")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", IMAP_USER)
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", IMAP_PASSWORD)

# Labels
PROCESSED_LABEL = os.getenv("GMAIL_PROCESSED_LABEL", "DwC-Processed")
INCOMING_LABEL = os.getenv("GMAIL_POLL_LABEL", "DwC-Incoming")


# ---------------------- Utilities ----------------------


def _log(msg: str):
    print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)


def post_discord(stage: str, err: Exception, **ctx):
    if not DISCORD_WEBHOOK_URL:
        _log(f"Discord webhook not set. {stage}: {err}")
        return
    payload = {
        "stage": stage,
        "message": str(err),
        "trace": traceback.format_exc(),
    }
    payload.update({k: v for k, v in ctx.items() if v is not None})
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        _log(f"Failed posting to Discord: {e}")


def strip_prefix(term: str) -> str:
    if ":" in term:
        return term.split(":", 1)[1]
    return term


def detect_encoding(b: bytes) -> str:
    try:
        guess = chardet.detect(b)
        enc = guess.get("encoding")
        return enc or "utf-8"
    except Exception:
        return "utf-8"


def sniff_delimiter(header_line: str) -> str:
    if "\t" in header_line:
        return "\t"
    return ","


def normalize_assertion(resp: Dict[str, Any]) -> Dict[str, Any]:
    # Expected normalized fields: status, result, comment, amendment
    status = resp.get("status")
    result = resp.get("result")
    comment = resp.get("comment")
    amendment = resp.get("amendment")

    # If amendment is embedded differently, try to infer
    if amendment is None and isinstance(resp.get("amendment"), dict):
        amendment = resp.get("amendment")

    return {
        "status": status,
        "result": result,
        "comment": comment,
        "amendment": amendment,
    }


# ---------------------- Gmail via IMAP/SMTP ----------------------


@dataclass
class MessageHandle:
    uid: int
    envelope_from: Optional[str]
    subject: Optional[str]
    thread_id: Optional[int]
    raw_bytes: bytes


def gmail_connect() -> IMAPClient:
    client = IMAPClient(IMAP_HOST, use_uid=True, ssl=True)
    client.login(IMAP_USER, IMAP_PASSWORD)
    client.select_folder("INBOX")
    return client


def gmail_list_unprocessed() -> List[MessageHandle]:
    client = gmail_connect()
    try:
        criteria = ["UNSEEN"]
        # Gmail raw search for label/from filtering
        raw_parts = []
        if GMAIL_POLL_LABEL:
            raw_parts.append(f"label:{GMAIL_POLL_LABEL}")
        if GMAIL_FROM_FILTER:
            raw_parts.append(f"from:{GMAIL_FROM_FILTER}")
        if raw_parts:
            uids = client.gmail_search(" ".join(raw_parts))
            # intersect with UNSEEN
            if uids:
                unseen = client.search(["UNSEEN"]) or []
                uids = list(set(uids).intersection(set(unseen)))
            else:
                uids = []
        else:
            uids = client.search(criteria)

        msgs: List[MessageHandle] = []
        if not uids:
            return msgs

        fetch_data = client.fetch(uids, [b"RFC822", b"X-GM-THRID", b"ENVELOPE"])
        for uid, data in fetch_data.items():
            raw = data.get(b"RFC822")
            env = data.get(b"ENVELOPE")
            thr = data.get(b"X-GM-THRID")
            subj = env.subject.decode() if env and env.subject else None
            from_addr = None
            if env and env.from_:
                a = env.from_[0]
                mailbox = a.mailbox.decode() if a.mailbox else ""
                host = a.host.decode() if a.host else ""
                from_addr = f"{mailbox}@{host}" if mailbox and host else None
            msgs.append(MessageHandle(uid=uid, envelope_from=from_addr, subject=subj, thread_id=thr, raw_bytes=raw))
        return msgs
    finally:
        try:
            client.logout()
        except Exception:
            pass


def gmail_mark_processed(uid: int):
    client = gmail_connect()
    try:
        # Add processed label, mark as seen, remove incoming if set
        try:
            client.add_gmail_labels(uid, [PROCESSED_LABEL])
        except Exception:
            pass
        try:
            if INCOMING_LABEL:
                client.remove_gmail_labels(uid, [INCOMING_LABEL])
        except Exception:
            pass
        client.add_flags(uid, [b"\\Seen"])
    finally:
        try:
            client.logout()
        except Exception:
            pass


def gmail_reply_with_attachments(original_msg: MessageHandle, body_text: str, attachments: List[Tuple[str, bytes]], subject_suffix: str = ""):
    # Parse original to get sender and subject
    msg = BytesParser(policy=policy.default).parsebytes(original_msg.raw_bytes)
    sender = msg.get("From")
    orig_subject = msg.get("Subject", "")
    to_addr = sender

    reply = EmailMessage()
    subject = f"BDQ Report: {orig_subject}"
    if subject_suffix:
        subject = f"{subject} — {subject_suffix}"
    reply["Subject"] = subject
    reply["From"] = REPLY_FROM or SMTP_USER
    reply["To"] = to_addr

    reply.set_content(body_text)

    total_size = 0
    for name, data in attachments:
        total_size += len(data)
        if total_size > MAX_ATTACHMENT_MB * 1024 * 1024:
            # Avoid exceeding limits: stop attaching further
            break
        maintype, subtype = ("application", "octet-stream")
        if name.endswith(".csv"):
            maintype, subtype = ("text", "csv")
        elif name.endswith(".json"):
            maintype, subtype = ("application", "json")
        reply.add_attachment(data, maintype=maintype, subtype=subtype, filename=name)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASSWORD)
        s.send_message(reply)


def gmail_download_first_attachment(original_msg: MessageHandle) -> Tuple[bytes, str]:
    # parse full message MIME
    msg = BytesParser(policy=policy.default).parsebytes(original_msg.raw_bytes)
    for part in msg.walk():
        if part.is_multipart():
            continue
        disp = part.get_content_disposition()
        filename = part.get_filename()
        if disp == "attachment" and filename:
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            return payload, filename
    raise RuntimeError("No attachment found in email")


# ---------------------- Core processing ----------------------


def extract_core(att: Tuple[bytes, str]) -> Tuple[bytes, str]:
    data, fname = att
    lower = fname.lower()
    if lower.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            # Try meta.xml to find core
            core_name = None
            try:
                if "meta.xml" in zf.namelist():
                    import xml.etree.ElementTree as ET

                    with zf.open("meta.xml") as f:
                        tree = ET.parse(f)
                        root = tree.getroot()
                        core_el = root.find("{http://rs.tdwg.org/dwc/text/}core")
                        if core_el is not None:
                            files_el = core_el.find("{http://rs.tdwg.org/dwc/text/}files")
                            if files_el is not None:
                                location_el = files_el.find("{http://rs.tdwg.org/dwc/text/}location")
                                if location_el is not None and location_el.text:
                                    core_name = location_el.text.strip()
            except Exception:
                core_name = None
            # Fallbacks
            candidates = [
                core_name,
                "occurrence.txt",
                "taxon.txt",
                "occurrence.csv",
                "taxon.csv",
            ]
            for cand in candidates:
                if not cand:
                    continue
                if cand in zf.namelist():
                    with zf.open(cand) as f:
                        return f.read(), cand
            # pick first text-like file
            for name in zf.namelist():
                if name.endswith(('.txt', '.tsv', '.csv')):
                    with zf.open(name) as f:
                        return f.read(), name
            raise RuntimeError("No core file found in ZIP")
    else:
        return data, fname


def load_core(core_bytes: bytes, fname: str) -> Tuple[List[Dict[str, str]], List[str], str]:
    encoding = detect_encoding(core_bytes)
    text = core_bytes.decode(encoding, errors="replace")
    # Normalize newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    buf = io.StringIO(text)
    # sniff delimiter
    first_line = buf.readline()
    delim = "\t" if ("\t" in first_line or fname.lower().endswith(".tsv") or fname.lower().endswith(".txt")) else ","
    header = [h.strip() for h in first_line.strip().split(delim)]
    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(buf, fieldnames=header, delimiter=delim, restval="")
    for row in reader:
        # Ensure all keys exist as strings
        for k in header:
            if k not in row or row[k] is None:
                row[k] = ""
            else:
                row[k] = str(row[k])
        rows.append(row)
    return rows, header, delim


def detect_core_type(header: Sequence[str]) -> str:
    if "occurrenceID" in header:
        return "occurrence"
    if "taxonID" in header:
        return "taxon"
    raise RuntimeError("Cannot detect core type: missing occurrenceID/taxonID in header")


def fetch_tests() -> List[Dict[str, Any]]:
    url = f"{BDQ_API_BASE}/api/v1/tests"
    r = requests.get(url, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    tests = r.json()
    if not isinstance(tests, list):
        raise RuntimeError("Unexpected tests response format")
    return tests


def plan_tests(tests: List[Dict[str, Any]], header: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    header_set = set(header)
    ok: List[Dict[str, Any]] = []
    for t in tests:
        acted = [strip_prefix(c) for c in t.get("actedUpon", [])]
        if all(c in header_set for c in acted):
            ok.append(t)
    validations = [t for t in ok if t.get("type") == "Validation"]
    amendments = [t for t in ok if t.get("type") == "Amendment"]
    return {"validations": validations, "amendments": amendments}


def build_unique_sets(plan: Dict[str, List[Dict[str, Any]]], rows: List[Dict[str, str]]) -> Dict[str, set]:
    uniq: Dict[str, set] = defaultdict(set)
    for group in ("validations", "amendments"):
        for t in plan[group]:
            cols = [strip_prefix(c) for c in t.get("actedUpon", [])]
            tid = t.get("id")
            for r in rows:
                key = tuple((r.get(c) or "").strip() for c in cols)
                uniq[tid].add(key)
    return uniq


def run_test_once(t: Dict[str, Any], key: Tuple[str, ...]) -> Tuple[str, Tuple[str, ...], Dict[str, Any]]:
    tid = t.get("id")
    cols = [strip_prefix(c) for c in t.get("actedUpon", [])]
    inputs = {f"dwc:{c}": v for c, v in zip(cols, key)}
    payload = {"id": tid, "inputs": inputs, "parameters": {}}
    try:
        r = requests.post(f"{BDQ_API_BASE}/api/v1/tests/run", json=payload, timeout=HTTP_TIMEOUT_S)
        r.raise_for_status()
        data = r.json()
        assertion = normalize_assertion(data)
        return tid, key, assertion
    except Exception as e:
        # On error, return a synthesized assertion indicating failure
        return tid, key, {"status": "ERROR", "result": "ERROR", "comment": str(e), "amendment": None}


def format_raw(row_index: int, t: Dict[str, Any], cols: List[str], key: Tuple[str, ...], assertion: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "row_index": row_index,
        "test_id": t.get("id"),
        "test_type": t.get("type"),
        "status": assertion.get("status"),
        "result": assertion.get("result"),
        "comment": assertion.get("comment"),
        "acted_upon": ",".join(cols),
        "values": ",".join(key),
        "amendment_json": json.dumps(assertion.get("amendment") or {}, ensure_ascii=False),
    }


def apply_amendments(rows: List[Dict[str, str]], raw_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    amended = [dict(r) for r in rows]
    for rr in raw_rows:
        if rr.get("test_type") != "Amendment":
            continue
        try:
            amendment = json.loads(rr.get("amendment_json", "{}"))
        except Exception:
            amendment = {}
        if not isinstance(amendment, dict) or not amendment:
            continue
        i = rr.get("row_index")
        if i is None:
            continue
        for k, v in amendment.items():
            col = strip_prefix(k)
            if 0 <= i < len(amended):
                amended[i][col] = v
    return amended


FAIL_RESULTS = {"NOT_COMPLIANT", "POTENTIAL_ISSUE", "INTERNAL_PREREQUISITES_NOT_MET", "INTERNAL_PREREQUISITES_NOT_MET_PARTIAL"}


def count_validation_failures(raw_rows: List[Dict[str, Any]]) -> Counter:
    c = Counter()
    for rr in raw_rows:
        if rr.get("test_type") != "Validation":
            continue
        res = (rr.get("result") or "").upper()
        if res and res != "COMPLIANT":
            acted = rr.get("acted_upon", "")
            for col in [x for x in acted.split(",") if x]:
                c[col] += 1
    return c


def to_csv(dict_rows: List[Dict[str, Any]]) -> bytes:
    if not dict_rows:
        return b""
    header = list(dict_rows[0].keys())
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=header)
    w.writeheader()
    for r in dict_rows:
        w.writerow(r)
    return buf.getvalue().encode("utf-8")


def to_csv_rows(rows: List[Dict[str, str]], header: Sequence[str]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(header))
    w.writeheader()
    for r in rows:
        w.writerow({h: r.get(h, "") for h in header})
    return buf.getvalue().encode("utf-8")


def render_summary(n_records: int, plan: Dict[str, List[Dict[str, Any]]], by_field: Counter) -> str:
    lines = []
    lines.append(f"Records processed: {n_records}")
    lines.append(f"Applicable validations: {len(plan['validations'])}; amendments: {len(plan['amendments'])}")
    if by_field:
        lines.append("")
        lines.append("Top field issues:")
        for col, cnt in by_field.most_common(10):
            lines.append(f"- {col}: {cnt} potential issues")
    lines.append("")
    lines.append("Note: The attached amended dataset applies the BDQ-proposed amendments.")
    return "\n".join(lines)


# ---------------------- HTTP endpoint ----------------------


@app.get("/poll")
def poll() -> Response:
    try:
        msgs = gmail_list_unprocessed()
        if not msgs:
            return Response("ok: none", 200)
        for m in msgs:
            try:
                process_message(m)
                gmail_mark_processed(m.uid)
            except Exception as e:
                post_discord("process_message", e, email_id=m.uid, filename=m.subject)
        return Response("ok", 200)
    except Exception as e:
        post_discord("poll", e)
        return Response("err", 500)


def process_message(msg: MessageHandle):
    att = gmail_download_first_attachment(msg)
    core_bytes, fname = extract_core(att)
    rows, header, delim = load_core(core_bytes, fname)
    core_type = detect_core_type(header)
    tests = fetch_tests()
    plan = plan_tests(tests, header)

    uniq = build_unique_sets(plan, rows)
    cache: Dict[Tuple[str, Tuple[str, ...]], Dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = []
        for t in plan["validations"] + plan["amendments"]:
            tid = t.get("id")
            for key in uniq.get(tid, set()):
                futures.append(ex.submit(run_test_once, t, key))
        for f in as_completed(futures):
            tid, key, assertion = f.result()
            cache[(tid, key)] = assertion

    raw_rows: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        for t in plan["validations"] + plan["amendments"]:
            cols = [strip_prefix(c) for c in t.get("actedUpon", [])]
            key = tuple((row.get(c) or "").strip() for c in cols)
            assertion = cache.get((t.get("id"), key))
            if assertion is None:
                assertion = {"status": "SKIPPED", "result": "SKIPPED", "comment": "not evaluated", "amendment": None}
            raw_rows.append(format_raw(i, t, cols, key, assertion))

    amended = apply_amendments(rows, raw_rows)
    by_field = count_validation_failures(raw_rows)

    raw_csv = to_csv(raw_rows)
    amended_csv = to_csv_rows(amended, header)
    summary = render_summary(len(rows), plan, by_field)

    subject_suffix = f"{os.path.basename(fname)} — {len(rows)} records"
    gmail_reply_with_attachments(msg, summary, [
        ("bdq_raw_results.csv", raw_csv),
        ("amended_dataset.csv", amended_csv),
    ], subject_suffix=subject_suffix)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)


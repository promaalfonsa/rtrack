#!/usr/bin/env python3
# app.py
# Refund Tracker web app modified to read prebuilt data from a dedicated branch (data-updates)
# via the GitHub Contents API (authenticated). This avoids redeploying Vercel on every data commit.

from flask import Flask, render_template, request, jsonify
import os
import re
import json
import time
import base64
import requests
import csv
import io
import threading
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict, OrderedDict
from decimal import Decimal, InvalidOperation
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__, static_folder="static", template_folder="templates")

# Jinja filter
def timestamp_to_string(ts):
    try:
        if not ts:
            return ""
        t = int(ts)
        dt = datetime.fromtimestamp(t)
        return dt.strftime("%d %b %Y %H:%M")
    except Exception:
        return str(ts)
app.jinja_env.filters['timestamp_to_string'] = timestamp_to_string

# --- Config ---
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/data"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "600"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "8"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "0.5"))

DISABLE_BACKGROUND_SYNC = os.getenv("DISABLE_BACKGROUND_SYNC", "1") == "1"
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "600"))
CRON_SECRET = os.getenv("CRON_SECRET", "")

# GitHub API / data branch config (for private repo)
# IMPORTANT: Do NOT hardcode a PAT here. Put the token in Vercel environment variables.
GITHUB_API_TOKEN = os.getenv("GITHUB_API_TOKEN", "")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")
GITHUB_DATA_BRANCH = os.getenv("GITHUB_DATA_BRANCH", "data-updates")
INMEM_CACHE_TTL = int(os.getenv("INMEM_CACHE_TTL", "600"))

# logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# HEADER_MAP (map common CSV header variants to canonical keys)
HEADER_MAP = {
    "date": "date",
    "order number": "order_number",
    "ordernumber": "order_number",
    "order_no": "order_number",
    "order": "order_number",
    "sellerorderno": "sellerorderno",
    "seller order no": "sellerorderno",
    "seller order no.": "sellerorderno",
    "seller order": "sellerorderno",
    "payment method": "payment_method",
    "refund method": "refund_method",
    "wallet number": "wallet_number",
    "phone": "wallet_number",
    "card number": "card_number",
    "transaction amount (in bdt)": "amount",
    "transaction amount": "amount",
    "amount": "amount",
    "cashback": "cashback",
    "cash_back": "cashback",
    "transaction id": "transaction_id",
    "transactionid": "transaction_id",
    "transaction": "transaction_id",
}

# --- Sources ---
SOURCES = {
    "Bkash": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=0&single=true&output=csv",
    "Nagad": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=710748437&single=true&output=csv",
    "UPay": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=300224932&single=true&output=csv",
    "EBL": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=1590412026&single=true&output=csv",
    "SSL": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=747884683&single=true&output=csv",
    "MTB": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=1127499824&single=true&output=csv",
    "BRAC": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=2018794438&single=true&output=csv",
    "CityBank": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=1655876507&single=true&output=csv",
    "SEBL": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRLeRzOYkgTFBvY1jNnCSy9jwg967E5lUd133CUnrhfaMsfEOfxLRjIUPoWPtdKaYhYjMduj7GIVU9E/pub?gid=119713136&single=true&output=csv",
}

# --- HTTP session with retries ---
def requests_session_with_retries():
    s = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def safe_decimal_str(value):
    if value is None:
        return ""
    v = str(value).strip()
    if not v:
        return ""
    if re.search(r"[Ee]", v) or re.match(r"^\d+(\.\d+)?$", v):
        try:
            d = Decimal(v)
            if d == d.to_integral():
                return format(d.quantize(Decimal(1)), "f")
            else:
                return format(d.normalize(), "f")
        except InvalidOperation:
            return v
    return v

def normalize_header(raw):
    if raw is None:
        return ""
    h = raw.strip().lower()
    return HEADER_MAP.get(h, h.replace(" ", "_"))

def try_parse_date(s):
    s = (s or "").strip()
    if not s:
        return None
    s_clean = s.strip()
    formats = ["%m/%d/%Y","%m/%d/%y","%m-%d-%Y","%m-%d-%y","%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%b %d %Y","%d %b %Y"]
    for fmt in formats:
        try:
            return datetime.strptime(s_clean, fmt)
        except Exception:
            pass
    m = re.search(r"(\d{1,2})[^\d](\d{1,2})[^\d](\d{2,4})", s_clean)
    if m:
        g1,g2,g3 = m.groups()
        if len(g3) == 2:
            year = int(g3) + (2000 if int(g3) < 70 else 1900)
        else:
            year = int(g3)
        for (a,b) in ((g1,g2),(g2,g1)):
            try:
                return datetime(year,int(a),int(b))
            except Exception:
                pass
    try:
        return datetime.fromisoformat(s_clean)
    except Exception:
        return None

def format_date_for_ui(dt):
    if not dt:
        return ""
    return dt.strftime("%d %b %Y")

def parse_csv_content(content, source_name):
    f = io.StringIO(content)
    reader = csv.reader(f)
    rows = list(reader)
    if not rows:
        return []
    raw_headers = rows[0]
    headers = [normalize_header(h) for h in raw_headers]
    records = []
    for r in rows[1:]:
        if not any(cell.strip() for cell in r):
            continue
        row = list(r) + [""] * max(0, len(headers)-len(r))
        rec = {}
        for i,key in enumerate(headers):
            rec[key] = row[i].strip()
        rec["order_number"] = safe_decimal_str(rec.get("order_number","") or "")
        rec["sellerorderno"] = (rec.get("sellerorderno","") or "").strip()
        rec["payment_method"] = (rec.get("payment_method","") or "").strip()
        rec["wallet_number"] = (rec.get("wallet_number","") or "").strip()
        rec["card_number"] = (rec.get("card_number","") or "").strip()
        rec["wallet_number_normalized"] = re.sub(r"\D","", rec["wallet_number"])
        rec["transaction_id"] = (rec.get("transaction_id","") or "").strip()
        rec["amount"] = (rec.get("amount","") or "").strip()
        rec["cashback"] = (rec.get("cashback","") or "").strip()
        rec["date"] = (rec.get("date","") or "").strip()
        dt = try_parse_date(rec["date"])
        rec["date_obj"] = dt
        rec["date_fmt"] = format_date_for_ui(dt) if dt else rec["date"]
        rec["order_number_norm"] = (rec["order_number"] or "").upper()
        rec["sellerorderno_norm"] = (rec["sellerorderno"] or "").upper()
        rec["transaction_id_norm"] = (rec["transaction_id"] or "").upper()
        rec["source"] = source_name
        records.append(rec)
    return records

def cache_paths_for_source(source_name):
    safe_name = re.sub(r"[^\w\-]","_", source_name).lower()
    csv_path = DATA_DIR / f"refunds_{safe_name}.csv"
    json_path = DATA_DIR / f"refunds_{safe_name}.json"
    return csv_path, json_path

def _make_json_safe(records):
    safe = []
    for r in records:
        rec = {}
        for k,v in r.items():
            if k == "date_obj":
                continue
            if isinstance(v, datetime):
                rec[k] = v.isoformat()
                continue
            if v is None or isinstance(v, (str,int,float,bool,list,dict)):
                rec[k] = v
                continue
            try:
                json.dumps({k:v})
                rec[k] = v
            except Exception:
                rec[k] = str(v)
        safe.append(rec)
    return safe

# --- local cache loader (same as before) ---
def load_cache_for_source(source_name):
    csv_path, json_path = cache_paths_for_source(source_name)
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as jf:
                records = json.load(jf)
                for rec in records:
                    rec.setdefault("wallet_number_normalized", re.sub(r"\D","", rec.get("wallet_number","")))
                    rec.setdefault("order_number_norm", (rec.get("order_number","") or "").upper())
                    rec.setdefault("sellerorderno_norm", (rec.get("sellerorderno","") or "").upper())
                    rec.setdefault("transaction_id_norm", (rec.get("transaction_id","") or "").upper())
                    rec.setdefault("source", source_name)
                    dt = try_parse_date(rec.get("date","") or rec.get("date_fmt",""))
                    rec["date_obj"] = dt
                    rec["date_fmt"] = format_date_for_ui(dt) if dt else (rec.get("date_fmt") or rec.get("date",""))
                return records
        except Exception as ex:
            logger.warning("Failed to load JSON cache for %s: %s", source_name, ex)
    if csv_path.exists():
        try:
            text = csv_path.read_text(encoding="utf-8", errors="replace")
            return parse_csv_content(text, source_name)
        except Exception as ex:
            logger.warning("Failed to parse CSV cache for %s: %s", source_name, ex)
    return []

# --- fetch helpers (same as before) ---
_REFRESH_EXECUTOR = ThreadPoolExecutor(max_workers=4)
_refresh_in_progress = set()
_refresh_lock = threading.Lock()

def _schedule_background_refresh(source_name: str, url: str):
    with _refresh_lock:
        if source_name in _refresh_in_progress:
            logger.debug("Background refresh already running for %s, skipping", source_name)
            return
        _refresh_in_progress.add(source_name)
    logger.info("Scheduling background refresh for %s", source_name)
    _REFRESH_EXECUTOR.submit(_background_refresh_worker, source_name, url)

def _background_refresh_worker(source_name: str, url: str):
    try:
        session = requests_session_with_retries()
        logger.info("Background: fetching remote CSV from %s (source=%s)", url, source_name)
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        csv_text = r.content.decode("utf-8", errors="replace")
        records = parse_csv_content(csv_text, source_name)
        save_cache_for_source(source_name, csv_text, records)
        logger.info("Background: refreshed cache for %s with %d records", source_name, len(records))
    except Exception as ex:
        logger.warning("Background refresh failed for %s: %s", source_name, ex)
    finally:
        with _refresh_lock:
            _refresh_in_progress.discard(source_name)

def fetch_source_if_needed(source_name, url):
    csv_path, json_path = cache_paths_for_source(source_name)
    now = time.time()
    if csv_path.exists():
        try:
            mtime = csv_path.stat().st_mtime
            age = now - mtime
            if age < CACHE_TTL:
                logger.info("Using cached file for %s (age %ds < %ds)", source_name, int(age), CACHE_TTL)
                return load_cache_for_source(source_name)
            else:
                logger.info("Cache stale for %s (age %ds >= %ds). Returning stale data and scheduling background refresh.", source_name, int(age), CACHE_TTL)
                recs = load_cache_for_source(source_name)
                _schedule_background_refresh(source_name, url)
                return recs
        except Exception as ex:
            logger.warning("Error reading cache for %s: %s", source_name, ex)
    session = requests_session_with_retries()
    try:
        logger.info("No cache present for %s — performing blocking fetch", source_name)
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        csv_text = r.content.decode("utf-8", errors="replace")
        records = parse_csv_content(csv_text, source_name)
        save_cache_for_source(source_name, csv_text, records)
        logger.info("Fetched and cached remote CSV for %s with %d records", source_name, len(records))
        return records
    except Exception as ex:
        logger.warning("Blocking fetch failed for %s: %s", source_name, ex)
        if csv_path.exists() or json_path.exists():
            logger.info("Falling back to disk cache for %s after fetch failure", source_name)
            return load_cache_for_source(source_name)
        logger.error("No cache available for %s and fetch failed", source_name)
        return []

def fetch_all_sources():
    all_records = []
    for source_name, url in SOURCES.items():
        recs = fetch_source_if_needed(source_name, url)
        if recs:
            all_records.extend(recs)
    return all_records

def build_indices(records):
    by_order = defaultdict(list)
    by_seller = defaultdict(list)
    for r in records:
        by_order[r.get("order_number_norm", "")].append(r)
        by_seller[r.get("sellerorderno_norm", "")].append(r)
    return by_order, by_seller

def unique_by_key(records, key_fn):
    seen = set()
    out = []
    for r in records:
        k = key_fn(r)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out

def detect_query_type(q):
    s = (q or "").strip()
    if not s:
        return "unknown", s
    s_clean = re.sub(r"[^\w\-]", "", s)
    m_phone = re.match(r"^0(1\d{9})$", s_clean)
    if m_phone:
        return "phone", m_phone.group(1)
    if re.match(r"^25\d{12}$", s_clean):
        return "order", s_clean
    if re.match(r"^CU[\w\-\_]+$", s_clean, flags=re.I):
        return "seller", s_clean.upper()
    cu_tokens = re.findall(r"(CU[0-9A-Za-z\-\_]+)", s, flags=re.I)
    if cu_tokens:
        return "seller", cu_tokens[0].upper()
    if re.search(r"[Ee]", s_clean) or re.match(r"^\d+$", s_clean):
        try:
            val = safe_decimal_str(s_clean)
            if re.match(r"^25\d{12}$", val):
                return "order", val
        except Exception:
            pass
    return "unknown", s_clean

def search_smarter(records, raw_query):
    qtype, qvalue = detect_query_type(raw_query)
    by_order, by_seller = build_indices(records)
    results = []
    if qtype == "phone":
        q_digits = re.sub(r"\D", "", qvalue)
        for r in records:
            if q_digits and q_digits in r.get("wallet_number_normalized", ""):
                results.append(r)
        results = unique_by_key(results, lambda x: (x.get("order_number_norm"), x.get("sellerorderno_norm"), x.get("transaction_id_norm")))
        return qtype, results
    if qtype == "order":
        exact = by_order.get(qvalue)
        if exact:
            results = list(exact)
            results = unique_by_key(results, lambda x: (x.get("order_number_norm"), x.get("sellerorderno_norm"), x.get("transaction_id_norm")))
            return qtype, results
        matched = []
        for order_key, recs in by_order.items():
            if order_key and qvalue in order_key:
                matched.extend(recs)
        results = unique_by_key(matched, lambda x: (x.get("order_number_norm"), x.get("sellerorderno_norm"), x.get("transaction_id_norm")))
        return qtype, results
    if qtype == "seller":
        exact_seller = by_seller.get(qvalue)
        if exact_seller:
            collected = []
            for seller_row in exact_seller:
                order_key = seller_row.get("order_number_norm")
                if order_key:
                    collected.extend(by_order.get(order_key, []))
            results = unique_by_key(collected, lambda x: (x.get("order_number_norm"), x.get("sellerorderno_norm"), x.get("transaction_id_norm")))
            return qtype, results
        matched_sellers = []
        for seller_key, recs in by_seller.items():
            if seller_key and qvalue in seller_key:
                matched_sellers.extend(recs)
        collected = []
        for seller_row in matched_sellers:
            order_key = seller_row.get("order_number_norm")
            if order_key:
                collected.extend(by_order.get(order_key, []))
        results = unique_by_key(collected, lambda x: (x.get("order_number_norm"), x.get("sellerorderno_norm"), x.get("transaction_id_norm")))
        return qtype, results
    q_up = (qvalue or "").upper()
    q_digits = re.sub(r"\D", "", qvalue)
    matched = []
    for r in records:
        if (q_up and (q_up in r.get("order_number_norm", "") or q_up in r.get("sellerorderno_norm", ""))) or (q_digits and q_digits in r.get("wallet_number_normalized", "")):
            matched.append(r)
    results = unique_by_key(matched, lambda x: (x.get("order_number_norm"), x.get("sellerorderno_norm"), x.get("transaction_id_norm")))
    return "unknown", results

# --- GitHub-authenticated loader for private repo branch ---
_inmem_all = None
_inmem_all_at = 0.0

def load_all_from_github_api():
    """
    Load data/all.json from GitHub contents API on branch specified by GITHUB_DATA_BRANCH.
    Requires GITHUB_API_TOKEN, GITHUB_OWNER and GITHUB_REPO env vars to be set in Vercel.
    """
    global _inmem_all, _inmem_all_at
    if not (GITHUB_API_TOKEN and GITHUB_OWNER and GITHUB_REPO):
        logger.debug("GitHub API credentials not configured")
        return None
    now = time.time()
    if _inmem_all is not None and (now - _inmem_all_at) < INMEM_CACHE_TTL:
        return _inmem_all
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/data/all.json?ref={GITHUB_DATA_BRANCH}"
    headers = {"Authorization": f"Bearer {GITHUB_API_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    try:
        r = requests.get(url, headers=headers, timeout=8)
        r.raise_for_status()
        payload = r.json()
        if not payload:
            return None
        # content may be base64 encoded (standard contents API)
        content_b64 = payload.get("content")
        if content_b64:
            raw = base64.b64decode(content_b64).decode("utf-8")
            data = json.loads(raw)
        else:
            # fallback: sometimes API can return raw if Accept header set; try text
            data = json.loads(r.text)
        if isinstance(data, list):
            for rec in data:
                rec.setdefault("wallet_number_normalized", re.sub(r"\D", "", rec.get("wallet_number", "")))
                rec.setdefault("order_number_norm", (rec.get("order_number", "") or "").upper())
                rec.setdefault("sellerorderno_norm", (rec.get("sellerorderno", "") or "").upper())
                rec.setdefault("transaction_id_norm", (rec.get("transaction_id", "") or "").upper())
                rec.setdefault("source", rec.get("source", "Unknown"))
                dt = try_parse_date(rec.get("date", "") or rec.get("date_fmt", ""))
                rec["date_obj"] = dt
                rec["date_fmt"] = format_date_for_ui(dt) if dt else (rec.get("date_fmt") or rec.get("date", ""))
            _inmem_all = data
            _inmem_all_at = time.time()
            logger.info("Loaded all.json from GitHub API (branch=%s) size=%d", GITHUB_DATA_BRANCH, len(data))
            return data
    except Exception as e:
        logger.warning("Failed to load all.json from GitHub API: %s", e)
    return None

def load_all_from_local():
    p = Path("data") / "all.json"
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for rec in data:
                rec.setdefault("wallet_number_normalized", re.sub(r"\D", "", rec.get("wallet_number", "")))
                rec.setdefault("order_number_norm", (rec.get("order_number", "") or "").upper())
                rec.setdefault("sellerorderno_norm", (rec.get("sellerorderno", "") or "").upper())
                rec.setdefault("transaction_id_norm", (rec.get("transaction_id", "") or "").upper())
                rec.setdefault("source", rec.get("source", "Unknown"))
                dt = try_parse_date(rec.get("date", "") or rec.get("date_fmt", ""))
                rec["date_obj"] = dt
                rec["date_fmt"] = format_date_for_ui(dt) if dt else (rec.get("date_fmt") or rec.get("date", ""))
            logger.info("Loaded all.json from local data (size=%d)", len(data))
            return data
    except Exception as e:
        logger.warning("Failed to load local all.json: %s", e)
    return None

def get_all_records():
    data = load_all_from_github_api()
    if data is not None:
        return data
    data = load_all_from_local()
    if data is not None:
        return data
    logger.warning("No prebuilt all.json available (github api/local). Returning empty dataset.")
    return []

# --- routes (unchanged logic but search() now uses get_all_records) ---
@app.route("/", methods=["GET"])
def index_route():
    source_names = list(SOURCES.keys())
    return render_template("index.html", source_names=source_names)

@app.route("/search", methods=["GET"])
def search_route():
    query = request.args.get("query", "").strip()
    if not query:
        return render_template("results.html", found=False, message="Please enter a search query.", results=[], query=query, grouped_results=[], source_names=[])
    records = get_all_records()
    if not records:
        return render_template("results.html", found=False, message="No data available from any source. Try again later.", results=[], query=query, grouped_results=[], source_names=[])
    detected_type, matches = search_smarter(records, query)
    if not matches:
        return render_template("results.html", found=False, message="not refunded yet", results=[], query=query, detected_type=detected_type, grouped_results=[], source_names=[])
    # group results
    groups = OrderedDict()
    for r in matches:
        key = r.get("order_number_norm") or r.get("order_number") or "UNKNOWN"
        groups.setdefault(key, []).append(r)
    group_items = []
    for k,recs in groups.items():
        dates = [r.get("date_obj").timestamp() for r in recs if r.get("date_obj")]
        group_ts = max(dates) if dates else 0
        group_items.append((k,recs,group_ts))
    group_items_sorted = sorted(group_items, key=lambda x: x[2], reverse=True)
    grouped_results = []
    for k,recs,ts in group_items_sorted:
        srcs=[]
        for r in recs:
            s = r.get("source","Unknown")
            if s not in srcs:
                srcs.append(s)
        grouped_results.append({"order_number":k,"records":recs,"count":len(recs),"sources":srcs})
    return render_template("results.html", found=True, results=matches, query=query, grouped_results=grouped_results, source_names=list(SOURCES.keys()))

@app.route("/internal/refresh", methods=["POST"])
def internal_refresh():
    if CRON_SECRET:
        auth = request.headers.get("Authorization","")
        token = ""
        if auth.startswith("Bearer "):
            token = auth[len("Bearer "):].strip()
        else:
            token = request.form.get("token","")
        if token != CRON_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        recs = fetch_all_sources()
        return jsonify({"ok": True, "message": f"refreshed {len(recs)} records"})
    except Exception as e:
        app.logger.exception("Refresh failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500

# background sync (unchanged)
def background_sync_loop():
    while True:
        try:
            app.logger.info("Background sync: starting fetch_all_sources()")
            fetch_all_sources()
            app.logger.info("Background sync: done")
        except Exception:
            app.logger.exception("Background sync error")
        time.sleep(REFRESH_INTERVAL)

if __name__ == "__main__":
    if not DISABLE_BACKGROUND_SYNC:
        t = threading.Thread(target=background_sync_loop, daemon=True)
        t.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)), debug=os.getenv("FLASK_DEBUG","0")=="1")

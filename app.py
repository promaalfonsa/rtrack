from flask import Flask, render_template, request, url_for
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import io
import re
import time
import json
from collections import defaultdict, OrderedDict
from decimal import Decimal, InvalidOperation
from pathlib import Path
import tempfile
import shutil
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading

app = Flask(__name__, static_folder="static", template_folder="templates")

# Register a Jinja filter to format timestamps for templates
def timestamp_to_string(ts):
    try:
        if not ts:
            return ""
        # ts may be float or int or string
        t = int(ts)
        dt = datetime.fromtimestamp(t)
        return dt.strftime("%d %b %Y %H:%M")
    except Exception:
        return str(ts)

app.jinja_env.filters['timestamp_to_string'] = timestamp_to_string

# --- Configuration ---
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

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CACHE_TTL = 600  # seconds (10 minutes)
REQUEST_TIMEOUT = 8  # seconds
MAX_RETRIES = 2
BACKOFF_FACTOR = 0.5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Map possible header names to canonical keys
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

# Background refresh pool and guard
_REFRESH_EXECUTOR = ThreadPoolExecutor(max_workers=4)
_refresh_in_progress = set()
_refresh_lock = threading.Lock()


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
    formats = [
        "%m/%d/%Y", "%m/%d/%y",
        "%m-%d-%Y", "%m-%d-%y",
        "%Y-%m-%d",
        "%d/%m/%Y", "%d-%m-%Y",
        "%b %d %Y", "%d %b %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s_clean, fmt)
        except Exception:
            pass
    m = re.search(r"(\d{1,2})[^\d](\d{1,2})[^\d](\d{2,4})", s_clean)
    if m:
        g1, g2, g3 = m.groups()
        if len(g3) == 2:
            year = int(g3) + (2000 if int(g3) < 70 else 1900)
        else:
            year = int(g3)
        for (a, b) in ((g1, g2), (g2, g1)):
            try:
                return datetime(year, int(a), int(b))
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
        row = list(r) + [""] * max(0, len(headers) - len(r))
        rec = {}
        for i, key in enumerate(headers):
            rec[key] = row[i].strip()
        rec["order_number"] = safe_decimal_str(rec.get("order_number", "") or "")
        rec["sellerorderno"] = (rec.get("sellerorderno", "") or "").strip()
        rec["payment_method"] = (rec.get("payment_method", "") or "").strip()
        rec["wallet_number"] = (rec.get("wallet_number", "") or "").strip()
        rec["card_number"] = (rec.get("card_number", "") or "").strip()
        rec["wallet_number_normalized"] = re.sub(r"\D", "", rec["wallet_number"])
        rec["transaction_id"] = (rec.get("transaction_id", "") or "").strip()
        rec["amount"] = (rec.get("amount", "") or "").strip()
        rec["cashback"] = (rec.get("cashback", "") or "").strip()
        rec["date"] = (rec.get("date", "") or "").strip()

        dt = try_parse_date(rec["date"])
        rec["date_obj"] = dt
        rec["date_fmt"] = format_date_for_ui(dt) if dt else rec["date"]

        rec["order_number_norm"] = rec["order_number"].upper()
        rec["sellerorderno_norm"] = rec["sellerorderno"].upper()
        rec["transaction_id_norm"] = rec["transaction_id"].upper()
        rec["source"] = source_name
        records.append(rec)
    return records


def cache_paths_for_source(source_name):
    safe_name = re.sub(r"[^\w\-]", "_", source_name).lower()
    csv_path = DATA_DIR / f"refunds_{safe_name}.csv"
    json_path = DATA_DIR / f"refunds_{safe_name}.json"
    return csv_path, json_path


def _make_json_safe(records):
    safe = []
    for r in records:
        rec = {}
        for k, v in r.items():
            if k == "date_obj":
                continue
            if isinstance(v, datetime):
                rec[k] = v.isoformat()
                continue
            if v is None or isinstance(v, (str, int, float, bool, list, dict)):
                rec[k] = v
                continue
            try:
                json.dumps({k: v})
                rec[k] = v
            except Exception:
                rec[k] = str(v)
        safe.append(rec)
    return safe


def save_cache_for_source(source_name, csv_text, records):
    csv_path, json_path = cache_paths_for_source(source_name)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # atomic write for CSV
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="") as tmp:
        tmp.write(csv_text)
        tmp_path = Path(tmp.name)
    shutil.move(str(tmp_path), str(csv_path))
    safe_records = _make_json_safe(records)
    try:
        with open(json_path, "w", encoding="utf-8") as jf:
            json.dump(safe_records, jf, ensure_ascii=False, indent=2)
    except Exception as ex:
        logger.warning("Failed to write JSON cache for %s: %s", source_name, ex)


def load_cache_for_source(source_name):
    csv_path, json_path = cache_paths_for_source(source_name)
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as jf:
                records = json.load(jf)
                for rec in records:
                    rec.setdefault("wallet_number_normalized", re.sub(r"\D", "", rec.get("wallet_number", "")))
                    rec.setdefault("order_number_norm", (rec.get("order_number", "") or "").upper())
                    rec.setdefault("sellerorderno_norm", (rec.get("sellerorderno", "") or "").upper())
                    rec.setdefault("transaction_id_norm", (rec.get("transaction_id", "") or "").upper())
                    rec.setdefault("source", source_name)
                    dt = try_parse_date(rec.get("date", "") or rec.get("date_fmt", ""))
                    rec["date_obj"] = dt
                    rec["date_fmt"] = format_date_for_ui(dt) if dt else (rec.get("date_fmt") or rec.get("date", ""))
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
    """
    Stale-while-revalidate:
      - If cache exists and fresh -> return it
      - If cache exists and stale -> return cached immediately, schedule background refresh
      - If no cache -> blocking fetch (first-run) with small timeout and fallback to empty
    """
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
            # fall through to blocking fetch
    # No cache available -> blocking fetch
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


def get_cache_mtime(source_name):
    csv_path, _ = cache_paths_for_source(source_name)
    if csv_path.exists():
        return csv_path.stat().st_mtime
    return None


# --- routes ---

@app.route("/", methods=["GET"])
def index():
    source_names = list(SOURCES.keys())
    return render_template("index.html", source_names=source_names)


@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("query", "").strip()
    sort_by = request.args.get("sort", "date")
    order = request.args.get("order", "desc")
    source_names = list(SOURCES.keys())

    if not query:
        return render_template("results.html", found=False, message="Please enter a search query.", results=[], query=query, sources=[], has_tx=False, has_card=False, has_cashback=False, has_wallet=False, sort_by=sort_by, order=order, grouped_results=[], source_names=source_names)

    # search across all sources (will use stale-while-revalidate to avoid long waits)
    records = fetch_all_sources()
    if not records:
        return render_template("results.html", found=False, message="No data available from any source.", results=[], query=query, sources=[], has_tx=False, has_card=False, has_cashback=False, has_wallet=False, sort_by=sort_by, order=order, grouped_results=[], source_names=source_names)

    detected_type, matches = search_smarter(records, query)
    if not matches:
        return render_template("results.html", found=False, message="not refunded yet", results=[], query=query, detected_type=detected_type, sources=[], has_tx=False, has_card=False, has_cashback=False, has_wallet=False, sort_by=sort_by, order=order, grouped_results=[], source_names=source_names)

    sources = sorted({r.get("source", "Unknown") for r in matches})
    has_tx = any(r.get("transaction_id") for r in matches)
    has_card = any(r.get("card_number") for r in matches)
    has_cashback = any(r.get("cashback") for r in matches)
    has_wallet = any(r.get("wallet_number") for r in matches)

    def sort_key_record(r):
        dt = r.get("date_obj")
        ts = dt.timestamp() if dt else 0
        return ts

    matches_sorted = sorted(matches, key=sort_key_record, reverse=(order == "desc"))

    groups = OrderedDict()
    for r in matches_sorted:
        key = r.get("order_number_norm") or r.get("order_number") or "UNKNOWN"
        groups.setdefault(key, []).append(r)

    group_items = []
    for k, recs in groups.items():
        dates = [r.get("date_obj").timestamp() for r in recs if r.get("date_obj")]
        group_ts = max(dates) if dates else 0
        group_items.append((k, recs, group_ts))
    group_items_sorted = sorted(group_items, key=lambda x: x[2], reverse=(order == "desc"))

    grouped_results = []
    for k, recs, ts in group_items_sorted:
        srcs = []
        for r in recs:
            s = r.get("source", "Unknown")
            if s not in srcs:
                srcs.append(s)
        grouped_results.append({
            "order_number": k,
            "records": recs,
            "count": len(recs),
            "sources": srcs
        })

    return render_template(
        "results.html",
        found=True,
        results=matches_sorted,
        query=query,
        sources=sources,
        has_tx=has_tx,
        has_card=has_card,
        has_cashback=has_cashback,
        has_wallet=has_wallet,
        sort_by=sort_by,
        order=order,
        grouped_results=grouped_results,
        source_names=source_names,
    )


@app.route("/sources", methods=["GET"])
def sources_view():
    source_names = list(SOURCES.keys())
    active = request.args.get("source") or source_names[0]
    query = request.args.get("query", "").strip()

    # server-side search limited to one source
    if query:
        recs = fetch_source_if_needed(active, SOURCES[active])
        _, matches = search_smarter(recs, query)
        def sort_key_record(r):
            dt = r.get("date_obj")
            return dt.timestamp() if dt else 0
        matches_sorted = sorted(matches, key=sort_key_record, reverse=True)
        groups = OrderedDict()
        for r in matches_sorted:
            key = r.get("order_number_norm") or r.get("order_number") or "UNKNOWN"
            groups.setdefault(key, []).append(r)
        group_items = []
        for k, recs in groups.items():
            dates = [r.get("date_obj").timestamp() for r in recs if r.get("date_obj")]
            group_ts = max(dates) if dates else 0
            group_items.append((k, recs, group_ts))
        group_items_sorted = sorted(group_items, key=lambda x: x[2], reverse=True)
        grouped_results = []
        for k, recs, ts in group_items_sorted:
            grouped_results.append({
                "order_number": k,
                "records": recs,
                "count": len(recs),
            })
        # Build a minimal grouped_by_source to render exactly the active source
        last_updated = {active: get_cache_mtime(active)}
        return render_template("sources.html", grouped_by_source={active: grouped_results}, source_names=source_names, active_source=active, last_updated=last_updated, query=query)

    # No query: build grouped inspection view for all sources
    grouped_by_source = OrderedDict()
    last_updated = {}
    for source_name in source_names:
        recs = fetch_source_if_needed(source_name, SOURCES[source_name])
        for r in recs:
            if "date_fmt" not in r or not r.get("date_fmt"):
                dt = try_parse_date(r.get("date", ""))
                r["date_obj"] = dt
                r["date_fmt"] = format_date_for_ui(dt) if dt else r.get("date", "")
        # group by order_number_norm and sort groups by latest date desc
        groups = OrderedDict()
        recs_sorted = sorted(recs, key=lambda x: x.get("date_obj").timestamp() if x.get("date_obj") else 0, reverse=True)
        for r in recs_sorted:
            key = r.get("order_number_norm") or r.get("order_number") or "UNKNOWN"
            groups.setdefault(key, []).append(r)
        group_items = []
        for k, rec_list in groups.items():
            dates = [rr.get("date_obj").timestamp() for rr in rec_list if rr.get("date_obj")]
            group_ts = max(dates) if dates else 0
            group_items.append((k, rec_list, group_ts))
        group_items_sorted = sorted(group_items, key=lambda x: x[2], reverse=True)
        group_dicts = []
        for k, rec_list, ts in group_items_sorted:
            group_dicts.append({
                "order_number": k,
                "records": rec_list,
                "count": len(rec_list),
            })
        grouped_by_source[source_name] = group_dicts
        last_updated[source_name] = get_cache_mtime(source_name)

    return render_template("sources.html", grouped_by_source=grouped_by_source, source_names=source_names, active_source=active, last_updated=last_updated)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
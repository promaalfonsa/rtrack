#!/usr/bin/env python3
"""
Fetch CSV sources, parse to records, write JSON files under data/.
This script is used by GitHub Actions (update-data workflow) to update data/*.json.
Fixed: avoid backslashes in f-string expressions by computing safe_name separately.
"""
import requests
import csv
import io
import json
import os
import re
from datetime import datetime

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

DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)

def normalize_header(raw):
    if raw is None:
        return ""
    h = raw.strip().lower()
    mapping = {
        "date":"date", "order number":"order_number", "ordernumber":"order_number", "order_no":"order_number",
        "order":"order_number", "sellerorderno":"sellerorderno", "payment method":"payment_method",
        "wallet number":"wallet_number", "phone":"wallet_number", "card number":"card_number",
        "amount":"amount", "transaction id":"transaction_id", "cashback":"cashback"
    }
    return mapping.get(h, h.replace(" ", "_"))

def parse_csv_text(text, source_name):
    f = io.StringIO(text)
    reader = csv.reader(f)
    rows = list(reader)
    if not rows:
        return []
    headers = [normalize_header(h) for h in rows[0]]
    records = []
    for r in rows[1:]:
        if not any(cell.strip() for cell in r):
            continue
        row = list(r) + [""] * max(0, len(headers) - len(r))
        rec = {}
        for i, h in enumerate(headers):
            rec[h] = row[i].strip()
        rec["source"] = source_name
        rec["wallet_number_normalized"] = re.sub(r"\D", "", rec.get("wallet_number","") or "")
        rec["order_number_norm"] = (rec.get("order_number","") or "").upper()
        rec["sellerorderno_norm"] = (rec.get("sellerorderno","") or "").upper()
        rec["transaction_id_norm"] = (rec.get("transaction_id","") or "").upper()
        records.append(rec)
    return records

def fetch_and_write():
    all_records = []
    for name, url in SOURCES.items():
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            text = r.content.decode("utf-8", errors="replace")
            recs = parse_csv_text(text, name)
            all_records.extend(recs)
            # compute safe file name without backslashes inside f-string expression
            safe_name = re.sub(r"[^\w\-]", "_", name).lower()
            fname = os.path.join(DATA_DIR, f"{safe_name}.json")
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(recs, f, ensure_ascii=False)
            print(f"Wrote {len(recs)} records for {name} -> {fname}")
        except Exception as e:
            print("Failed to fetch", name, e)
    combined_path = os.path.join(DATA_DIR, "all.json")
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False)
    print("Wrote combined all.json with", len(all_records), "records")

if __name__ == "__main__":
    fetch_and_write()

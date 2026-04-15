"""
backend.py — Data access layer for ChartSearch.

Configuration via environment variables (set in app.yaml):
  CHARTSEARCH_CATALOG     — Unity Catalog catalog name  (default: main)
  CHARTSEARCH_SCHEMA      — Schema name                 (default: chart_search_gold)
  CHARTSEARCH_TABLE       — Table name                  (default: crew_data_dummy)
  DATABRICKS_WAREHOUSE_ID — SQL warehouse ID (injected via app.yaml)

Fuzzy matching strategy (when fuzzy=True in search_records):
  - SQL handles all exact/date/range/vendor filters as normal
  - Fuzzy text fields (first_name, last_name, chart_name, project_name) are
    excluded from the SQL WHERE clause and instead post-filtered in Python
    using rapidfuzz.fuzz.partial_ratio — this correctly handles partial
    substring matches (e.g. "Care Gab" → "Care Gap Analysis 2023")
  - Threshold: 80 out of 100 (adjust FUZZY_THRESHOLD below)
"""

import os
from rapidfuzz import fuzz
from databricks.sdk.core import Config
from databricks import sql

# ─── Configuration ────────────────────────────────────────────────────────────
CATALOG    = os.getenv("CHARTSEARCH_CATALOG", "main")
SCHEMA     = os.getenv("CHARTSEARCH_SCHEMA",  "chart_search_gold")
TABLE      = os.getenv("CHARTSEARCH_TABLE",   "crew_data_dummy")
FULL_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"

# Minimum fuzzy score (0–100). 80 = tolerates ~1-2 character typos in partial matches.
FUZZY_THRESHOLD = 80

# Fields handled by Python-side fuzzy matching (excluded from SQL when fuzzy=True)
FUZZY_FIELDS = {"first_name", "last_name", "chart_name", "project_name"}


def _get_conn():
    cfg = Config()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


def get_vendors() -> list[str]:
    """Return ['ALL'] + sorted distinct vendor values from the table."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT vendor FROM {FULL_TABLE} ORDER BY vendor")
            rows = cur.fetchall()
    return ["ALL"] + [r[0] for r in rows if r[0]]


def _fuzzy_filter(records: list[dict], filters: dict) -> list[dict]:
    """
    Post-filter records using Python-side partial fuzzy matching.

    Uses rapidfuzz.fuzz.partial_ratio which finds the best matching
    substring window — so "Care Gab" correctly matches "Care Gap Analysis 2023"
    because it compares against the best 8-char window in the column value.

    Score range: 0–100. Records below FUZZY_THRESHOLD are excluded.
    """
    active = {
        col: filters[col].strip().lower()
        for col in FUZZY_FIELDS
        if filters.get(col) and filters[col].strip()
    }
    if not active:
        return records

    result = []
    for row in records:
        keep = True
        for col, search_val in active.items():
            col_val = str(row.get(col) or "").lower()
            score = fuzz.partial_ratio(search_val, col_val)
            if score < FUZZY_THRESHOLD:
                keep = False
                break
        if keep:
            result.append(row)
    return result


def search_records(filters: dict, fuzzy: bool = False) -> list[dict]:
    """
    Query crew_data_dummy with active filters.

    When fuzzy=False:
      All fields are filtered in SQL (exact / prefix / contains / date / range).

    When fuzzy=True:
      - first_name, last_name, chart_name, project_name are SKIPPED in SQL.
      - SQL still applies all other filters (IDs, vendor, dates, NPI).
      - Python then scores the SQL results with rapidfuzz.fuzz.partial_ratio
        and keeps rows above FUZZY_THRESHOLD.

    Non-fuzzy filter modes:
      member_card_id   → prefix  (LIKE 'val%')
      individual_id    → exact
      chart_name       → contains
      chart_request_id → exact
      first_name       → case-insensitive exact
      last_name        → case-insensitive exact
      dob              → exact date
      vendor           → exact (skipped when 'ALL')
      npi_id           → exact
      dos_start_date   → >= value
      dos_end_date     → <= value
      project_name     → contains
    """
    clauses: list[str] = []
    params:  list      = []

    def _val(v) -> str | None:
        return str(v).strip() if v and str(v).strip() else None

    def _add(col: str, raw_val, mode: str):
        # In fuzzy mode, skip SQL clauses for fields handled by Python
        if fuzzy and col in FUZZY_FIELDS:
            return

        v = _val(raw_val)
        if not v:
            return

        if mode == "exact":
            clauses.append(f"CAST({col} AS STRING) = ?")
            params.append(v)
        elif mode == "prefix":
            clauses.append(f"CAST({col} AS STRING) LIKE ?")
            params.append(v + "%")
        elif mode == "contains":
            clauses.append(f"LOWER(CAST({col} AS STRING)) LIKE ?")
            params.append(f"%{v.lower()}%")
        elif mode == "iexact":
            clauses.append(f"LOWER(CAST({col} AS STRING)) = ?")
            params.append(v.lower())
        elif mode == "date":
            clauses.append(f"CAST({col} AS STRING) = ?")
            params.append(v)
        elif mode == "gte":
            clauses.append(f"{col} >= CAST(? AS DATE)")
            params.append(v)
        elif mode == "lte":
            clauses.append(f"{col} <= CAST(? AS DATE)")
            params.append(v)

    _add("member_card_id",    filters.get("member_card_id"),    "prefix")
    _add("individual_id",     filters.get("individual_id"),     "exact")
    _add("chart_name",        filters.get("chart_name"),        "contains")
    _add("chart_request_id",  filters.get("chart_request_id"),  "exact")
    _add("first_name",        filters.get("first_name"),        "iexact")
    _add("last_name",         filters.get("last_name"),         "iexact")
    _add("dob",               filters.get("dob"),               "date")
    _add("npi_id",            filters.get("npi_id"),            "exact")
    _add("dos_start_date",    filters.get("dos_start_date"),    "gte")
    _add("dos_end_date",      filters.get("dos_end_date"),      "lte")
    _add("project_name",      filters.get("project_name"),      "contains")

    vendor = (filters.get("vendor") or "ALL").strip()
    if vendor and vendor != "ALL":
        clauses.append("vendor = ?")
        params.append(vendor)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    query = f"""
        SELECT
            member_card_id,
            individual_id,
            first_name,
            last_name,
            CAST(dob            AS STRING) AS dob,
            vendor,
            npi_id,
            CAST(dos_start_date AS STRING) AS dos_start_date,
            CAST(dos_end_date   AS STRING) AS dos_end_date,
            project_name,
            chart_name,
            chart_request_id
        FROM {FULL_TABLE}
        {where}
        ORDER BY last_name, first_name
        LIMIT 5000
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            cols = [d[0] for d in cur.description]
            records = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Apply Python-side fuzzy filtering for text fields
    if fuzzy:
        records = _fuzzy_filter(records, filters)

    return records

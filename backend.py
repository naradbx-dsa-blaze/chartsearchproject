"""
backend.py — Data access layer for ChartSearch demo.

Configuration via environment variables (set in app.yaml):
  CHARTSEARCH_CATALOG   — Unity Catalog catalog name  (default: main)
  CHARTSEARCH_SCHEMA    — Schema name                 (default: chart_search_gold)
  CHARTSEARCH_TABLE     — Table name                  (default: crew_data_dummy)
  DATABRICKS_WAREHOUSE_ID — SQL warehouse ID (injected automatically via app.yaml valueFrom)
"""

import os
from databricks.sdk.core import Config
from databricks import sql

# ─── Configuration ────────────────────────────────────────────────────────────
CATALOG    = os.getenv("CHARTSEARCH_CATALOG", "main")
SCHEMA     = os.getenv("CHARTSEARCH_SCHEMA",  "chart_search_gold")
TABLE      = os.getenv("CHARTSEARCH_TABLE",   "crew_data_dummy")
FULL_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`{TABLE}`"


def _get_conn():
    """Create a new SQL warehouse connection using SDK auth (service principal)."""
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


def search_records(filters: dict) -> list[dict]:
    """
    Run a parameterized query against crew_data_dummy with active filters.

    Filter modes:
      member_card_id   → prefix match (LIKE 'val%')
      individual_id    → exact
      chart_name       → case-insensitive contains
      chart_request_id → exact
      first_name       → case-insensitive exact
      last_name        → case-insensitive exact
      dob              → exact date string 'YYYY-MM-DD'
      vendor           → exact (skipped when 'ALL')
      npi_id           → exact
      dos_start_date   → dos_start_date >= value
      dos_end_date     → dos_end_date   <= value
      project_name     → case-insensitive contains
    """
    clauses: list[str] = []
    params:  list      = []

    def _val(v) -> str | None:
        return str(v).strip() if v and str(v).strip() else None

    def _add(col: str, raw_val, mode: str):
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

    # Vendor: dropdown value is already a clean string or "ALL"
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
            return [dict(zip(cols, row)) for row in cur.fetchall()]

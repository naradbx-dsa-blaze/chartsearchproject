# ChartSearch — CVS RI Team

A Databricks App that replicates the Aetna **ChartSearch** UI for the CVS RI (Risk Intelligence) team. Backed by a Unity Catalog Delta table with fully synthetic data — no PHI or PII.

---

## Overview

ChartSearch lets RI team members search and filter member chart records across key dimensions: member identity, vendor, date of service, NPI, and project. Results are displayed in a paginated, sortable grid matching the original ChartSearch look and feel.

---

## Project Structure

```
chartsearchproject/
├── app.py           # Dash app — layout, filter bar, callbacks, result table
├── backend.py       # Data layer — parameterized SQL warehouse queries
├── theme.py         # Color palette and layout constants (edit here to restyle)
├── requirements.txt # Python dependencies
├── app.yaml         # Databricks App config (command, env vars, warehouse)
└── README.md
```

---

## Data

| Property | Value |
|---|---|
| Catalog | `main` |
| Schema | `chart_search_gold` |
| Table | `crew_data_dummy` |
| Rows | 500 synthetic records |
| Vendors | Optum, Ciox Health, MedAssets, DST Health Solutions, Datavant |

All data is randomly generated — safe for demos and development.

---

## Search Fields

| Field | Filter Behavior |
|---|---|
| Member Card ID | Prefix match |
| Individual ID | Exact match |
| Chart Name | Case-insensitive contains |
| Chart Request ID | Exact match |
| First Name | Case-insensitive exact |
| Last Name | Case-insensitive exact |
| DOB | Exact date |
| Vendor | Dropdown — exact match or ALL |
| NPI ID | Exact match |
| DOS Range Start | `dos_start_date >=` value |
| DOS Range End | `dos_end_date <=` value |
| Project Name | Case-insensitive contains |

---

## Configuration

### Change catalog / schema / table

Edit `app.yaml`:

```yaml
env:
  - name: CHARTSEARCH_CATALOG
    value: "main"          # change to your catalog
  - name: CHARTSEARCH_SCHEMA
    value: "chart_search_gold"   # change to your schema
  - name: CHARTSEARCH_TABLE
    value: "crew_data_dummy"     # change to your table
  - name: DATABRICKS_WAREHOUSE_ID
    value: "d4ef05c5632d476b"    # change to your warehouse ID
```

### Change colors / theme

All colors are in `theme.py` under the `COLORS` dict — named by intent:

```python
COLORS = {
    "navDark":           "#2C3E50",   # left nav background
    "primaryHeaderGrey": "#D1D1D1",   # filter bar background
    "accentTeal":        "#4DB8B8",   # teal stripe + active nav item
    "btnSearch":         "#3A9D5D",   # Search button (green)
    "btnClear":          "#607D8B",   # Clear button (blue-gray)
    "tableHeaderBg":     "#4A90A4",   # result table header
    # ... see theme.py for full list
}
```

---

## Deployment

### Prerequisites
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and configured
- Access to the `e2-demo-field-eng` workspace

### Deploy (first time)

```bash
# 1. Create the app
databricks apps create chartsearchproject

# 2. Upload source code
databricks workspace import-dir . \
  /Workspace/Users/<your-email>/apps/chartsearchproject \
  --overwrite

# 3. Deploy
databricks apps deploy chartsearchproject \
  --source-code-path /Workspace/Users/<your-email>/apps/chartsearchproject
```

### Redeploy after changes

```bash
databricks workspace import-dir . \
  /Workspace/Users/<your-email>/apps/chartsearchproject \
  --overwrite

databricks apps deploy chartsearchproject \
  --source-code-path /Workspace/Users/<your-email>/apps/chartsearchproject
```

### Check logs

```bash
databricks apps logs chartsearchproject
databricks apps get chartsearchproject
```

---

## Live App

| Property | Value |
|---|---|
| URL | https://chartsearch-demo-1444828305810485.aws.databricksapps.com |
| Status | Active |
| Warehouse | `d4ef05c5632d476b` |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | [Dash](https://dash.plotly.com/) + [dash-bootstrap-components](https://dash-bootstrap-components.opensource.faculty.ai/) |
| Backend | Python + `databricks-sql-connector` |
| Auth | Databricks SDK `Config()` — service principal |
| Data | Delta table on Unity Catalog |
| Platform | Databricks Apps (serverless) |

---

## Team

CVS RI (Risk Intelligence) Team

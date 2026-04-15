"""
app.py — ChartSearch Demo  (Dash)

Visual replica of the Aetna ChartSearch UI backed by a Databricks SQL warehouse.
All styling is driven by theme.py; all data access goes through backend.py.
"""

import os

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, dash_table, dcc, html

from backend import get_vendors, search_records
from theme import COLORS, FONT_FAMILY, NAV_WIDTH

# ─── App Init ─────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        # Bootstrap Icons (search icon in nav + header)
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css",
    ],
    suppress_callback_exceptions=True,
    title="ChartSearch",
)
server = app.server  # expose for Gunicorn if needed

# Pre-load vendor list at startup (cached in module scope)
try:
    _VENDORS = get_vendors()
except Exception as exc:
    print(f"[WARN] Could not load vendors: {exc}")
    _VENDORS = ["ALL"]

# ─── Column definitions for result DataTable ──────────────────────────────────
RESULT_COLUMNS = [
    {"name": "Member Card ID",    "id": "member_card_id"},
    {"name": "Individual ID",     "id": "individual_id"},
    {"name": "First Name",        "id": "first_name"},
    {"name": "Last Name",         "id": "last_name"},
    {"name": "DOB",               "id": "dob"},
    {"name": "Vendor",            "id": "vendor"},
    {"name": "NPI ID",            "id": "npi_id"},
    {"name": "DOS Start",         "id": "dos_start_date"},
    {"name": "DOS End",           "id": "dos_end_date"},
    {"name": "Project Name",      "id": "project_name"},
    {"name": "Chart Name",        "id": "chart_name"},
    {"name": "Chart Request ID",  "id": "chart_request_id"},
]

# ─── Shared style helpers ─────────────────────────────────────────────────────
_INPUT_BASE = {
    "backgroundColor": COLORS["filterInputBg"],
    "border":          f"1px solid {COLORS['filterBorder']}",
    "borderRadius":    "3px",
    "padding":         "5px 8px",
    "fontSize":        "12px",
    "width":           "100%",
    "height":          "32px",
    "boxSizing":       "border-box",
    "fontFamily":      FONT_FAMILY,
    "color":           "#333",
    "outline":         "none",
}
_LABEL_STYLE = {
    "fontSize":      "10px",
    "fontWeight":    "700",
    "color":         COLORS["filterLabelText"],
    "marginBottom":  "3px",
    "display":       "block",
    "textTransform": "uppercase",
    "letterSpacing": "0.5px",
    "whiteSpace":    "nowrap",
}
_BTN_BASE = {
    "height":       "34px",
    "fontSize":     "13px",
    "fontWeight":   "600",
    "border":       "none",
    "borderRadius": "3px",
    "padding":      "0 18px",
    "cursor":       "pointer",
    "fontFamily":   FONT_FAMILY,
}


def _text_input(fid, placeholder=""):
    return dcc.Input(
        id=fid,
        type="text",
        placeholder=placeholder,
        debounce=False,
        style=_INPUT_BASE,
    )


def _date_input(fid):
    return dcc.Input(
        id=fid,
        type="date",
        style={**_INPUT_BASE, "cursor": "pointer", "paddingRight": "4px"},
    )


def _filter_col(label, child, width=3):
    return dbc.Col(
        [html.Label(label, style=_LABEL_STYLE), child],
        xs=12, sm=6, md=width,
        style={"padding": "4px 6px"},
    )


# ─── Left Navigation Bar ──────────────────────────────────────────────────────
left_nav = html.Div(
    style={
        "width":           NAV_WIDTH,
        "minWidth":        NAV_WIDTH,
        "backgroundColor": COLORS["navDark"],
        "color":           COLORS["navText"],
        "height":          "100vh",
        "display":         "flex",
        "flexDirection":   "column",
        "overflowY":       "auto",
        "flexShrink":      "0",
    },
    children=[
        # ── Logo / title ───────────────────────────────────────────────────
        html.Div(
            style={
                "backgroundColor": COLORS["navDarker"],
                "padding":         "14px 12px",
                "borderBottom":    f"1px solid {COLORS['navBorder']}",
            },
            children=[
                html.Div("CHART", style={
                    "fontSize":      "17px",
                    "fontWeight":    "900",
                    "letterSpacing": "2px",
                    "color":         "#FFFFFF",
                    "lineHeight":    "1",
                }),
                html.Div("SEARCH", style={
                    "fontSize":      "17px",
                    "fontWeight":    "900",
                    "letterSpacing": "2px",
                    "color":         COLORS["accentTeal"],
                    "lineHeight":    "1",
                    "marginTop":     "1px",
                }),
                html.Div("Demo App", style={
                    "fontSize":   "10px",
                    "color":      COLORS["navSubText"],
                    "marginTop":  "4px",
                }),
            ],
        ),
        # ── Active nav item ────────────────────────────────────────────────
        html.Div(
            style={
                "backgroundColor": COLORS["navActiveItem"],
                "padding":         "13px 14px",
                "display":         "flex",
                "alignItems":      "center",
                "cursor":          "pointer",
                "borderLeft":      f"4px solid {COLORS['navActiveBorder']}",
                "marginTop":       "4px",
            },
            children=[
                html.I(
                    className="bi bi-search",
                    style={"marginRight": "10px", "fontSize": "15px"},
                ),
                html.Span("ChartSearch", style={"fontSize": "13px", "fontWeight": "700"}),
            ],
        ),
        # ── Placeholder nav items (for visual fidelity) ────────────────────
        *[
            html.Div(
                label,
                style={
                    "padding":    "11px 18px",
                    "fontSize":   "12px",
                    "color":      COLORS["navSubText"],
                    "cursor":     "pointer",
                    "borderLeft": "4px solid transparent",
                },
            )
            for label in ["Dashboard", "Reports", "Settings", "Help"]
        ],
    ],
)

# ─── Filter Bar ───────────────────────────────────────────────────────────────
filter_bar = html.Div(
    style={
        "backgroundColor": COLORS["primaryHeaderGrey"],
        "padding":         "10px 12px 6px 12px",
        "borderBottom":    f"3px solid {COLORS['accentTeal']}",
        "flexShrink":      "0",
    },
    children=[
        # Row 1 ──────────────────────────────────────────────────────────────
        dbc.Row([
            _filter_col("Member Card ID",   _text_input("f-member-card-id")),
            _filter_col("Individual ID",    _text_input("f-individual-id")),
            _filter_col("Chart Name",       _text_input("f-chart-name")),
            _filter_col("Chart Request ID", _text_input("f-chart-request-id")),
        ], className="mb-1 gx-0"),

        # Row 2 ──────────────────────────────────────────────────────────────
        dbc.Row([
            _filter_col("First Name", _text_input("f-first-name")),
            _filter_col("Last Name",  _text_input("f-last-name")),
            _filter_col("DOB",        _date_input("f-dob")),
            _filter_col("Vendor", dcc.Dropdown(
                id="f-vendor",
                options=[{"label": v, "value": v} for v in _VENDORS],
                value="ALL",
                clearable=False,
                style={
                    "fontSize":    "12px",
                    "minHeight":   "32px",
                    "fontFamily":  FONT_FAMILY,
                },
            )),
        ], className="mb-1 gx-0"),

        # Row 3 ──────────────────────────────────────────────────────────────
        dbc.Row([
            _filter_col("NPI ID",          _text_input("f-npi-id")),
            _filter_col("DOS Range Start",  _date_input("f-dos-start")),
            _filter_col("DOS Range End",    _date_input("f-dos-end")),
            _filter_col("Project Name",     _text_input("f-project-name")),
        ], className="mb-2 gx-0"),

        # Buttons + Fuzzy toggle ──────────────────────────────────────────────
        dbc.Row([
            dbc.Col(
                html.Button(
                    [html.I(className="bi bi-search me-1"), "Search"],
                    id="btn-search",
                    n_clicks=0,
                    style={
                        **_BTN_BASE,
                        "backgroundColor": COLORS["btnSearch"],
                        "color":           COLORS["btnSearchText"],
                        "marginRight":     "8px",
                    },
                ),
                width="auto",
                style={"padding": "0 6px"},
            ),
            dbc.Col(
                html.Button(
                    "Clear",
                    id="btn-clear",
                    n_clicks=0,
                    style={
                        **_BTN_BASE,
                        "backgroundColor": COLORS["btnClear"],
                        "color":           COLORS["btnClearText"],
                    },
                ),
                width="auto",
                style={"padding": "0 6px"},
            ),
            # ── Fuzzy Search toggle ───────────────────────────────────────────
            dbc.Col(
                html.Div(
                    [
                        dcc.Checklist(
                            id="fuzzy-toggle",
                            options=[{"label": "", "value": "fuzzy"}],
                            value=[],
                            style={"display": "inline-block", "marginRight": "5px"},
                            inputStyle={
                                "cursor":       "pointer",
                                "width":        "14px",
                                "height":       "14px",
                                "accentColor":  COLORS["accentTeal"],
                            },
                        ),
                        html.Span(
                            "Fuzzy Search",
                            style={
                                "fontSize":    "12px",
                                "fontWeight":  "600",
                                "color":       "#444",
                                "marginRight": "4px",
                            },
                        ),
                        html.Span(
                            "~",
                            title=(
                                "Fuzzy mode: tolerates minor spelling mistakes in "
                                "First Name, Last Name, Chart Name, and Project Name. "
                                "Uses Levenshtein distance (≤2 edits for names, ≤3 for text fields)."
                            ),
                            style={
                                "fontSize":     "12px",
                                "color":        COLORS["accentTeal"],
                                "fontWeight":   "700",
                                "cursor":       "help",
                                "borderBottom": f"1px dotted {COLORS['accentTeal']}",
                            },
                        ),
                    ],
                    style={
                        "display":     "flex",
                        "alignItems":  "center",
                        "marginLeft":  "12px",
                        "padding":     "0 6px",
                    },
                ),
                width="auto",
                style={"padding": "0"},
            ),
        ], className="gx-0", align="center"),
    ],
)

# ─── Results Area ─────────────────────────────────────────────────────────────
results_area = html.Div(
    style={"flex": "1", "overflowY": "auto", "display": "flex", "flexDirection": "column"},
    children=[
        # Teal accent strip
        html.Div(
            style={
                "backgroundColor": COLORS["accentTealBg"],
                "borderTop":       f"3px solid {COLORS['accentTeal']}",
                "borderBottom":    f"1px solid {COLORS['tableBorder']}",
                "padding":         "7px 14px",
                "display":         "flex",
                "alignItems":      "center",
                "justifyContent":  "space-between",
                "flexShrink":      "0",
            },
            children=[
                html.Span(
                    "Search Results",
                    style={"fontWeight": "700", "fontSize": "12px", "color": "#2C5F6E"},
                ),
                html.Span(
                    id="result-count",
                    style={"fontSize": "11px", "color": "#666"},
                ),
            ],
        ),
        # Loading wrapper + table container
        dcc.Loading(
            id="loading-results",
            type="circle",
            color=COLORS["accentTeal"],
            children=html.Div(
                id="result-table-container",
                style={"padding": "0 8px 16px 8px", "overflowX": "auto"},
            ),
        ),
    ],
)

# ─── App Layout ───────────────────────────────────────────────────────────────
app.layout = html.Div(
    style={
        "display":    "flex",
        "flexDirection": "row",
        "height":     "100vh",
        "overflow":   "hidden",
        "fontFamily": FONT_FAMILY,
        "backgroundColor": COLORS["pageBg"],
    },
    children=[
        left_nav,
        # ── Main content ────────────────────────────────────────────────────
        html.Div(
            style={
                "flex":          "1",
                "display":       "flex",
                "flexDirection": "column",
                "overflow":      "hidden",
                "minWidth":      "0",
            },
            children=[
                # Page header bar
                html.Div(
                    style={
                        "backgroundColor": COLORS["pageHeader"],
                        "padding":         "10px 18px",
                        "borderBottom":    "1px solid #DDD",
                        "display":         "flex",
                        "alignItems":      "center",
                        "flexShrink":      "0",
                    },
                    children=[
                        html.I(
                            className="bi bi-search",
                            style={
                                "color":       COLORS["accentTeal"],
                                "fontSize":    "17px",
                                "marginRight": "10px",
                            },
                        ),
                        html.Span(
                            "ChartSearch",
                            style={
                                "fontSize":   "16px",
                                "fontWeight": "700",
                                "color":      "#2C3E50",
                            },
                        ),
                    ],
                ),
                filter_bar,
                results_area,
            ],
        ),
    ],
)

# ─── Callbacks ────────────────────────────────────────────────────────────────

@app.callback(
    Output("result-table-container", "children"),
    Output("result-count", "children"),
    Input("btn-search", "n_clicks"),
    State("f-member-card-id",   "value"),
    State("f-individual-id",    "value"),
    State("f-chart-name",       "value"),
    State("f-chart-request-id", "value"),
    State("f-first-name",       "value"),
    State("f-last-name",        "value"),
    State("f-dob",              "value"),
    State("f-vendor",           "value"),
    State("f-npi-id",           "value"),
    State("f-dos-start",        "value"),
    State("f-dos-end",          "value"),
    State("f-project-name",     "value"),
    State("fuzzy-toggle",       "value"),
    prevent_initial_call=True,
)
def run_search(
    _clicks,
    member_card_id, individual_id, chart_name, chart_request_id,
    first_name, last_name, dob, vendor, npi_id,
    dos_start, dos_end, project_name, fuzzy_value,
):
    is_fuzzy = bool(fuzzy_value)
    filters = {
        "member_card_id":   member_card_id,
        "individual_id":    individual_id,
        "chart_name":       chart_name,
        "chart_request_id": chart_request_id,
        "first_name":       first_name,
        "last_name":        last_name,
        "dob":              dob,
        "vendor":           vendor,
        "npi_id":           npi_id,
        "dos_start_date":   dos_start,
        "dos_end_date":     dos_end,
        "project_name":     project_name,
    }
    try:
        records = search_records(filters, fuzzy=is_fuzzy)
    except Exception as exc:
        return (
            html.Div(
                f"Query error: {exc}",
                style={"color": "red", "padding": "16px", "fontSize": "13px"},
            ),
            "Error",
        )

    if not records:
        return (
            html.Div(
                "No records matched your search criteria.",
                style={"padding": "20px", "color": "#777", "fontSize": "13px"},
            ),
            "0 records",
        )

    table = dash_table.DataTable(
        data=records,
        columns=RESULT_COLUMNS,
        page_size=25,
        page_action="native",
        sort_action="native",
        filter_action="none",
        style_table={"overflowX": "auto", "minWidth": "100%"},
        style_header={
            "backgroundColor": COLORS["tableHeaderBg"],
            "color":           COLORS["tableHeaderText"],
            "fontWeight":      "700",
            "fontSize":        "11px",
            "padding":         "9px 10px",
            "border":          f"1px solid {COLORS['tableBorder']}",
            "textAlign":       "left",
            "whiteSpace":      "nowrap",
        },
        style_data={
            "fontSize":  "12px",
            "padding":   "7px 10px",
            "border":    f"1px solid {COLORS['tableBorder']}",
            "color":     COLORS["tableText"],
        },
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": COLORS["tableRowOdd"],
            },
        ],
        style_cell={
            "textAlign":  "left",
            "fontFamily": FONT_FAMILY,
            "whiteSpace": "nowrap",
            "overflow":   "hidden",
            "textOverflow": "ellipsis",
            "maxWidth":   "200px",
        },
    )

    fuzzy_tag = "  ·  fuzzy match on" if is_fuzzy else ""
    count_label = f"{len(records):,} record{'s' if len(records) != 1 else ''} found{fuzzy_tag}"
    return table, count_label


@app.callback(
    Output("f-member-card-id",   "value"),
    Output("f-individual-id",    "value"),
    Output("f-chart-name",       "value"),
    Output("f-chart-request-id", "value"),
    Output("f-first-name",       "value"),
    Output("f-last-name",        "value"),
    Output("f-dob",              "value"),
    Output("f-vendor",           "value"),
    Output("f-npi-id",           "value"),
    Output("f-dos-start",        "value"),
    Output("f-dos-end",          "value"),
    Output("f-project-name",     "value"),
    Output("result-table-container", "children", allow_duplicate=True),
    Output("result-count",           "children", allow_duplicate=True),
    Output("fuzzy-toggle",           "value",    allow_duplicate=True),
    Input("btn-clear", "n_clicks"),
    prevent_initial_call=True,
)
def clear_all(_):
    return None, None, None, None, None, None, None, "ALL", None, None, None, None, None, "", []


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)

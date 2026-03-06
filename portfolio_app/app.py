"""
#----------------------------------------------------------------#

Bruce's Analytics Portfolio App

This app (powered by Dash) showcases customer analytics
across modalities.

Please refer to README.md file for more information.

All dependencies here are installed via requirements.txt
when deployed.

#----------------------------------------------------------------#
"""

# Standard library imports
import os
import sys
import functools
from pathlib import Path
from io import BytesIO
from datetime import datetime

# Third party imports
from minio import Minio as mc
import dash
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import duckdb as ddb

# Local imports
from portfolio_app.scripts.main_data_pipeline import run_dbt_ops as rdops
from portfolio_app.scripts.analytics_queries import (
    run_lifecycle_analysis as la,
    run_purchase_analysis as pa,
    run_demographics_analysis as da,
    run_business_analysis as ba,
    run_engagement_analysis as ea,
    run_churn_analysis as ca,
)
from portfolio_app.scripts.constants import (
    DB_PATH, LOG, MINIO_BUCKET_NAME, MINIO_ENDPOINT, MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD, MINIO_USE_SSL, DBT_PROFILES_DIR,
)

# ─── Setup ─────────────────────────────────────────────────────────────────────

sys.path.append(str(Path(__file__).resolve().parents[1]))
os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_DIR)

DB_PATH = DB_PATH.resolve()
LOG.info("Database path: %s", DB_PATH)

if not DB_PATH.exists():
    raise FileNotFoundError(f"Database file not found at: {DB_PATH}")

# ─── App initialization ─────────────────────────────────────────────────────────

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE, dbc.icons.BOOTSTRAP],
    title="Bruce's Analytics Portfolio",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server  # Expose Flask server for deployment

# ─── Constants ─────────────────────────────────────────────────────────────────

CHART_H = 500          # Standard chart height
CHART_H_FULL = 450     # Full-width chart height

TECH_BADGES = ["dbt", "DuckDB", "Plotly", "MinIO", "Dash", "Python"]

RESUME_URL = (
    "https://github.com/Brucelee352/Product_data_pipelining/blob/"
    "e0b968643ea3455fc5368490c73133f6fb70ac37/misc/BruceLee_2025Resume_b.pdf"
)

# ─── Data helpers ───────────────────────────────────────────────────────────────


def minio_client():
    """Return a MinIO client."""
    return mc(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=MINIO_USE_SSL,
    )


@functools.lru_cache(maxsize=1)
def load_from_s3(bucket_name: str, file_name: str) -> pd.DataFrame:
    """Load a Parquet file from MinIO and cache the result in memory."""
    try:
        client = minio_client()
        response = client.get_object(bucket_name, file_name)
        df = pd.read_parquet(BytesIO(response.read()))
        LOG.info("Loaded %s from MinIO.", file_name)
        return df
    except Exception as e:
        LOG.error("Error loading %s from MinIO: %s", file_name, str(e))
        raise


def ddb_connect():
    """Return a DuckDB connection."""
    try:
        con = ddb.connect(str(DB_PATH))
        LOG.info("Database connection successful.")
        return con
    except Exception as e:
        LOG.error("Error connecting to database: %s", str(e))
        raise


# ─── KPI computation ────────────────────────────────────────────────────────────


def compute_kpis(con) -> dict:
    """Query headline KPI values from the product schema."""
    schema = "main.product_schema"
    defaults = {"total_revenue": "N/A", "total_users": "N/A",
                "avg_churn": "N/A", "avg_session": "N/A"}
    try:
        rev = con.execute(
            f"SELECT ROUND(SUM(price), 2) FROM {schema} WHERE purchase_status='completed'"
        ).fetchone()[0]

        users = con.execute(
            f"SELECT COUNT(DISTINCT user_id) FROM {schema}"
        ).fetchone()[0]

        churn = con.execute(f"""
            SELECT ROUND(AVG(churn_rate), 2) FROM (
                SELECT
                    ROUND(
                        COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END)
                        * 100.0 / NULLIF(COUNT(*), 0),
                    2) AS churn_rate
                FROM {schema}
                GROUP BY DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP))
            )
        """).fetchone()[0]

        session = con.execute(
            f"SELECT ROUND(AVG(session_duration_minutes), 1) FROM {schema}"
        ).fetchone()[0]

        return {
            "total_revenue": f"${rev:,.2f}" if rev is not None else "N/A",
            "total_users":   f"{users:,}"   if users is not None else "N/A",
            "avg_churn":     f"{churn}%"    if churn is not None else "N/A",
            "avg_session":   f"{session} min" if session is not None else "N/A",
        }
    except Exception as e:
        LOG.error("KPI computation error: %s", str(e))
        return defaults


# ─── Chart builders ─────────────────────────────────────────────────────────────


def _dark(extra: dict = None) -> dict:
    """Return a base dark-theme layout dict, optionally merged with extras."""
    base = {"template": "plotly_dark", "margin": dict(t=55, b=45, l=45, r=25)}
    if extra:
        base.update(extra)
    return base


def build_churn_chart(con):
    df = ca(con=con)
    if df.empty:
        return None
    # Format cohort_month as a readable string for categorical color
    df["cohort_label"] = pd.to_datetime(df["cohort_month"]).dt.strftime("%Y-%m")
    fig = px.line(
        df,
        x="cohort_label",
        y="churn_rate",
        markers=True,
        hover_data=["cohort_size", "avg_days_to_churn"],
        title="Churn Rate by Cohort Month",
        height=CHART_H,
        color_discrete_sequence=["#7FDBFF"],
    )
    fig.update_layout(**_dark({
        "xaxis_title": "Cohort Month",
        "yaxis_title": "Churn Rate (%)",
    }))
    return fig


def build_purchase_chart(con):
    df = pa(con=con)
    if df.empty:
        return None
    fig = px.scatter(
        df,
        x="month",
        y="total_revenue",
        size="total_revenue",
        color="price_tier",
        hover_data=["product_name", "total_revenue", "avg_price"],
        title="Revenue by Month & Product",
        opacity=0.75,
        height=CHART_H,
    )
    fig.update_layout(**_dark({
        "xaxis_title": "Month",
        "yaxis_title": "Revenue ($)",
        "legend_title": "Price Tier",
    }))
    return fig


def build_os_chart(con):
    df = la(con=con)
    if df.empty:
        return None
    df_grouped = (
        df.groupby("os", as_index=False)
        .agg({"total_revenue": "sum", "total_customers": "sum", "total_purchases": "sum"})
        .sort_values("total_revenue", ascending=True)
    )
    fig = px.bar(
        df_grouped,
        x="total_revenue",
        y="os",
        orientation="h",
        color="total_revenue",
        color_continuous_scale="Blues",
        text="total_revenue",
        title="Total Revenue by Operating System",
        height=CHART_H_FULL,
    )
    fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    fig.update_layout(**_dark({
        "xaxis_title": "Total Revenue ($)",
        "yaxis_title": "Operating System",
        "coloraxis_showscale": False,
        "margin": dict(t=55, b=45, l=90, r=90),
    }))
    return fig


def build_demographics_chart(con):
    df = da(con=con)
    if df.empty:
        return None
    fig = px.bar(
        df,
        x="product_name",
        y="unique_users",
        color="browser",
        barmode="relative",
        hover_data=["avg_purchase_value", "avg_session_duration"],
        title="Usage by Product & Browser",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        height=CHART_H,
    )
    fig.update_layout(**_dark({
        "xaxis_title": "Product Name",
        "yaxis_title": "Unique Users",
        "legend_title": "Browser",
    }))
    return fig


def build_business_chart(con):
    df = ba(con=con)
    if df.empty:
        return None
    fig = px.bar(
        df,
        x="device_type",
        y="unique_users",
        color="conversion_rate",
        color_continuous_scale="Teal",
        hover_data=["total_sessions", "avg_session_duration"],
        title="Users by Device Type",
        height=CHART_H,
    )
    fig.update_layout(**_dark({
        "xaxis_title": "Device Type",
        "yaxis_title": "Unique Users",
        "coloraxis_colorbar_title": "Conv. %",
    }))
    return fig


def build_engagement_chart(con):
    df = ea(con=con)
    if df.empty:
        return None
    fig = px.bar(
        df,
        x="hour",
        y="revenue",
        color="total_sessions",
        color_continuous_scale="Turbo",
        hover_data=["total_sessions", "avg_session_duration"],
        title="Revenue & Sessions by Hour of Day",
        height=CHART_H_FULL,
    )
    fig.update_layout(**_dark({
        "xaxis_title": "Hour of Day",
        "yaxis_title": "Revenue ($)",
        "coloraxis_colorbar_title": "Sessions",
    }))
    return fig


# ─── Layout components ──────────────────────────────────────────────────────────


def _chart_or_alert(fig, msg: str):
    """Wrap a figure in dcc.Graph, or show an actionable alert if None."""
    if fig is not None:
        return dcc.Loading(
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
            type="circle",
            color="#7FDBFF",
        )
    return dbc.Alert(
        [html.Strong("No data available. "), msg],
        color="warning",
        className="mt-2",
    )


def make_header():
    return dbc.Row(
        dbc.Col([
            html.H2("Bruce's Analytics Portfolio", className="mb-1 fw-bold"),
            html.P(
                "End-to-end Analytics Engineering: synthetic data generation → "
                "dbt transformations → DuckDB → interactive dashboards.",
                className="text-muted mb-2",
            ),
            html.Div([
                dbc.Badge(tech, color="secondary", className="me-1 mb-1", pill=True)
                for tech in TECH_BADGES
            ]),
        ]),
        className="mt-3 mb-3",
    )


def make_button_row():
    return dbc.Row(
        dbc.Col(
            html.Div([
                # Social / external links
                dbc.ButtonGroup([
                    dbc.Button(
                        [html.I(className="bi bi-github me-1"), "Repository"],
                        href="https://github.com/brucelee352/Product_data_pipelining",
                        external_link=True,
                        color="primary",
                        outline=True,
                    ),
                    dbc.Button(
                        [html.I(className="bi bi-linkedin me-1"), "LinkedIn"],
                        href="https://www.linkedin.com/in/brucealee/",
                        external_link=True,
                        color="primary",
                        outline=True,
                    ),
                    dbc.Button(
                        [html.I(className="bi bi-file-earmark-person me-1"), "Resume"],
                        href=RESUME_URL,
                        external_link=True,
                        color="primary",
                        outline=True,
                    ),
                ], className="me-4"),
                # Functional controls
                dbc.Button(
                    [html.I(className="bi bi-arrow-clockwise me-1"), "Refresh Pipeline"],
                    id="refresh-btn",
                    color="warning",
                    className="me-2",
                ),
                dbc.Button(
                    [html.I(className="bi bi-download me-1"), "Download Data"],
                    id="download-btn",
                    color="success",
                ),
                dcc.Download(id="download-data"),
            ], className="d-flex align-items-center flex-wrap gap-1"),
        ),
        className="mb-3",
    )


def make_kpi_row(kpis: dict):
    tiles = [
        ("Total Revenue",    kpis["total_revenue"], "success", "bi-currency-dollar"),
        ("Total Users",      kpis["total_users"],   "info",    "bi-people-fill"),
        ("Avg Churn Rate",   kpis["avg_churn"],     "danger",  "bi-person-dash-fill"),
        ("Avg Session Time", kpis["avg_session"],   "warning", "bi-clock-fill"),
    ]
    cards = [
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    html.Div([
                        html.I(className=f"bi {icon} fs-2 text-{color} me-3"),
                        html.Div([
                            html.H4(value, className=f"mb-0 text-{color}"),
                            html.Small(label, className="text-muted"),
                        ]),
                    ], className="d-flex align-items-center"),
                ),
                className="border-0 shadow-sm h-100",
            ),
            md=3, sm=6, className="mb-3",
        )
        for label, value, color, icon in tiles
    ]
    return dbc.Row(cards)


def make_tabs(con):
    churn_fig    = build_churn_chart(con)
    purchase_fig = build_purchase_chart(con)
    os_fig       = build_os_chart(con)
    demo_fig     = build_demographics_chart(con)
    biz_fig      = build_business_chart(con)
    eng_fig      = build_engagement_chart(con)

    revenue_tab = dbc.Tab(
        label="Revenue Centric",
        tab_id="tab-revenue",
        children=[
            dbc.Row([
                dbc.Col(
                    _chart_or_alert(churn_fig, "Run the pipeline first."),
                    md=8,
                ),
                dbc.Col(
                    _chart_or_alert(purchase_fig, "Run the pipeline first."),
                    md=4,
                ),
            ], className="mt-3"),
            html.Hr(),
            dbc.Row(
                dbc.Col(_chart_or_alert(os_fig, "Run the pipeline first.")),
            ),
        ],
    )

    user_tab = dbc.Tab(
        label="User Centric",
        tab_id="tab-user",
        children=[
            dbc.Row([
                dbc.Col(
                    _chart_or_alert(demo_fig, "Run the pipeline first."),
                    md=8,
                ),
                dbc.Col(
                    _chart_or_alert(biz_fig, "Run the pipeline first."),
                    md=4,
                ),
            ], className="mt-3"),
            html.Hr(),
            dbc.Row(
                dbc.Col(_chart_or_alert(eng_fig, "Run the pipeline first.")),
            ),
        ],
    )

    return dbc.Tabs(
        [revenue_tab, user_tab],
        id="main-tabs",
        active_tab="tab-revenue",
        className="mt-2",
    )


def make_footer():
    return dbc.Row(
        dbc.Col(
            html.Small(
                f"Bruce Anthony Lee · Analytics Engineering Portfolio · "
                f"Data as of {datetime.now().strftime('%Y-%m-%d')}",
                className="text-muted",
            ),
            className="text-center py-3",
        )
    )


# ─── Full layout (called per browser session) ───────────────────────────────────


def serve_layout():
    """Build and return the full app layout. Called fresh on each page load."""
    try:
        con = ddb_connect()
        kpis = compute_kpis(con)
        tabs = make_tabs(con)
    except Exception as e:
        LOG.error("Layout build error: %s", str(e))
        kpis = {k: "N/A" for k in ["total_revenue", "total_users", "avg_churn", "avg_session"]}
        tabs = dbc.Alert(
            [
                html.Strong("Could not load data. "),
                f"{e} — please run the pipeline first, then refresh the page.",
            ],
            color="danger",
        )

    return dbc.Container(
        [
            make_header(),
            make_button_row(),
            html.Div(id="refresh-status", className="mb-2"),
            html.Hr(),
            make_kpi_row(kpis),
            tabs,
            html.Hr(className="mt-4"),
            make_footer(),
        ],
        fluid=True,
    )


app.layout = serve_layout

# ─── Callbacks ──────────────────────────────────────────────────────────────────


@app.callback(
    Output("refresh-status", "children"),
    Input("refresh-btn", "n_clicks"),
    prevent_initial_call=True,
)
def on_refresh(_):
    """Re-run dbt models and notify the user."""
    try:
        load_from_s3.cache_clear()
        df = load_from_s3(MINIO_BUCKET_NAME, "cleaned_data.parquet")
        con = ddb.connect(str(DB_PATH))
        con.register("parquet_data", df)
        rdops()
        return dbc.Alert(
            [
                html.Strong("Pipeline refreshed. "),
                "Reload the page to see updated charts.",
            ],
            color="success",
            dismissable=True,
            duration=8000,
        )
    except Exception as e:
        LOG.error("Refresh error: %s", str(e))
        return dbc.Alert(
            [html.Strong("Refresh failed. "), str(e)],
            color="danger",
            dismissable=True,
        )


@app.callback(
    Output("download-data", "data"),
    Input("download-btn", "n_clicks"),
    prevent_initial_call=True,
)
def on_download(_):
    """Stream cleaned data as a CSV download."""
    try:
        df = load_from_s3(MINIO_BUCKET_NAME, "cleaned_data.parquet")
        return dcc.send_data_frame(df.to_csv, "cleaned_data.csv", index=False)
    except Exception as e:
        LOG.error("Download error: %s", str(e))
        return dash.no_update


# ─── Entry point ────────────────────────────────────────────────────────────────


def main():
    """Run the Dash development server."""
    app.run(debug=False, host="0.0.0.0", port=8050)


if __name__ == "__main__":
    main()

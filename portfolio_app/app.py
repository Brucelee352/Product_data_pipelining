"""Kroger product analytics dashboard — Dash app."""

# Standard library imports
import contextlib
import logging
import os
from pathlib import Path

# Third-party imports
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, no_update
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parents[1]
DB_PATH = PROJECT_ROOT / os.environ.get(
    "DB_PATH", "dbt_pipeline_demo/databases/kroger_pipeline.duckdb"
)

LOG.info("Database path: %s", DB_PATH)

# ---------------------------------------------------------------------------
# Optional pipeline import
# ---------------------------------------------------------------------------
try:
    from portfolio_app.scripts.main_pipeline import run_dbt_ops  # noqa: E402
    _PIPELINE_AVAILABLE = True
except Exception as _pipeline_import_err:  # pragma: no cover
    LOG.warning("Pipeline import failed (%s) — Refresh will be disabled.", _pipeline_import_err)
    _PIPELINE_AVAILABLE = False
    run_dbt_ops = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# DuckDB connection helper
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def db_connect():
    """Yield a read-only DuckDB connection, closing it on exit."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        yield con
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Data loaders — each returns a DataFrame or raises
# ---------------------------------------------------------------------------

def _load_category_distribution() -> pd.DataFrame:
    with db_connect() as con:
        return con.execute(
            "SELECT category, product_count, avg_regular_price, avg_promo_price "
            "FROM main_marts.mart_category_distribution"
        ).df()


def _load_fact_prices_for_box() -> pd.DataFrame:
    with db_connect() as con:
        return con.execute(
            "SELECT category, regular_price "
            "FROM main_marts.fact_prices "
            "WHERE regular_price IS NOT NULL "
            "LIMIT 5000"
        ).df()


def _load_location_sales() -> pd.DataFrame:
    with db_connect() as con:
        return con.execute(
            "SELECT location_id, name, city, state, latitude, longitude, "
            "instore_count, delivery_count, curbside_count, shiptohome_count, "
            "total_products, avg_price, dominant_fulfillment "
            "FROM main_marts.mart_location_sales"
        ).df()


def _load_price_by_category_latest() -> pd.DataFrame:
    """Return one row per category using the most-recent effective_date."""
    with db_connect() as con:
        return con.execute(
            """
            SELECT category, avg_regular_price, avg_promo_price, avg_discount_pct
            FROM main_marts.mart_price_by_category
            QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY effective_date DESC) = 1
            """
        ).df()


def _load_state_price_timeseries() -> pd.DataFrame:
    with db_connect() as con:
        return con.execute(
            """
            SELECT
                state,
                DATE_TRUNC('month', effective_date) AS month,
                AVG(regular_price) AS avg_price
            FROM main_marts.fact_prices
            WHERE effective_date IS NOT NULL
            GROUP BY state, month
            ORDER BY month
            """
        ).df()


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

_EMPTY_STATE = dbc.Alert(
    "Data unavailable — run the pipeline first.", color="warning"
)


def _empty(msg: str = "Data unavailable — run the pipeline first.") -> dbc.Alert:
    return dbc.Alert(msg, color="warning")


# Chart 1 — Bar: Product count by category
def build_category_bar() -> dcc.Graph | dbc.Alert:
    try:
        df = _load_category_distribution()
        if df.empty:
            return _empty()
        df = df.sort_values("product_count", ascending=False)
        fig = px.bar(
            df,
            x="category",
            y="product_count",
            color="category",
            title="Products by Category",
            hover_data={"avg_regular_price": True, "category": False},
            labels={"product_count": "Product Count", "category": "Category",
                    "avg_regular_price": "Avg Regular Price ($)"},
        )
        fig.update_layout(
            showlegend=False,
            xaxis_tickangle=-30,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#dee2e6",
        )
        return dcc.Graph(figure=fig, style={"height": "420px"})
    except Exception as exc:
        LOG.error("Chart 1 error: %s", exc)
        return _empty(f"Products by Category unavailable: {exc}")


# Chart 2 — Box plot: Price distribution per category
def build_price_box() -> dcc.Graph | dbc.Alert:
    try:
        df = _load_fact_prices_for_box()
        if df.empty:
            return _empty()
        fig = px.box(
            df,
            x="category",
            y="regular_price",
            color="category",
            title="Price Distribution by Category",
            labels={"regular_price": "Regular Price ($)", "category": "Category"},
        )
        fig.add_annotation(
            text="Based on current price snapshot",
            xref="paper", yref="paper",
            x=1.0, y=1.05,
            showarrow=False,
            font={"size": 10, "color": "#adb5bd"},
            xanchor="right",
        )
        fig.update_layout(
            showlegend=False,
            xaxis_tickangle=-30,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#dee2e6",
        )
        return dcc.Graph(figure=fig, style={"height": "420px"})
    except Exception as exc:
        LOG.error("Chart 2 error: %s", exc)
        return _empty(f"Price distribution unavailable: {exc}")


# Chart 3 — Scatter geo map: Store locations
def build_location_map(fulfillment_filter: str = "All") -> dcc.Graph | dbc.Alert:
    try:
        df = _load_location_sales()
        if df.empty:
            return _empty()

        # Normalise the dominant_fulfillment values to display labels
        _label_map = {
            "instore": "In Store",
            "delivery": "Delivery",
            "curbside": "Curbside",
            "shiptohome": "Ship to Home",
            "in store": "In Store",
            "ship to home": "Ship to Home",
        }
        df["dominant_fulfillment"] = (
            df["dominant_fulfillment"]
            .str.strip()
            .str.lower()
            .map(lambda v: _label_map.get(v, v.title()))
        )

        if fulfillment_filter != "All":
            df = df[df["dominant_fulfillment"] == fulfillment_filter]
            if df.empty:
                return _empty(f"No stores found for fulfillment type: {fulfillment_filter}")

        fig = px.scatter_geo(
            df,
            lat="latitude",
            lon="longitude",
            color="dominant_fulfillment",
            size="total_products",
            hover_name="name",
            hover_data={"city": True, "state": True, "avg_price": True,
                        "latitude": False, "longitude": False},
            scope="usa",
            title="Kroger Store Locations by Fulfillment Channel",
            labels={"dominant_fulfillment": "Fulfillment", "avg_price": "Avg Price ($)",
                    "total_products": "Total Products"},
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#dee2e6",
            geo=dict(bgcolor="rgba(0,0,0,0)", landcolor="#2c3e50",
                     lakecolor="#1a252f", countrycolor="#555"),
        )
        return dcc.Graph(figure=fig, style={"height": "520px"})
    except Exception as exc:
        LOG.error("Chart 3 error: %s", exc)
        return _empty(f"Store map unavailable: {exc}")


# Chart 4 — Grouped bar: Regular vs promo price per category
def build_price_comparison_bar() -> dcc.Graph | dbc.Alert:
    try:
        df = _load_price_by_category_latest()
        if df.empty:
            return _empty()

        # Identify highest-discount category for annotation
        max_discount_row = df.loc[df["avg_discount_pct"].idxmax()] if "avg_discount_pct" in df.columns else None

        # Melt to long form
        df_melted = df.melt(
            id_vars=["category", "avg_discount_pct"],
            value_vars=["avg_regular_price", "avg_promo_price"],
            var_name="price_type",
            value_name="price",
        )
        df_melted["price_type"] = df_melted["price_type"].map(
            {"avg_regular_price": "Regular Price", "avg_promo_price": "Promo Price"}
        )

        fig = px.bar(
            df_melted,
            x="category",
            y="price",
            color="price_type",
            barmode="group",
            title="Regular vs. Promotional Prices by Category",
            labels={"price": "Price ($)", "category": "Category",
                    "price_type": "Price Type"},
            color_discrete_map={"Regular Price": "#4e73df", "Promo Price": "#e74a3b"},
        )

        # Annotate highest-discount category
        if max_discount_row is not None:
            fig.add_annotation(
                x=max_discount_row["category"],
                y=max_discount_row["avg_regular_price"],
                text=f"Highest discount\n({max_discount_row['avg_discount_pct']:.1f}%)",
                showarrow=True,
                arrowhead=2,
                font={"size": 10, "color": "#f6c23e"},
                arrowcolor="#f6c23e",
                yshift=10,
            )

        fig.update_layout(
            xaxis_tickangle=-30,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#dee2e6",
            legend_title_text="Price Type",
        )
        return dcc.Graph(figure=fig, style={"height": "420px"})
    except Exception as exc:
        LOG.error("Chart 4 error: %s", exc)
        return _empty(f"Price comparison unavailable: {exc}")


# Chart 5 — Time series or bar: Average price by state over time
def build_state_price_chart() -> dcc.Graph | dbc.Alert:
    try:
        df = _load_state_price_timeseries()
        if df.empty:
            return _empty()

        df["month"] = pd.to_datetime(df["month"])
        unique_months = df["month"].nunique()

        if unique_months >= 2:
            fig = px.line(
                df,
                x="month",
                y="avg_price",
                color="state",
                title="Average Product Price by State Over Time",
                labels={"avg_price": "Avg Price ($)", "month": "Month", "state": "State"},
            )
        else:
            # Fallback: bar chart of avg price per state
            df_agg = df.groupby("state", as_index=False)["avg_price"].mean()
            df_agg = df_agg.sort_values("avg_price", ascending=False)
            fig = px.bar(
                df_agg,
                x="state",
                y="avg_price",
                color="state",
                title="Average Product Price by State (Current Snapshot)",
                labels={"avg_price": "Avg Price ($)", "state": "State"},
            )
            fig.update_layout(showlegend=False)

        fig.update_layout(
            xaxis_tickangle=-30,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#dee2e6",
        )
        return dcc.Graph(figure=fig, style={"height": "420px"})
    except Exception as exc:
        LOG.error("Chart 5 error: %s", exc)
        return _empty(f"State price chart unavailable: {exc}")


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

def _header() -> html.Div:
    return html.Div(
        [
            html.H2("Kroger Product Analytics", className="mb-1"),
            html.P(
                "Market research dashboard — product availability, pricing, and fulfillment trends",
                className="text-muted mb-0",
            ),
            html.Hr(),
        ]
    )


def _refresh_button_row() -> dbc.Row:
    return dbc.Row(
        [
            dbc.Col(
                dbc.Button(
                    "Refresh Data",
                    id="refresh-button",
                    color="primary",
                    className="w-100",
                    title="Re-run the dbt models to refresh data",
                    disabled=(not _PIPELINE_AVAILABLE),
                ),
                width=2,
            ),
            dbc.Col(html.Div(id="refresh-status"), width=10),
        ],
        className="mb-3 g-2 align-items-center",
    )


def _tab_products() -> dcc.Tab:
    return dcc.Tab(
        label="Products",
        children=[
            dbc.Row(
                dbc.Col(
                    dcc.Loading(
                        id="loading-chart1",
                        type="circle",
                        children=html.Div(id="chart-category-bar"),
                    ),
                    width=12,
                ),
                className="mt-3",
            ),
            dbc.Row(
                dbc.Col(
                    dcc.Loading(
                        id="loading-chart2",
                        type="circle",
                        children=html.Div(id="chart-price-box"),
                    ),
                    width=12,
                ),
                className="mt-3",
            ),
        ],
    )


def _tab_pricing() -> dcc.Tab:
    return dcc.Tab(
        label="Pricing",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Loading(
                            id="loading-chart4",
                            type="circle",
                            children=html.Div(id="chart-price-comparison"),
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dcc.Loading(
                            id="loading-chart5",
                            type="circle",
                            children=html.Div(id="chart-state-price"),
                        ),
                        width=6,
                    ),
                ],
                className="mt-3",
            ),
        ],
    )


def _tab_locations() -> dcc.Tab:
    return dcc.Tab(
        label="Locations",
        children=[
            dbc.Row(
                dbc.Col(
                    dcc.Dropdown(
                        id="fulfillment-filter",
                        options=[
                            {"label": "All", "value": "All"},
                            {"label": "In Store", "value": "In Store"},
                            {"label": "Delivery", "value": "Delivery"},
                            {"label": "Curbside", "value": "Curbside"},
                            {"label": "Ship to Home", "value": "Ship to Home"},
                        ],
                        value="All",
                        clearable=False,
                        style={"color": "#212529"},
                    ),
                    width=4,
                ),
                className="mt-3 mb-2",
            ),
            dbc.Row(
                dbc.Col(
                    dcc.Loading(
                        id="loading-chart3",
                        type="circle",
                        children=html.Div(id="chart-location-map"),
                    ),
                    width=12,
                ),
            ),
        ],
    )


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> Dash:
    """Create and configure the Dash application."""
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.DARKLY],
        title="Kroger Analytics Dashboard",
    )

    app.layout = dbc.Container(
        [
            _header(),
            _refresh_button_row(),
            dcc.Tabs(
                [
                    _tab_products(),
                    _tab_pricing(),
                    _tab_locations(),
                ],
            ),
        ],
        fluid=True,
        className="py-3",
    )

    _register_callbacks(app)
    return app


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def _register_callbacks(app: Dash) -> None:
    """Register all Dash callbacks."""

    # ------------------------------------------------------------------
    # On page load — populate static charts (Products tab)
    # ------------------------------------------------------------------

    @app.callback(
        Output("chart-category-bar", "children"),
        Input("chart-category-bar", "id"),
    )
    def render_category_bar(_id):
        return build_category_bar()

    @app.callback(
        Output("chart-price-box", "children"),
        Input("chart-price-box", "id"),
    )
    def render_price_box(_id):
        return build_price_box()

    # ------------------------------------------------------------------
    # Pricing tab — static charts
    # ------------------------------------------------------------------

    @app.callback(
        Output("chart-price-comparison", "children"),
        Input("chart-price-comparison", "id"),
    )
    def render_price_comparison(_id):
        return build_price_comparison_bar()

    @app.callback(
        Output("chart-state-price", "children"),
        Input("chart-state-price", "id"),
    )
    def render_state_price(_id):
        return build_state_price_chart()

    # ------------------------------------------------------------------
    # Locations tab — map filtered by dropdown
    # ------------------------------------------------------------------

    @app.callback(
        Output("chart-location-map", "children"),
        Input("fulfillment-filter", "value"),
    )
    def render_location_map(fulfillment_filter: str):
        return build_location_map(fulfillment_filter or "All")

    # ------------------------------------------------------------------
    # Refresh button — re-run dbt models
    # ------------------------------------------------------------------

    @app.callback(
        Output("refresh-status", "children"),
        Input("refresh-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def handle_refresh(n_clicks):
        if not n_clicks:
            return no_update
        if not _PIPELINE_AVAILABLE or run_dbt_ops is None:
            return dbc.Alert(
                "Pipeline module not available — cannot refresh data.",
                color="warning",
                dismissable=True,
                duration=6000,
            )
        try:
            run_dbt_ops()
            return dbc.Alert(
                "Pipeline ran successfully — refresh the page to see updated charts.",
                color="success",
                dismissable=True,
                duration=6000,
            )
        except Exception as exc:
            LOG.error("Refresh failed: %s", exc)
            return dbc.Alert(
                f"Pipeline error: {exc}",
                color="danger",
                dismissable=True,
                duration=8000,
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Start the Dash dev server."""
    app = create_app()
    app.run(debug=False, host="0.0.0.0", port=8050)


if __name__ == "__main__":
    main()

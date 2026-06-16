"""Kroger product analytics dashboard.

Single-page, static Dash dashboard for store operations staff and
non-executive stakeholders. Reads the dbt mart tables directly from DuckDB
and renders five charts covering pricing, availability, and fulfillment.

No interactivity controls (no tabs, dropdowns, sliders, or buttons) — every
chart is a static render of the underlying data.
"""

import contextlib
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()
LOG = logging.getLogger(__name__)
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
)

PROJECT_ROOT = Path(__file__).parents[1]
DB_PATH = PROJECT_ROOT / os.environ.get(
    "DB_PATH", "dbt_pipeline_demo/databases/kroger_pipeline.duckdb"
)

LOG.info("Database path: %s", DB_PATH)

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
LIGHT_LAYOUT = dict(
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#F0F4F8",
    font_color="#003087",
    legend=dict(bgcolor="rgba(255,255,255,0.85)", bordercolor="#003087", borderwidth=1),
    margin=dict(l=40, r=20, t=70, b=40),
)

# Geo styling for the map figure so it blends with the light Kroger theme.
LIGHT_GEO = dict(
    bgcolor="#FFFFFF",
    landcolor="#D6E4F0",
    lakecolor="#A9CCE3",
    subunitcolor="#7FB3D3",
    countrycolor="#7FB3D3",
    coastlinecolor="#5499C7",
)


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


def _empty(msg: str) -> dbc.Alert:
    """Standard empty-state alert shown when a chart cannot be built."""
    return dbc.Alert(msg, color="info", className="m-3")


# ---------------------------------------------------------------------------
# Chart 1 — Box & Whisker: Product Price Distribution, Q1 2026
# ---------------------------------------------------------------------------
def build_box_chart() -> "dcc.Graph | dbc.Alert":
    """Box plot of regular price per category for Q1 2026 (with fallback)."""
    try:
        q1_sql = """
            SELECT category, regular_price
            FROM main_marts.fact_prices
            WHERE effective_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
              AND regular_price > 0 AND category IS NOT NULL
        """
        with db_connect() as con:
            df = con.execute(q1_sql).df()
            title = "Product Price Distribution — Q1 2026"

            # Fallback to all available data if Q1 2026 has no rows.
            if df.empty:
                df = con.execute(
                    """
                    SELECT category, regular_price
                    FROM main_marts.fact_prices
                    WHERE regular_price > 0 AND category IS NOT NULL
                    """
                ).df()
                rng = con.execute(
                    "SELECT MIN(effective_date), MAX(effective_date) "
                    "FROM main_marts.fact_prices"
                ).fetchone()
                if rng and rng[0] is not None:
                    title = (
                        "Product Price Distribution — "
                        f"{rng[0]:%b %Y} to {rng[1]:%b %Y}"
                    )
                else:
                    title = "Product Price Distribution — All Available Data"

        if df.empty:
            return _empty("No price data available for the box plot.")

        # Sort categories by median price descending.
        order = (
            df.groupby("category")["regular_price"]
            .median()
            .sort_values(ascending=False)
            .index.tolist()
        )

        fig = px.box(
            df,
            x="category",
            y="regular_price",
            color="category",
            category_orders={"category": order},
            title=title,
            labels={"category": "Category", "regular_price": "Regular Price ($)"},
        )
        fig.update_layout(showlegend=False, xaxis_tickangle=-30, **LIGHT_LAYOUT)
        fig.update_yaxes(title_text="Regular Price ($)")
        return dcc.Graph(figure=fig, style={"height": "460px"})
    except Exception as exc:  # noqa: BLE001
        LOG.error("Chart 1 (box) failed: %s", exc)
        return _empty(f"Price distribution chart unavailable: {exc}")


# ---------------------------------------------------------------------------
# Chart 2 — Fulfillment & Pricing Map (choropleth + scatter overlay)
# ---------------------------------------------------------------------------
def build_map_chart() -> "dcc.Graph | dbc.Alert":
    """Combined choropleth (state avg price) + store-location scatter overlay."""
    try:
        state_sql = """
            SELECT state,
                   ROUND(AVG(avg_price), 2) AS state_avg_price,
                   SUM(total_products)       AS state_total_products
            FROM main_marts.mart_location_sales
            WHERE state IS NOT NULL
            GROUP BY state
        """
        # Pull zip_code from dim_locations via join so this works whether or
        # not mart_location_sales has been rebuilt with the new zip_code column.
        loc_sql = """
            SELECT m.location_id, m.name, m.city, m.state,
                   dl.zip_code,
                   m.latitude, m.longitude,
                   m.physical_count, m.online_count,
                   m.total_products, m.avg_price
            FROM main_marts.mart_location_sales m
            LEFT JOIN main_marts.dim_locations dl
                   ON m.location_id = dl.location_id
            WHERE m.latitude IS NOT NULL AND m.longitude IS NOT NULL
        """
        with db_connect() as con:
            state_df = con.execute(state_sql).df()
            loc_df = con.execute(loc_sql).df()

        if loc_df.empty:
            return _empty("No store location data available for the map.")

        loc_df["zip_code"] = loc_df.get("zip_code", pd.Series(dtype="object"))
        loc_df["zip_code"] = loc_df["zip_code"].fillna("N/A")

        # Shared color range so choropleth and scatter use one visual scale.
        price_vals = pd.concat(
            [state_df["state_avg_price"], loc_df["avg_price"]], ignore_index=True
        ).dropna()
        cmin = float(price_vals.min()) if not price_vals.empty else None
        cmax = float(price_vals.max()) if not price_vals.empty else None

        hover_text = [
            (
                f"<b>{row['name']}</b><br>"
                f"{row['city']}, {row['state']}  ·  ZIP {row['zip_code']}<br>"
                f"Avg catalog price: ${row['avg_price']:.2f}<br>"
                f"Unique products in catalog: {int(row['total_products'])}<br>"
                f"<br>"
                f"Physical (in-store or curbside): {int(row['physical_count'])} products<br>"
                f"Online (delivery or ship-to-home): {int(row['online_count'])} products<br>"
                f"<i>(A product may be counted in both channels)</i>"
            )
            for _, row in loc_df.iterrows()
        ]

        fig = go.Figure()

        if not state_df.empty:
            fig.add_trace(
                go.Choropleth(
                    locations=state_df["state"],
                    z=state_df["state_avg_price"],
                    locationmode="USA-states",
                    colorscale="deep",
                    zmin=cmin,
                    zmax=cmax,
                    marker_line_color="#566573",
                    colorbar=dict(title="Avg Price ($)", x=1.0),
                    hovertext=state_df["state"]
                    + ": $"
                    + state_df["state_avg_price"].round(2).astype(str),
                    hoverinfo="text",
                    name="State avg price",
                )
            )

        # Scale bubble sizes relative to the largest store catalog.
        max_products = max(int(loc_df["total_products"].max()), 1)
        sizes = 8 + (loc_df["total_products"] / max_products) * 30

        fig.add_trace(
            go.Scattergeo(
                lat=loc_df["latitude"],
                lon=loc_df["longitude"],
                mode="markers",
                marker=dict(
                    size=sizes,
                    color=loc_df["avg_price"],
                    colorscale="Blues_r",
                    cmin=cmin,
                    cmax=cmax,
                    line=dict(width=0.8, color="#0d0d0d"),
                    showscale=False,
                    sizemode="diameter",
                ),
                text=hover_text,
                hoverinfo="text",
                name="Store",
            )
        )

        fig.update_layout(
            title="Store Fulfillment KPIs & Avg Price by Location",
            geo_scope="usa",
            geo=LIGHT_GEO,
            **LIGHT_LAYOUT,
        )
        return dcc.Graph(figure=fig, style={"height": "600px"})
    except Exception as exc:  # noqa: BLE001
        LOG.error("Chart 2 (map) failed: %s", exc)
        return _empty(f"Fulfillment & pricing map unavailable: {exc}")


# ---------------------------------------------------------------------------
# Chart 3 — Stacked bar: stock availability mix by category
# ---------------------------------------------------------------------------
def build_chart_3() -> "dcc.Graph | dbc.Alert":
    """Are products in stock? Stock-status mix per category (% of items)."""
    try:
        sql = """
            SELECT category, stock_level, COUNT(*) AS n
            FROM main_marts.fact_prices
            WHERE stock_level IS NOT NULL AND category IS NOT NULL
            GROUP BY category, stock_level
        """
        with db_connect() as con:
            df = con.execute(sql).df()

        if df.empty:
            return _empty("No stock-availability data available.")

        label_map = {
            "HIGH": "In stock (high)",
            "LOW": "Low stock",
            "TEMPORARILY_OUT_OF_STOCK": "Out of stock",
        }
        df["status"] = df["stock_level"].map(label_map).fillna(df["stock_level"])

        # Convert to share-of-category so categories compare fairly.
        df["pct"] = df["n"] / df.groupby("category")["n"].transform("sum") * 100

        # Order categories by how often they are out of stock (worst first).
        oos = (
            df[df["stock_level"] == "TEMPORARILY_OUT_OF_STOCK"]
            .set_index("category")["pct"]
            .reindex(df["category"].unique())
            .fillna(0)
            .sort_values(ascending=False)
        )

        fig = px.bar(
            df,
            x="pct",
            y="category",
            color="status",
            orientation="h",
            category_orders={
                "category": oos.index.tolist(),
                "status": ["Out of stock", "Low stock", "In stock (high)"],
            },
            color_discrete_map={
                "In stock (high)": "#27ae60",
                "Low stock": "#f6c23e",
                "Out of stock": "#e74c3c",
            },
            title="Stock Availability by Category",
            labels={"pct": "Share of items (%)", "category": "Category",
                    "status": "Status"},
            custom_data=["n"],
        )
        fig.update_traces(
            hovertemplate="%{y}<br>%{fullData.name}: %{x:.1f}%"
            " (%{customdata[0]} items)<extra></extra>"
        )
        fig.update_layout(barmode="stack", legend_title_text="Status",
                          **LIGHT_LAYOUT)
        fig.update_xaxes(range=[0, 100], ticksuffix="%")
        return dcc.Graph(figure=fig, style={"height": "460px"})
    except Exception as exc:  # noqa: BLE001
        LOG.error("Chart 3 (stock) failed: %s", exc)
        return _empty(f"Stock availability chart unavailable: {exc}")


# ---------------------------------------------------------------------------
# Chart 4 — Bar: average discount depth by category
# ---------------------------------------------------------------------------
def build_chart_4() -> "dcc.Graph | dbc.Alert":
    """Where are the deepest deals? Average promo discount per category."""
    try:
        sql = """
            SELECT category,
                   ROUND(AVG(discount_pct), 1) AS avg_discount_pct,
                   ROUND(AVG(regular_price), 2) AS avg_regular_price,
                   COUNT(*)                     AS promo_items
            FROM main_marts.fact_prices
            WHERE discount_pct > 0 AND category IS NOT NULL
            GROUP BY category
            HAVING COUNT(*) >= 5
            ORDER BY avg_discount_pct DESC
        """
        with db_connect() as con:
            df = con.execute(sql).df()

        if df.empty:
            return _empty("No promotional-discount data available.")

        fig = px.bar(
            df,
            x="avg_discount_pct",
            y="category",
            orientation="h",
            color="avg_discount_pct",
            color_continuous_scale="Sunset",
            title="Average Promotional Discount Depth by Category",
            labels={"avg_discount_pct": "Avg discount (%)", "category": "Category"},
            custom_data=["avg_regular_price", "promo_items"],
        )
        fig.update_traces(
            hovertemplate="%{y}<br>Avg discount: %{x:.1f}%<br>"
            "Avg regular price: $%{customdata[0]:.2f}<br>"
            "Promo items: %{customdata[1]}<extra></extra>"
        )
        fig.update_yaxes(categoryorder="total ascending")
        fig.update_layout(coloraxis_showscale=False, **LIGHT_LAYOUT)
        fig.update_xaxes(ticksuffix="%")
        return dcc.Graph(figure=fig, style={"height": "440px"})
    except Exception as exc:  # noqa: BLE001
        LOG.error("Chart 4 (discount) failed: %s", exc)
        return _empty(f"Discount-depth chart unavailable: {exc}")


# ---------------------------------------------------------------------------
# Chart 5 — Bubble scatter: category value map (base price vs. discount depth)
# ---------------------------------------------------------------------------
def build_chart_5() -> "dcc.Graph | dbc.Alert":
    """Category value map: base price vs. promo discount depth."""
    try:
        sql = """
            SELECT
                category,
                ROUND(AVG(regular_price), 2)                                              AS avg_regular_price,
                ROUND(AVG(CASE WHEN discount_pct > 0 THEN discount_pct ELSE NULL END), 1) AS avg_discount_pct,
                COUNT(DISTINCT product_id)                                                AS product_count,
                COUNT(CASE WHEN promo_price IS NOT NULL
                           AND promo_price < regular_price THEN 1 END)                   AS promo_items
            FROM main_marts.fact_prices
            WHERE category IS NOT NULL AND regular_price > 0
            GROUP BY category
            HAVING COUNT(*) >= 5
            ORDER BY avg_regular_price DESC
        """
        with db_connect() as con:
            df = con.execute(sql).df()

        if df.empty:
            return _empty("No category value data available.")

        fig = px.scatter(
            df,
            x="avg_regular_price",
            y="avg_discount_pct",
            size="product_count",
            color="avg_regular_price",
            color_continuous_scale="Magenta",
            text="category",
            title="Category Value Map — Base Price vs. Discount Depth",
            labels={
                "avg_regular_price": "Avg Regular Price ($)",
                "avg_discount_pct": "Avg Discount When on Promo (%)",
                "product_count": "Number of Products",
            },
            custom_data=["promo_items", "product_count"],
        )
        fig.update_traces(
            textposition="top center",
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Avg price: $%{x:.2f}<br>"
                "Avg discount: %{y:.1f}%<br>"
                "Products: %{customdata[1]}<br>"
                "Promo items: %{customdata[0]}<extra></extra>"
            ),
        )
        fig.update_layout(coloraxis_showscale=False, **LIGHT_LAYOUT)

        # Add light reference lines through the medians to create quadrant context
        fig.add_hline(y=df["avg_discount_pct"].median(), line_dash="dot",
                      line_color="rgba(100,100,100,0.4)", annotation_text="Median discount")
        fig.add_vline(x=df["avg_regular_price"].median(), line_dash="dot",
                      line_color="rgba(100,100,100,0.4)", annotation_text="Median price")
        return dcc.Graph(figure=fig, style={"height": "440px"})
    except Exception as exc:  # noqa: BLE001
        LOG.error("Chart 5 (category value map) failed: %s", exc)
        return _empty(f"Category value map unavailable: {exc}")


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
def _loading(child):
    """Wrap a chart output in a circular loading spinner."""
    return dcc.Loading(type="circle", children=child)


def _header() -> dbc.Row:
    return dbc.Row(
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H2(
                        "Kroger Product Analytics",
                        className="mb-1 fw-bold",
                        style={"color": "#FFFFFF"},
                    ),
                    html.P(
                        "Store operations insights — product pricing, availability, and fulfillment",
                        className="mb-0",
                        style={"color": "#A9CCE3", "fontSize": "0.95rem"},
                    ),
                    html.A(
                        "dbt Docs ↗",
                        href="/dbt-kro/",
                        target="_blank",
                        style={"fontSize": "12px", "color": C["blue"], "display": "block", "marginBottom": "6px", "textDecoration": "none"},
                    ),
                ]),
                style={
                    "backgroundColor": "#003087",
                    "borderRadius": "8px",
                    "border": "none",
                },
            ),
            width=12,
        ),
        className="mt-3 mb-3",
    )


def create_layout() -> dbc.Container:
    return dbc.Container(
        [
            _header(),
            # Row 1: box plot (wide) + stock availability
            dbc.Row(
                [
                    dbc.Col(_loading(build_box_chart()), lg=7, md=12),
                    dbc.Col(_loading(build_chart_3()), lg=5, md=12),
                ],
                className="mb-2 g-3",
            ),
            # Row 2: full-width map
            dbc.Row(
                dbc.Col(_loading(build_map_chart()), width=12),
                className="mb-2 g-3",
            ),
            # Row 3: discount depth + fulfillment channel mix
            dbc.Row(
                [
                    dbc.Col(_loading(build_chart_4()), lg=6, md=12),
                    dbc.Col(_loading(build_chart_5()), lg=6, md=12),
                ],
                className="mb-4 g-3",
            ),
        ],
        fluid=True,
        className="py-2",
    )


# ---------------------------------------------------------------------------
# App factory & entry point
# ---------------------------------------------------------------------------
def create_app() -> Dash:
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.FLATLY],
        title="Kroger Product Analytics",
    )
    app.layout = create_layout  # function ref so layout rebuilds per request
    return app


# Module-level instances for gunicorn: `portfolio_app.app:server`
app = create_app()
server = app.server


def main() -> None:
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8050)))


if __name__ == "__main__":
    main()

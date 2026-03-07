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
from io import BytesIO as io

# Third party imports
from minio import Minio as mc
from dash import Dash, html, dcc, Input, Output, no_update
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
    run_churn_analysis as ca
)
from portfolio_app.scripts.constants import (
    DB_PATH, LOG, MINIO_BUCKET_NAME, MINIO_ENDPOINT, MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD, MINIO_USE_SSL, DBT_PROFILES_DIR
)


# Add the parent directory to PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Resolves the path to ensure correctness
DB_PATH = DB_PATH.resolve()

# Print the path for debugging
LOG.info("Database path: %s", DB_PATH)

# Verifies that the path exists
if not DB_PATH.exists():
    raise FileNotFoundError(f"Database file not found at: {DB_PATH}")


def header():
    """Render the app header."""
    return html.Div([
        html.H1("Bruce's Analytics Portfolio"),
        html.H5("An app showcasing customer analytics across modalities."),
        html.Hr()
    ])


def minio_client():
    """Get or create MinIO client."""
    return mc(endpoint=MINIO_ENDPOINT,
              access_key=MINIO_ROOT_USER,
              secret_key=MINIO_ROOT_PASSWORD,
              secure=MINIO_USE_SSL)


@functools.lru_cache(maxsize=32)
def load_from_s3(bucket_name, file_name):
    """Load a Parquet file from MinIO."""
    try:
        client = minio_client()
        response = client.get_object(bucket_name, file_name)
        df = pd.read_parquet(io(response.read()))
        LOG.info("Successfully loaded %s from MinIO.", file_name)
        return df
    except Exception as e:
        LOG.error("Error loading %s from MinIO: %s", file_name, str(e))
        raise


# Connect to DuckDB
def ddb_connect():
    """Create and return a DuckDB connection."""
    try:
        con = ddb.connect(str(DB_PATH))
        LOG.info("Database connection successful!")
        return con
    except Exception as e:
        LOG.error("Error connecting to database: %s", str(e))
        raise


def run_dbt(df: pd.DataFrame):
    """Run dbt transformations on a Parquet dataframe."""
    try:
        con = ddb.connect(str(DB_PATH))
        con.register('parquet_data', df)
        rdops()  # Run dbt operations
        LOG.info("dbt operations completed successfully!")
    except Exception as e:
        LOG.error("Error running dbt operations: %s", str(e))
        raise


def render_buttons():
    """Render top action buttons."""
    resume_url = ("https://github.com/Brucelee352/Product_data_pipelining/blob/"
                  "e0b968643ea3455fc5368490c73133f6fb70ac37/misc/BruceLee_2025Resume_b.pdf")

    return dbc.Row([
        dbc.Col(
            dbc.Button(
                "Repository",
                href="https://github.com/brucelee352/Product_data_pipelining",
                external_link=True, color="primary", className="w-100",
                title="This project's GitHub repository"
            ), width=2
        ),
        dbc.Col(
            dbc.Button(
                "LinkedIn",
                href="https://www.linkedin.com/in/brucealee/",
                external_link=True, color="primary", className="w-100",
                title="My LinkedIn profile"
            ), width=2
        ),
        dbc.Col(
            dbc.Button(
                "Resume",
                href=resume_url,
                external_link=True, color="primary", className="w-100",
                title="Click to view my resume"
            ), width=2
        ),
        dbc.Col(
            dbc.Button(
                "Refresh", id="refresh-button",
                color="primary", className="w-100",
                title="Rerun the data pipeline's models"
            ), width=2
        ),
        dbc.Col(
            dbc.Button(
                "Download Data", id="download-button",
                color="primary", className="w-100",
                title="Download cleaned data as CSV"
            ), width=2
        ),
    ], className="mb-3 g-2", justify="start")


def refresh_pipeline():
    """Run data pipeline operations and return a status message."""
    try:
        df = load_from_s3(MINIO_BUCKET_NAME, 'cleaned_data.parquet')
        run_dbt(df)
        return "Tables materialized successfully!"
    except Exception as e:
        return f"Error modelling tables: {str(e)}"


def download_data():
    """Return CSV data for download."""
    df = load_from_s3(MINIO_BUCKET_NAME, 'cleaned_data.parquet')
    return df.to_csv(index=False)


def build_churn_chart(con):
    """Build the churn analysis line chart."""
    try:
        churn_analysis = ca(con=con)
        if churn_analysis.empty:
            return dbc.Alert("No data found for churn analysis.", color="warning")
        fig = px.line(
            churn_analysis,
            x='cohort_month',
            y='churn_rate',
            color='cohort_size',
            text='avg_days_to_churn',
            title='Churn Analysis by Cohort',
            height=600
        )
        fig.update_layout(
            xaxis_title='Cohort Month', yaxis_title='Churn Rate',
            legend_title='Cohort Size')
        return dcc.Graph(figure=fig)
    except Exception as e:
        return dbc.Alert(f"Error fetching churn analysis data: {e}", color="danger")


def build_purchase_chart(con):
    """Build the purchase analysis scatter chart."""
    try:
        purchase_analysis = pa(con=con)
        if purchase_analysis.empty:
            return dbc.Alert("No data found for purchase analysis.", color="warning")
        fig = px.scatter(
            purchase_analysis,
            x='month',
            y='total_revenue',
            size='total_revenue',
            hover_data=['product_name', 'total_revenue', 'avg_price'],
            color='price_tier',
            title='Revenue by Month, Product',
            opacity=0.7,
            log_x=True,
            width=500,
            height=500
        )
        fig.update_layout(
            xaxis_title='Month',
            yaxis_title='Revenue',
            legend_title='Price Tier'
        )
        return dcc.Graph(figure=fig)
    except Exception as e:
        return dbc.Alert(f"Error fetching purchase analysis data: {e}", color="danger")


def build_lifecycle_chart(con):
    """Build the lifecycle analysis pie chart."""
    try:
        lifecycle_analysis = la(con=con)
        if lifecycle_analysis.empty:
            return dbc.Alert("No data found for lifecycle analysis.", color="warning")
        df_grouped = lifecycle_analysis.groupby(
            'os', as_index=False).agg({
                'total_revenue': 'sum',
                'total_customers': 'sum',
                'total_purchases': 'sum'
            })
        fig = px.pie(
            df_grouped,
            names='os',
            values='total_revenue',
            title='Total Revenue by Operating System',
            hole=0.2,
            color_discrete_sequence=[
                "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]
        )
        fig.update_traces(opacity=0.75)
        fig.update_layout(
            height=400,
            width=400,
            margin=dict(t=50, l=0, r=0, b=0),
            legend_title='Operating System'
        )
        return dcc.Graph(figure=fig)
    except Exception as e:
        return dbc.Alert(f"Error creating lifecycle analysis chart: {e}", color="danger")


def build_demographics_chart(con):
    """Build the demographics analysis bar chart."""
    try:
        demo_analysis = da(con=con)
        if demo_analysis.empty:
            return dbc.Alert("No data found for demographics analysis.", color="warning")
        fig = px.bar(
            demo_analysis,
            x='product_name',
            y='unique_users',
            color='browser',
            barmode='relative',
            hover_data=['avg_purchase_value', 'avg_session_duration'],
            title='Usage by Product and Browser',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig.update_layout(
            xaxis_title='Product Name',
            yaxis_title='Unique Users',
            legend_title='Browser',
            height=500
        )
        return dcc.Graph(figure=fig)
    except Exception as e:
        LOG.error("Error fetching demographics analysis data: %s", str(e))
        return dbc.Alert(f"Error fetching demographics analysis data: {e}", color="danger")


def build_business_chart(con):
    """Build the business analysis bar chart."""
    try:
        business_analysis = ba(con=con)
        if business_analysis.empty:
            return dbc.Alert("No data found for business analysis.", color="warning")
        fig = px.bar(
            business_analysis,
            x='device_type',
            y='unique_users',
            hover_data=['total_sessions', 'avg_session_duration'],
            color='conversion_rate',
            color_continuous_scale='Geyser'
        )
        fig.update_layout(
            title='Users by Device Type',
            xaxis_title='Device Type',
            yaxis_title='Unique Users',
            legend_title='Conversion Rate',
            height=600,
            width=800
        )
        return dcc.Graph(figure=fig)
    except Exception as e:
        LOG.error("Error fetching business analysis data: %s", str(e))
        return dbc.Alert(f"Error fetching business analysis data: {e}", color="danger")


def build_engagement_chart(con):
    """Build the engagement analysis bar chart."""
    try:
        engagement_analysis = ea(con=con)
        if engagement_analysis.empty:
            return dbc.Alert("No data found for engagement analysis.", color="warning")
        fig = px.bar(
            engagement_analysis,
            x='hour',
            y='revenue',
            color='hour',
            hover_data=['total_sessions', 'avg_session_duration'],
            height=400,
            barmode='relative',
            color_continuous_scale='Turbo',
            log_y=True
        )
        fig.update_layout(
            title='Engagement per Hour',
            xaxis_title='Hour',
            yaxis_title='Revenue',
            legend_title='Hour',
            xaxis={'categoryorder': 'total ascending'}
        )
        return dcc.Graph(figure=fig)
    except Exception as e:
        LOG.error("Error fetching engagement analysis data: %s", str(e))
        return dbc.Alert(f"Error fetching engagement analysis data: {e}", color="danger")


def render_revenue_charts():
    """Render revenue-focused charts."""
    with ddb_connect() as con:
        chart1 = build_churn_chart(con)
        chart2 = build_purchase_chart(con)
        chart3 = build_lifecycle_chart(con)

    return html.Div([
        dbc.Row([
            dbc.Col(chart1, width=8),
            dbc.Col(chart2, width=4),
        ]),
        html.Hr(),
        chart3
    ])


def render_user_charts():
    """Render user-centric charts."""
    with ddb_connect() as con:
        chart4 = build_demographics_chart(con)
        chart5 = build_business_chart(con)
        chart6 = build_engagement_chart(con)

    return html.Div([
        dbc.Row([
            dbc.Col(chart4, width=8),
            dbc.Col(chart5, width=4),
        ]),
        html.Hr(),
        chart6
    ])


def render_charts():
    """Render all charts in tabs."""
    return dcc.Tabs([
        dcc.Tab(label='Revenue Centric', children=[render_revenue_charts()]),
        dcc.Tab(label='User Centric', children=[render_user_charts()])
    ])


def register_callbacks(app):
    """Register Dash callbacks for interactive elements."""

    @app.callback(
        Output("refresh-status", "children"),
        Input("refresh-button", "n_clicks"),
        prevent_initial_call=True
    )
    def handle_refresh(n_clicks):
        if n_clicks:
            msg = refresh_pipeline()
            if msg.startswith("Error"):
                return dbc.Alert(msg, color="danger", dismissable=True, duration=4000)
            return dbc.Alert(msg, color="success", dismissable=True, duration=4000)
        return no_update

    @app.callback(
        Output("download-csv", "data"),
        Input("download-button", "n_clicks"),
        prevent_initial_call=True
    )
    def handle_download(n_clicks):
        if n_clicks:
            csv_data = download_data()
            return dict(content=csv_data, filename="cleaned_data.csv", type="text/csv")
        return no_update


def create_app():
    """Create and configure the Dash application."""
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        title="Bruce's Analytics Portfolio"
    )

    app.layout = dbc.Container([
        header(),
        render_buttons(),
        dcc.Download(id="download-csv"),
        html.Div(id="refresh-status"),
        render_charts()
    ], fluid=True)

    register_callbacks(app)
    return app


def main():
    """Executes the overall app."""
    try:
        os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_DIR)
        app = create_app()
        app.run(debug=True, host="0.0.0.0", port=8050)
    except Exception as e:
        LOG.error("Application error: %s", str(e))
        raise


if __name__ == "__main__":
    main()

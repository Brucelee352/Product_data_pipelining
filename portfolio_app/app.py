"""
#----------------------------------------------------------------#

Bruce's Analytics Portfolio App

This app (powered by streamlit) showcases customer analytics
across modalities.

Please refer to README.md file for more information.

All dependencies here are installed via requirements.txt
when deployed.

#----------------------------------------------------------------#

"""

# Standard library imports
import os
import sys
from pathlib import Path
from io import BytesIO as io
import time
# Third party imports
from minio import Minio as mc
import streamlit as st
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

# Configuration and setup


st.set_page_config(
    page_icon='📊',
    layout: Layout='wide',
    initial_sidebar_state='collapsed')


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
    container = st.container()
    with container:
        st.title("Bruce's Analytics Portfolio")
        st.subheader("An app showcasing customer analytics across modalities.")

    st.divider()


def minio_client():
    """Get or create MinIO client."""
    return mc(endpoint=MINIO_ENDPOINT,
              access_key=MINIO_ROOT_USER,
              secret_key=MINIO_ROOT_PASSWORD,
              secure=MINIO_USE_SSL)


@st.cache_data(show_spinner=False)
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


@st.cache_data(show_spinner=False)
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
    container = st.container()
    with container:
        cols = st.columns([0.5, 0.5, 0.5, 0.5, 0.5],
                          gap="small", vertical_alignment="center")

    # Repository link
    with cols[0]:
        st.link_button(
            label="Repository",
            url="https://github.com/brucelee352/Product_data_pipelining",
            icon="💾",
            help="This project's GitHub repository",
            type="primary"
        )
    # LinkedIn link
    with cols[1]:
        st.link_button(
            label="LinkedIn",
            url="https://www.linkedin.com/in/brucealee/",
            icon="👔",
            help="My LinkedIn profile",
            type="primary"
        )
    # Resume link
    resume_url = ("https://github.com/Brucelee352/Product_data_pipelining/blob/"
                  "e0b968643ea3455fc5368490c73133f6fb70ac37/misc/BruceLee_2025Resume_b.pdf")
    with cols[2]:
        st.link_button(
            label="Resume",
            url=resume_url,
            icon="📄",
            help="Click to view my resume",
            type="primary"
        )
    # Refresh button with transient message
    with cols[3]:  # Dedicated container for refresh messages
        if st.button(
                label="Refresh",
                key="refresh_button",
                icon="🔄",
                type="primary",
                help="Rerun the data pipeline's models"):
            refresh_pipeline()

        # Download button with transient message
    with cols[4]:
        download_msg_container = st.empty()  # Dedicated container for download messages
        csv_data = download_data()
        if st.download_button(
            label="Download Data",
            key="download_button",
            icon="📥",
            data=csv_data,
            file_name="cleaned_data.csv",
            mime="text/csv",
            type="primary"
        ):
            download_msg_container.empty()


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
    return df.to_csv()


def render_revenue_charts(con):
    """Render revenue-focused charts."""
    col1, col2 = st.columns([2, 1])
    with col1:
        try:
            churn_analysis = ca(con=con)
            if churn_analysis.empty:
                st.warning("No data found for churn analysis.")
            else:
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
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error fetching churn analysis data: {e}")

    with col2:
        try:
            purchase_analysis = pa(con=con)
            if purchase_analysis.empty:
                st.warning("No data found for purchase analysis.")
            else:
                fig2 = px.scatter(
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
                fig2.update_layout(
                    xaxis_title='Month',
                    yaxis_title='Revenue',
                    legend_title='Price Tier'
                )
                st.plotly_chart(fig2, use_container_width=True)
        except Exception as e:
            st.error(f"Error fetching purchase analysis data: {e}")

    st.divider()

    try:
        lifecycle_analysis = la(con=con)
        if lifecycle_analysis.empty:
            st.warning("No data found for lifecycle analysis.")
        else:
            df_grouped = lifecycle_analysis.groupby(
                'os', as_index=False).agg({
                    'total_revenue': 'sum',
                    'total_customers': 'sum',
                    'total_purchases': 'sum'
                })
            fig3 = px.pie(
                df_grouped,
                names='os',
                values='total_revenue',
                title='Total Revenue by Operating System',
                hole=0.2,
                color_discrete_sequence=[
                    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]
            )
            fig3.update_traces(opacity=0.75)
            fig3.update_layout(
                height=400,
                width=400,
                margin=dict(t=50, l=0, r=0, b=0),
                legend_title='Operating System'
            )
            st.plotly_chart(fig3, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating lifecycle analysis chart: {e}")


def render_user_charts(con):
    """Render user-centric charts."""
    col1, col2 = st.columns([2, 1])
    with col1:  # Chart 4
        try:
            demo_analysis = da(con=con)
            if demo_analysis.empty:
                st.warning("No data found for demographics analysis.")
            else:
                fig3 = px.bar(
                    demo_analysis,
                    x='product_name',
                    y='unique_users',
                    color='browser',
                    barmode='relative',
                    hover_data=['avg_purchase_value', 'avg_session_duration'],
                    title='Usage by Product and Browser',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig3.update_layout(
                    xaxis_title='Product Name',
                    yaxis_title='Unique Users',
                    legend_title='Browser',
                    height=500
                )
                st.plotly_chart(fig3, use_container_width=True)
        except Exception as e:
            st.error(
                LOG.error("Error fetching demographics analysis data: %s", str(e)))
    with col2:  # Chart 5
        try:
            business_analysis = ba(con=con)
            if business_analysis.empty:
                st.warning("No data found for business analysis.")
            else:
                fig4 = px.bar(
                    business_analysis,
                    x='device_type',
                    y='unique_users',
                    hover_data=['total_sessions', 'avg_session_duration'],
                    color='conversion_rate',
                    color_continuous_scale='Geyser'
                )
                fig4.update_layout(
                    title='Users by Device Type',
                    xaxis_title='Device Type',
                    yaxis_title='Unique Users',
                    legend_title='Conversion Rate',
                    height=600,
                    width=800
                )
                st.plotly_chart(fig4, use_container_width=True)
        except Exception as e:
            st.error(LOG.error("Error fetching business analysis data: %s", str(e)))
    st.divider()
    try:  # Chart 6
        engagement_analysis = ea(con=con)
        if engagement_analysis.empty:
            st.warning("No data found for engagement analysis.")
        else:
            fig5 = px.bar(
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
            fig5.update_layout(
                title='Engagement per Hour',
                xaxis_title='Hour',
                yaxis_title='Revenue',
                legend_title='Hour',
                xaxis={'categoryorder': 'total ascending'}
            )
            st.plotly_chart(fig5, use_container_width=True)
    except Exception as e:
        st.error(LOG.error("Error fetching engagement analysis data: %s", str(e)))


def render_charts():
    """Render all charts in tabs."""
    with ddb_connect() as con:
        tab1, tab2 = st.tabs(['Revenue Centric', 'User Centric'])
        with tab1:
            render_revenue_charts(con)
        with tab2:
            render_user_charts(con)


def app():
    """Main application flow."""
    header()
    render_buttons()
    render_charts()


def main():
    """Executes the overall app."""
    try:
        app()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        LOG.error("Application error: %s", str(e))


if __name__ == "__main__":
    os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_DIR)
    main()

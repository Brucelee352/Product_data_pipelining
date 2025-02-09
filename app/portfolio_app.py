
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
import sys
from pathlib import Path
from io import BytesIO as io

# Third party imports
import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb as ddb
from minio import Minio as s3

# Local imports from data pipeline, for dbt operations
from scripts.main_data_pipeline import run_dbt_ops as rdops

# Local imports for queries
from scripts.analytics_queries import (
    run_lifecycle_analysis as la,
    run_purchase_analysis as pa,
    run_demographics_analysis as da,
    run_business_analysis as ba,
    run_engagement_analysis as ea,
    run_churn_analysis as ca)

# Local imports for constants
from scripts.constants import (
    DB_PATH, LOG, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET_NAME, MINIO_USE_SSL
)


# Resolves the path to ensure correctness
DB_PATH = DB_PATH.resolve()

# Print the path for debugging
LOG.info("Database path: %s", DB_PATH)

# Verifies that the path exists
if not DB_PATH.exists():
    raise FileNotFoundError(f"Database file not found at: {DB_PATH}")

# MinIO configuration
S3_CONFIG = {
    'endpoint': f'{MINIO_ENDPOINT}',
    'access_key': f'{MINIO_ACCESS_KEY}',
    'secret_key': f'{MINIO_SECRET_KEY}',
    'bucket': f'{MINIO_BUCKET_NAME}',
    'use_ssl': f'{MINIO_USE_SSL}',
}

# Initialize MinIO client
client = s3(
    endpoint=S3_CONFIG['endpoint'],
    access_key=S3_CONFIG['access_key'],
    secret_key=S3_CONFIG['secret_key'],
    secure=False
)

# Loads a Parquet file from S3


@st.cache_data
def load_from_s3(bucket_name, file_name):
    """Load a Parquet file from MinIO."""
    try:
        response = client.get_object(bucket_name, file_name)
        df = pd.read_parquet(io(response.read()))
        LOG.info("Successfully loaded %s from MinIO.", file_name)
        return df
    except Exception as e:
        LOG.error("Error loading %s from MinIO: %s", file_name, str(e))
        raise


# Runs dbt operations on Parquet data
def run_dbt(df):
    """Run dbt transformations on a Parquet dataframe."""
    try:
        with ddb.connect(str(DB_PATH)) as con:
            con.register('parquet_data', df)
            rdops()  # Run dbt operations
        LOG.info("dbt operations completed successfully!")
    except Exception as e:
        LOG.error("Error running dbt operations: %s", str(e))
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


# Streamlit configuration
st.set_page_config(
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='collapsed'
)

# Title and Subheader
st.title('Bruce\'s Analytics Portfolio')
st.subheader('An app showcasing customer analytics across modalities.')

st.divider()

# Columns for buttons
col1, col2, col3, col4, col5 = st.columns([0.5, 0.5, 0.5, 0.5, 0.5],
                                          gap="small",
                                          vertical_alignment="center")

# Links to GitHub repository and LinkedIn profile
with col1:
    st.link_button(label="Repository",
                   url="https://github.com/brucelee352/Product_data_pipelining",
                   icon="💾",
                   help="This project's GitHub repository",
                   type="primary")
with col2:
    st.link_button(label="LinkedIn",
                   url="https://www.linkedin.com/in/brucealee/",
                   icon="👔",
                   help="My LinkedIn profile",
                   type="primary")

# Link to resume
url = ("https://github.com/Brucelee352/Product_data_pipelining/blob/e0b968643ea3455fc5368490c73133f6fb70ac37/misc/BruceLee_2025Resume_b.pdf")
with col3:
    st.link_button(label="Resume",
                   url=url,
                   icon="📄",
                   help="Click to open my resume in a new tab",
                   type="primary")

# Runs dbt to rematerialize the tables and loads their data
with col4:
    if st.button(label="Refresh",
                 type="secondary",
                 help="Rerun the data pipeline's models"):
        with st.spinner("Re-running data materialization..."):
            try:
                # Load Parquet file from MinIO
                df = load_from_s3(S3_CONFIG['bucket'], 'cleaned_data.parquet')
                run_dbt(df)
                st.success("Data pipeline completed successfully!")
            except FileNotFoundError as e:
                st.error(LOG.error("Error running data pipeline: %s", str(e)))
            except ValueError as e:
                st.error(LOG.error("Error running data pipeline: %s", str(e)))

# Gives the user the option to download pipeline output
with col5:
    df = load_from_s3(S3_CONFIG['bucket'], 'cleaned_data.parquet')
    if st.download_button(label="Download Data",
                          data=df.to_csv(),
                          file_name="cleaned_data.csv",
                          mime="text/csv"):
        with st.spinner("Downloading data..."):
            try:
                st.success("Data downloaded successfully!")
            except Exception as e:
                st.error(LOG.error("Error downloading data: %s", str(e)))

# Main application


@st.cache_data
def app():
    """Carries out website functions."""
    with ddb_connect() as con:
        tab1, tab2 = st.tabs(['Revenue Centric', 'User Centric'])
        # Revenue Centric data
        with tab1:  # Chart 1
            col1, col2 = st.columns([2, 1])
            with col1:
                try:
                    churn_analysis = ca(con=con)
                    if churn_analysis.empty:
                        st.warning("No data found for churn analysis.")
                    else:
                        fig6 = px.line(
                            churn_analysis,
                            x='cohort_month',
                            y='churn_rate',
                            color='cohort_size',
                            text='avg_days_to_churn',
                            title='Churn Analysis by Cohort',
                            height=600,
                            line_group="cohort_size"
                        )
                        fig6.update_layout(
                            xaxis_title='Cohort Month',
                            yaxis_title='Churn Rate',
                            legend_title='Cohort Size'
                        )
                        st.plotly_chart(fig6, use_container_width=True)

                except Exception as e:
                    st.error(
                        LOG.error("Error fetching churn analysis data: %s",
                                  str(e))
                    )
            with col2:  # Chart 2
                try:
                    purchase_analysis = pa(con=con)
                    if purchase_analysis.empty:
                        st.warning("No data found for purchase analysis.")
                    else:

                        fig2 = px.scatter(purchase_analysis,
                                          x='month',
                                          y='total_revenue',
                                          size='total_revenue',
                                          hover_data=['product_name',
                                                      'total_revenue',
                                                      'avg_price'],
                                          color='price_tier',
                                          title='Purchases by Product',
                                          opacity=0.7,
                                          log_x=True)
                        fig2.update_layout(
                            title=('Purchase Analysis by Product '
                                   'and Price Tier'),
                            width=500,
                            height=500,
                            xaxis_title='Month',
                            yaxis_title='Total Revenue',
                            legend_title='Price Tier',
                            xaxis={'categoryorder': 'total descending'}
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                except Exception as e:
                    st.error(
                        LOG.error(
                            "Error fetching purchase analysis data: %s", str(e)
                        )
                    )
            st.divider()
            try:  # Chart 3
                lifecycle_analysis = la(con=con)
                if lifecycle_analysis.empty:
                    st.warning("No data found for lifecycle analysis.")
                else:
                    df_grouped = lifecycle_analysis.groupby(
                        'os', as_index=False).agg(
                        {
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
                    color_discrete_sequence=["#636EFA",
                                             "#EF553B",
                                             "#00CC96",
                                             "#AB63FA",
                                             "#FFA15A"]
                )
                fig.update_traces(opacity=0.75)
                fig.update_layout(
                    height=400,
                    width=400,
                    margin=dict(t=50, l=0, r=0, b=0),
                    legend_title='Operating System'
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(LOG.error("Error creating pie chart: %s", str(e)))
        # User Centric data
        with tab2:
            col1, col2 = st.columns([2, 1])
            with col1:  # Chart 4
                try:
                    demo_analysis = da(con=con)
                    if demo_analysis.empty:
                        st.warning("No data found for demographics analysis.")
                    else:
                        fig3 = px.bar(demo_analysis,
                                      x='product_name',
                                      y='unique_users',
                                      color='browser',
                                      barmode='relative',
                                      hover_data=[
                                          'avg_purchase_value',
                                          'avg_session_duration'
                                      ],
                                      title='Usage by Product and Browser',
                                      color_discrete_sequence=(
                                          px.colors.qualitative.Pastel
                                      )
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
                        LOG.error(
                            "Error fetching demographics analysis data: %s",
                            str(e)
                        )
                    )
            with col2:  # Chart 5
                try:
                    business_analysis = ba(con=con)
                    if business_analysis.empty:
                        st.warning("No data found for business analysis.")
                    else:
                        fig4 = px.bar(business_analysis,
                                      x='device_type',
                                      y='unique_users',
                                      hover_data=['total_sessions',
                                                  'avg_session_duration'],
                                      color='conversion_rate',
                                      color_continuous_scale='Geyser')
                        fig4.update_layout(
                            title='Business Analysis by Device Type',
                            xaxis_title='Device Type',
                            yaxis_title='Unique Users',
                            legend_title='Conversion Rate',
                            height=600,
                            width=800
                        )
                        st.plotly_chart(fig4, use_container_width=True)
                except Exception as e:
                    st.error(
                        LOG.error(
                            "Error fetching business analysis data: %s", str(e)
                        )
                    )
            st.divider()
            try:  # Chart 6
                engagement_analysis = ea(con=con)
                if engagement_analysis.empty:
                    st.warning("No data found for engagement analysis.")
                else:

                    fig5 = px.bar(engagement_analysis,
                                  x='hour',
                                  y='revenue',
                                  color='hour',
                                  hover_data=['total_sessions',
                                              'avg_session_duration'],
                                  height=400,
                                  barmode='relative',
                                  color_continuous_scale='Turbo',
                                  log_y=True)
                    fig5.update_layout(
                        title='Engagement Analysis by Hour',
                        xaxis_title='Hour',
                        yaxis_title='Revenue',
                        legend_title='Hour',
                        xaxis={'categoryorder': 'total ascending'}
                    )
                    st.plotly_chart(fig5, use_container_width=True)

            except Exception as e:
                st.error(
                    LOG.error(
                        "Error fetching engagement analysis data: %s", str(e)))


def main():
    """Executes the overall app."""
    app()


if __name__ == "__main__":
    main()

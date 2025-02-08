import os
from pathlib import Path
import logging
import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb as ddb
from minio import Minio as s3
from io import BytesIO as io

# Local imports from data pipeline for dbt operations
from scripts.main_data_pipeline import run_dbt_ops as rdops


# Local imports for analytics queries
from scripts.analytics_queries import (
    run_lifecycle_analysis as la,
    run_purchase_analysis as pa,
    run_demographics_analysis as da,
    run_business_analysis as ba,
    run_engagement_analysis as ea,
    run_churn_analysis as ca)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup project paths
PROJECT_ROOT = Path(__file__).parents[1]
DBT_ROOT = PROJECT_ROOT / 'Product_data_pipelining' / 'dbt_pipeline_demo'
db_dir = DBT_ROOT / 'databases'
db_path = db_dir / 'dbt_pipeline_demo.duckdb'

# Resolve the path to ensure correctness
db_path = db_path.resolve()

# Print the path for debugging
logger.info("Database path: %s", db_path)

# Verify file exists
if not db_path.exists():
    raise FileNotFoundError(f"Database file not found at: {db_path}")

# MinIO configuration
S3_CONFIG = {
    'endpoint': os.getenv('MINIO_ENDPOINT'),
    'access_key': os.getenv('MINIO_ACCESS_KEY'),
    'secret_key': os.getenv('MINIO_SECRET_KEY'),
    'bucket': os.getenv('MINIO_BUCKET_NAME', 'sim-api-data'),
    'use_ssl': os.getenv('MINIO_USE_SSL', 'False').lower() in {'true', '1', 'yes'},
}

# Initialize MinIO client
client = s3(
    endpoint=S3_CONFIG['endpoint'],
    access_key=S3_CONFIG['access_key'],
    secret_key=S3_CONFIG['secret_key'],
    secure=S3_CONFIG['use_ssl']
)

# Load Parquet file from MinIO


@st.cache_data
def load_from_s3(bucket_name, file_name):
    """Load a Parquet file from MinIO."""
    try:
        response = client.get_object(bucket_name, file_name)
        df = pd.read_parquet(io(response.read()))
        logger.info(f"Successfully loaded {file_name} from MinIO.")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_name} from MinIO: {str(e)}")
        raise


# Run dbt operations on Parquet data

def run_dbt(df):
    """Run dbt transformations on a Parquet dataframe."""
    try:
        # Convert dataframe to DuckDB table
        with ddb.connect(str(db_path)) as con:
            con.register('parquet_data', df)
            rdops()  # Run dbt operations
        logger.info("dbt operations completed successfully!")
    except Exception as e:
        logger.error(f"Error running dbt operations: {str(e)}")
        raise

# DuckDB connection function


def ddb_connect():
    """Create and return a DuckDB connection."""
    try:
        con = ddb.connect(str(db_path))
        logger.info("Database connection successful!")
        return con
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise


# Streamlit config
st.set_page_config(
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='auto'
)

st.title('Bruce\'s Analytics Portfolio')
st.subheader('An app showcasing customer analytics across modalities.')


# Run the pipeline and load data
if st.button("Refresh Data"):
    with st.spinner("Re-running data materialization..."):
        try:
            # Load Parquet file from MinIO
            df = load_from_s3(S3_CONFIG['bucket'], 'cleaned_data.parquet')
            run_dbt(df)
            st.success("Data pipeline completed successfully!")
        except Exception as e:
            st.error(f"Error running data pipeline: {str(e)}")

# Main app


@st.cache_data
def app():
    """Carries out website functions."""
    with ddb_connect() as con:
        tab1, tab2 = st.tabs(['User Centric', 'Revenue Centric'])
        with tab1:
            col1, col2 = st.columns([2, 1])
            with col1:
                try:
                    df = da(con=con)
                    if df.empty:
                        st.warning("No data found for demographics analysis.")
                    else:
                        fig3 = px.bar(df,
                                      x='product_name',
                                      y='unique_users',
                                      color='browser',
                                      barmode='relative',
                                      hover_data=[
                                        'avg_purchase_value',
                                        'avg_session_duration'
                                      ],
                                      title='Usage by Product and Browser',
                                      color_discrete_sequence=px.colors.qualitative.Pastel)
                        fig3.update_layout(
                            xaxis_title='Product Name',
                            yaxis_title='Unique Users',
                            legend_title='Browser',
                            height=500
                        )
                        st.plotly_chart(fig3, use_container_width=True)
                except Exception as e:
                    st.error(
                        f"Error fetching demographics analysis data: {str(e)}")

            with col2:
                try:
                    df = pa(con=con)
                    if df.empty:
                        st.warning("No data found for purchase analysis.")
                    else:
                        fig2 = px.scatter(df,
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
                            title='Purchase Analysis by Product and Price Tier',
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
                        f"Error fetching purchase analysis data: {str(e)}")

            try:
                df = ea(con=con)
                if df.empty:
                    st.warning("No data found for engagement analysis.")
                else:
                    fig5 = px.bar(df,
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
                st.error(f"Error fetching engagement analysis data: {str(e)}")

        with tab2:
            col1, col2 = st.columns([2, 1])
            with col1:
                try:
                    df = la(con=con)

                    df_grouped = df.groupby('os', as_index=False).agg({
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
                        height=600,
                        width=600,
                        margin=dict(t=50, l=0, r=0, b=0),
                        legend_title='Operating System'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:

                    st.error(f"Error creating pie chart: {str(e)}")

            with col2:
                try:
                    df = ba(con=con)
                    if df.empty:
                        st.warning("No data found for business analysis.")
                    else:
                        fig4 = px.bar(df,
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
                        f"Error fetching business analysis data: {str(e)}")

            try:
                df = ca(con=con)
                if df.empty:
                    st.warning("No data found for churn analysis.")
                else:
                    fig6 = px.line(df,
                                   x='cohort_month',
                                   y='churn_rate',
                                   color='cohort_size',
                                   text='avg_days_to_churn',
                                   title='Churn Analysis by Cohort',
                                   height=600,
                                   line_group="cohort_size"
                                   )
                    fig6.update_layout(
                        title='Churn Analysis by Cohort',
                        xaxis_title='Cohort Month',
                        yaxis_title='Churn Rate',
                        legend_title='Cohort Size'
                    )
                    st.plotly_chart(fig6, use_container_width=True)
            except Exception as e:
                st.error(f"Error fetching churn analysis data: {str(e)}")


def main():
    app()


if __name__ == "__main__":
    main()

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import logging
import duckdb as ddb
from main_data_pipeline import run_dbt_ops as rdops, generate_reports as gr
import analytics_queries as aq
from minio import Minio as s3
from io import BytesIO as io
import os



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
logger.info(f"Database path: {db_path}")

# Verify file exists
if not db_path.exists():
    raise FileNotFoundError(f"Database file not found at: {db_path}")

# MinIO configuration
S3_CONFIG = {
    'endpoint': os.getenv('MINIO_ENDPOINT'),
    'access_key': os.getenv('MINIO_ACCESS_KEY'),
    'secret_key': os.getenv('MINIO_SECRET_KEY'),
    'bucket': os.getenv('MINIO_BUCKET_NAME', 'sim-api-data'),  # Default bucket name
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
    page_title='Product Analytics Dashboard — Demo',
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='auto'
)

st.subheader('An app showcasing customer analytics across modalities.')

# Run the pipeline and load data
if st.button("Refresh Data"):
    with st.spinner("Running data pipeline..."):
        try:
            # Load Parquet file from MinIO
            df = load_from_s3(S3_CONFIG['bucket'], 'cleaned_data.parquet')
            
            # Run dbt operations on the Parquet data
            run_dbt(df)
            
            st.success("Data pipeline completed successfully!")
        except Exception as e:
            st.error(f"Error running data pipeline: {str(e)}")

# Main app
with ddb_connect() as con:
    tab1, tab2 = st.tabs(['User Centric', 'Revenue Centric'])

    with tab1:
        col1, col2 = st.columns([2,1])
        with col1:
            try:
                df = aq.run_lifecycle_analysis(con=con)

                if df.empty:
                    st.warning("No data found for lifecycle analysis.")
                else:
                    fig1 = px.bar(
                        df, 
                        x='product_name', 
                        y='total_revenue', 
                        hover_data=['avg_customer_value', 'avg_purchase_value', 'avg_purchase_frequency_rate'], 
                        color='total_revenue',  
                        color_continuous_scale='Sunset')
                    fig1.update_layout(
                        barmode="group",
                        xaxis_title="Product Name",
                        yaxis_title="Total Revenue",
                        legend_title="Total Revenue",
                        xaxis={'categoryorder': 'total ascending'}
                    )
                    st.plotly_chart(fig1)
            except Exception as e:

                st.error(f"Error fetching lifecycle analysis data: {str(e)}")

        with col2:
            try:
                df = aq.run_purchase_analysis(con=con)
                if df.empty:
                    st.warning("No data found for purchase analysis.")
                else:
                    fig2 = px.line(df, 
                                   x='total_purchases', 
                                   y='product_name', 
                                   hover_data=['product_name', 'avg_price'], 
                                   color = 'price_tier',
                                   title='Purchases by Product',
                                   markers=True)
                    fig2.update_layout(
                        title='Purchase Analysis by Product and Price Tier',
                        xaxis_title='Product Name',
                        yaxis_title='Total Purchases',
                        legend_title='Price Tier',
                        xaxis={'categoryorder': 'total ascending'}
                    )
                    st.plotly_chart(fig2, use_container_width=True)
            except Exception as e:

                st.error(f"Error fetching purchase analysis data: {str(e)}")

                
        try:
            df = aq.run_engagement_analysis(con=con)
            if df.empty:
                st.warning("No data found for engagement analysis.")
            else:
                bar_chart = px.bar(df, 
                                  x='hour', 
                                  y='total_sessions', 
                                  size='revenue', 
                                  color='revenue', 
                                  hover_data=['hour', 'avg_session_duration'],
                                  color_continuous_scale='Turbo',
                                  )
                line_chart = px.line(df, 
                fig5.update_layout(
                    title='Engagement Analysis by Hour',
                    xaxis_title='Hour',
                    yaxis_title='Total Sessions',
                    legend_title='Hour'
                )
                st.plotly_chart(fig5, use_container_width=True)
        except Exception as e:
            st.error(f"Error fetching engagement analysis data: {str(e)}")

    with tab2:
        col1, col2 = st.columns([2,1])
        with col1:
            try:
                df = aq.run_demographics_analysis(con=con)

                if df.empty:
                    st.warning("No data found for demographics analysis.")
                else:

                    fig3 = px.line(df, x='total_sessions', y='unique_users', color='device_type', text=['os', 'browser', 'avg_purchase_value', 'avg_session_duration'], markers=True)
                    st.plotly_chart(fig3, use_container_width=True)
            except Exception as e:
                st.error(f"Error fetching demographics analysis data: {str(e)}")

        with col2:
            try:
                df = aq.run_business_analysis(con=con)
                if df.empty:
                    st.warning("No data found for business analysis.")
                else:
                    fig4 = px.bar(df, x='job_title', y='unique_users', text=['total_sessions', 'average_session_duration'], color='conversion_rate')
                    st.plotly_chart(fig4, use_container_width=True)
            except Exception as e:
                st.error(f"Error fetching business analysis data: {str(e)}")
        
        try:
            df = aq.run_churn_analysis(con=con)
            if df.empty:
                st.warning("No data found for churn analysis.")
            else:
                fig6 = px.bar(df, x='cohort_month', y='churn_rate', color='cohort_size', text='avg_days_to_churn', title='Churn Analysis by Cohort', barmode='group', height=600)
                st.plotly_chart(fig6, use_container_width=True)
        except Exception as e:
            st.error(f"Error fetching churn analysis data: {str(e)}") 
    

    
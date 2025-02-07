import os
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import logging
import analytics_queries as aq
from minio import Minio as minio
from io import BytesIO as io

# Setup project paths 
PROJECT_ROOT = Path(__file__).parents[5]


S3_CONFIG = {
    'endpoint': os.getenv('MINIO_ENDPOINT'),
    'access_key': os.getenv('MINIO_ACCESS_KEY'),
    'secret_key': os.getenv('MINIO_SECRET_KEY'),
    'bucket': os.getenv('MINIO_BUCKET_NAME'),
    'use_ssl': os.getenv('MINIO_USE_SSL', 'False' ).lower() in {'true', '1', 'yes'},
}

client = minio.Minio(
            endpoint=S3_CONFIG['endpoint'],
            access_key=S3_CONFIG['access_key'],
            secret_key=S3_CONFIG['secret_key'],
            bucket=S3_CONFIG['bucket'],
            secure=S3_CONFIG['use_ssl']
        )

bucket_name = 'sim-api-data'
file_name = 'cleaned_data.parquet'
local_path = PROJECT_ROOT / 'data' / file_name

@st.cache_data
def load_parquet(file_name):

    # Check if Parquet file exists locally
    if os.path.exists(local_path):
        st.write(f"Loading cached {file_name} locally...")
        return pd.read_parquet(local_path)

    # If not found, fetch from MinIO
    try:
        st.write(f"Fetching {file_name} from MinIO...")
        response = client.get_object(client.bucket, file_name)

        # Read Parquet data into a Pandas DataFrame
        df = pd.read_parquet(io.BytesIO(response.read()))

        # Save locally for caching
        os.makedirs("data", exist_ok=True)
        df.to_parquet(local_path, index=False)

        return df

    except Exception as e:
        st.error(f"Error fetching {file_name}: {e}")
        return None


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


st.set_page_config(
    page_title= 'Product Analytics Dashboard — Demo',
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='auto')


st.subheader('An app showcasing customer analytics across modalities.')


col1, col2, col3, col4 = st.columns([2,1,1,2])

tab1, tab2 = st.tabs(['User Centric', 'Revenue Centric'])

with tab1:
    with col1: 
        df = aq.run_lifecycle_analysis()
        fig1 = px.line(
            df,
            x = 'total_sessions',
            y = 'completed_purchases',
            hover = 'avg_session_duration',
            color = 'product_name'
            )

    with col2: 
        df = aq.run_purchase_analysis()
        fig2 = px.scatter(
            df,
            x = 'total_purchases',
            y = 'avg_price',
            color = 'price_tier',
            size = 'total_purchases'
        )

with tab2:
    with col3: 
        df = aq.run_demographics_analysis(con=con)
        fig3 = px.line(
            df,
            x = 'total_sessions',
            y = 'unique_users',
            color = 'device_type', 
            text = ['os,', 'browser', 'avg_purchase_value', 'avg_session_duration'],
            markers = True
        )

    with col4: 
        df = aq.run_business_analysis(con=con)
        fig4 = px.bar(
            df,
            x = 'job_title',
            y = 'unique_users',
            text = ['total_sessions', 'average_session_duration'],
            color = 'conversion_rate'
        )  

engagement = aq.run_engagement_analysis(con=con)
fig5 = px.scatter(
    engagement,
    x = 'hour',
    y = 'total_sessions',
    size = 'revenue',
    color = 'hour',
    text = ['unique_users', 'avg_session_duration'],
    markers = True
    )

churn = aq.run_churn_analysis(con=con)
fig6 = px.bar(
    churn,
    x='cohort_month',
    y='churn_rate', 
    color='cohort_size',
    text='avg_days_to_churn', 
    title='Churn Analysis by Cohort', 
    barmode='group', 
    height=600)

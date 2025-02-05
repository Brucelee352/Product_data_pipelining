import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Product Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { 
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
    .stMetric h3 { 
        font-size: 1.1rem;
        color: #666;
        margin-bottom: 0.5rem;
    }
    .stMetric p { 
        font-size: 1.8rem;
        font-weight: bold;
        margin: 0;
    }
    .stPlotlyChart { border-radius: 8px; }
    </style>
""", unsafe_allow_html=True)


def load_latest_data():
    """Load the most recent data files from the reports directory"""
    reports_dir = Path('reports')

    if not reports_dir.exists():
        st.error("Directory not found. Please run the data pipeline first.")
        return {}

    # Get latest files for each analysis type
    latest_files = {
        'lifecycle': next(reports_dir.glob('*lifecycle*.csv'), None),
        'product': next(reports_dir.glob('*purchase*.csv'), None),
        'engagement': next(reports_dir.glob('*engagement*.csv'), None),
        'platform': next(reports_dir.glob('*demographics*.csv'), None),
        'time': next(reports_dir.glob('*churn*.csv'), None)
    }

    # Load dataframes with error handling
    dfs = {}
    for analysis_type, file in latest_files.items():
        if file:
            try:
                df = pd.read_csv(file)
                dfs[analysis_type] = df
            except Exception as e:
                st.sidebar.error(f"Error loading {analysis_type}: {str(e)}")

    return dfs


def display_kpis(data):
    """Display key performance indicators"""
    st.subheader("Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    if 'lifecycle' in data:
        lifecycle = data['lifecycle']
        col1.metric("Total Users", f"{
                    int(lifecycle['total_accounts'].iloc[0]):,}")
        col2.metric("Active Users", f"{
                    int(lifecycle['active_accounts'].iloc[0]):,}")
        col3.metric("Churn Rate", f"{lifecycle['churn_rate'].iloc[0]:.1f}%")
        col4.metric("Avg Lifetime", f"{
                    lifecycle['avg_account_lifetime_days'].iloc[0]:.1f} days")


def display_product_performance(data):
    """Display product performance metrics"""
    if 'purchase' in data:
        st.subheader("Product Performance")
        product_data = data['purchase']

        # Product performance by price tier
        fig = px.bar(product_data,
                     x='price_tier',
                     y=['total_purchases', 'avg_price'],
                     title="Product Performance by Price Tier",
                     barmode='group',
                     labels={'value': 'Count/Value', 'variable': 'Metric'},
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, use_container_width=True)


def display_user_engagement(data):
    """Display user engagement metrics"""
    if 'engagement' in data:
        st.subheader("User Engagement")
        engagement_data = data['engagement']

        # Engagement analysis
        fig = px.line(engagement_data,
                      x='engagement_level',
                      y=['total_users', 'churned_users', 'avg_session_duration'],
                      title="User Engagement Analysis",
                      labels={'value': 'Count/Value', 'variable': 'Metric'},
                      color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, use_container_width=True)


def display_platform_usage(data):
    """Display platform usage metrics"""
    if 'demographics' in data:
        st.subheader("Platform Usage")
        platform_data = data['demographics']

        # Platform usage metrics
        fig = px.sunburst(platform_data,
                          path=['device_type', 'os', 'browser'],
                          values='total_sessions',
                          color='avg_purchase_value',
                          title="Platform Usage and Purchase Value",
                          color_continuous_scale='Blues')
        st.plotly_chart(fig, use_container_width=True)


def display_churn_analysis(data):
    """Display churn analysis metrics"""
    if 'churn' in data:
        st.subheader("Cohort Analysis")
        churn_data = data['churn']

        # Cohort analysis
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=churn_data['cohort_month'],
            y=churn_data['cohort_size'],
            name='Cohort Size',
            mode='lines+markers',
            line=dict(color='#1f77b4')
        ))
        fig.add_trace(go.Scatter(
            x=churn_data['cohort_month'],
            y=churn_data['churn_rate'],
            name='Churn Rate (%)',
            mode='lines+markers',
            yaxis='y2',
            line=dict(color='#ff7f0e')
        ))
        fig.update_layout(
            title='User Growth and Churn by Cohort',
            yaxis=dict(title='Cohort Size'),
            yaxis2=dict(title='Churn Rate (%)', overlaying='y', side='right'),
            template='plotly_white'
        )
        st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("📊 Product Analytics Dashboard")
    st.markdown("Explore key metrics and insights from our product usage data.")

    # Add debug toggle
    debug = st.sidebar.checkbox("Show Debug Info")

    try:
        data = load_latest_data()

        if not data:
            st.warning("No data found. Please check pipeline output.")
            return

        if debug:
            st.sidebar.write("Loaded data keys:", list(data.keys()))

        # Display sections
        display_kpis(data)
        display_product_performance(data)
        display_user_engagement(data)
        display_platform_usage(data)
        display_churn_analysis(data)

    except Exception as e:
        st.error(f"Error: {str(e)}")
        if debug:
            st.exception(e)


if __name__ == "__main__":
    main()

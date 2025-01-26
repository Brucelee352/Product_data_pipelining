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

# Custom CSS
st.markdown("""
    <style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: white;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)


def load_latest_data():
    """Load the most recent data files from the reports directory"""
    reports_dir = Path('reports')

    if not reports_dir.exists():
        st.error("Reports directory not found. Please run the data pipeline first.")
        return {}

    # Debug info
    st.sidebar.write("Available Files:")
    for file in reports_dir.glob('*.csv'):
        st.sidebar.write(f"- {file.name}")

    # Get latest files for each analysis type
    latest_files = {}
    for file in reports_dir.glob('*.csv'):
        # Simplified file matching
        if 'lifecycle' in file.name.lower():
            latest_files['lifecycle'] = file
        elif 'purchase' in file.name.lower():
            latest_files['product'] = file
        elif 'engagement' in file.name.lower():
            latest_files['engagement'] = file
        elif 'platform' in file.name.lower():
            latest_files['platform'] = file
        elif 'time' in file.name.lower():
            latest_files['time'] = file

    # Load dataframes with error handling
    dfs = {}
    for analysis_type, file in latest_files.items():
        try:
            df = pd.read_csv(file)
            st.sidebar.write(
                f"\n{analysis_type} columns:", df.columns.tolist())
            dfs[analysis_type] = df
        except Exception as e:
            st.sidebar.error(f"Error loading {analysis_type}: {str(e)}")

    return dfs


def main():
    # Header
    st.title("Product Analytics Dashboard")

    # Add debug toggle
    debug = st.sidebar.checkbox("Show Debug Info")

    try:
        data = load_latest_data()

        if not data:
            st.warning(
                "No data available. Please check if the pipeline generated the reports.")
            return

        if debug:
            st.sidebar.write("Loaded data keys:", list(data.keys()))

        # Top-level metrics
        col1, col2, col3, col4 = st.columns(4)

        lifecycle = data.get('lifecycle')
        if lifecycle is not None:
            try:
                col1.metric("Total Users",
                            f"{int(lifecycle['total_accounts'].iloc[0]):,}")
                col2.metric("Active Users",
                            f"{int(lifecycle['active_accounts'].iloc[0]):,}")
                col3.metric("Churn Rate",
                            f"{lifecycle['churn_rate'].iloc[0]:.1f}%")
                col4.metric("Avg Lifetime",
                            f"{lifecycle['avg_account_lifetime_days'].iloc[0]:.1f}d")
            except Exception as e:
                if debug:
                    st.sidebar.error(f"Metrics error: {str(e)}")

        # Product Performance
        product_data = data.get('product')
        if product_data is not None:
            try:
                st.subheader("Product Performance")
                fig = px.bar(product_data,
                             x='product_name',
                             y=['total_views', 'completed_purchases'],
                             title="Product Views vs Purchases",
                             barmode='group')
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                if debug:
                    st.sidebar.error(f"Product chart error: {str(e)}")

        # User Engagement
        col1, col2 = st.columns(2)

        engagement_data = data.get('engagement')
        if engagement_data is not None:
            try:
                # Show hourly session distribution
                fig1 = px.bar(engagement_data,
                              x='hour_of_day',
                              y=['total_sessions', 'unique_users'],
                              title="Hourly User Activity",
                              barmode='group')
                col1.plotly_chart(fig1)
            except Exception as e:
                if debug:
                    st.sidebar.error(f"Engagement chart error: {str(e)}")

        platform_data = data.get('platform')
        if platform_data is not None:
            try:
                # Show platform usage and churn
                fig2 = px.sunburst(platform_data,
                                   path=['device_type', 'os', 'browser'],
                                   values='total_users',
                                   color='churn_rate',
                                   title="Platform Usage and Churn Rate")
                col2.plotly_chart(fig2)
            except Exception as e:
                if debug:
                    st.sidebar.error(f"Platform chart error: {str(e)}")

        # Time Analysis
        time_data = data.get('time')
        if time_data is not None:
            try:
                st.subheader("Cohort Analysis")
                fig = go.Figure()

                # Add traces for users and churn
                fig.add_trace(go.Scatter(
                    x=time_data['cohort_month'],
                    y=time_data['total_users'],
                    name='Total Users',
                    mode='lines+markers'
                ))
                fig.add_trace(go.Scatter(
                    x=time_data['cohort_month'],
                    y=time_data['churn_rate'],
                    name='Churn Rate (%)',
                    mode='lines+markers',
                    yaxis='y2'
                ))

                # Update layout for dual axis
                fig.update_layout(
                    title='User Growth and Churn by Cohort',
                    yaxis=dict(title='Total Users'),
                    yaxis2=dict(title='Churn Rate (%)',
                                overlaying='y',
                                side='right')
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                if debug:
                    st.sidebar.error(f"Time chart error: {str(e)}")

    except Exception as e:
        st.error(f"Error: {str(e)}")
        if debug:
            st.exception(e)


if __name__ == "__main__":
    main()

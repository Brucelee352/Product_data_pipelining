"""
Analytics queries for the product data pipeline.
All queries run against dbt-created tables in the main schema.
"""

import logging
from pathlib import Path
import pandas as pd
from duckdb import DuckDBPyConnection

log = logging.getLogger(__name__)
PRODUCT_SCHEMA = 'main.product_schema'

def run_lifecycle_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user lifecycle metrics."""
    try:
        query = f"""
<<<<<<< Updated upstream
            SELECT 
                product_name,
                COUNT(*) AS total_sessions,
                AVG(session_duration_minutes) AS avg_session_duration,
                SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) AS completed_purchases
            FROM {PRODUCT_SCHEMA}
            GROUP BY product_name
=======
            WITH lifecycle_data AS (
                SELECT
                    product_name,
                    purchase_status,
                    SUM(TRY_CAST(price AS INTEGER)) AS total_revenue,
                    COUNT(DISTINCT user_id) as total_customers,
                    COUNT(*) as total_purchases
                FROM {PRODUCT_SCHEMA}
                WHERE purchase_status = 'completed'
                GROUP BY product_name, purchase_status
            )
            SELECT 
                   product_name,
                   total_purchases,
                   total_revenue,
                   total_customers,
                   ROUND(total_revenue / total_purchases, 2) as avg_purchase_value,
                   ROUND(total_customers / total_purchases, 2) as avg_purchase_frequency_rate,
                   ROUND(avg_purchase_value * avg_purchase_frequency_rate, 2) as avg_customer_value
            FROM lifecycle_data
            GROUP BY product_name, total_revenue, total_customers, total_purchases;
>>>>>>> Stashed changes
        """
        result = conn.execute(query).fetchdf()
        return result
<<<<<<< Updated upstream
=======


>>>>>>> Stashed changes
    except Exception as e:
        log.error(f"Error in lifecycle analysis: {str(e)}")
        raise

def run_purchase_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze purchase patterns."""
    try:
        query = f"""
<<<<<<< Updated upstream
            SELECT 
=======
            SELECT
                product_name,
>>>>>>> Stashed changes
                price_tier,
                COUNT(*) AS total_purchases,
                ROUND(AVG(price), 2) AS avg_price
            FROM {PRODUCT_SCHEMA}
            WHERE purchase_status = 'completed'
            GROUP BY price_tier, product_name
        """
        result = conn.execute(query).fetchdf()
        return result
    except Exception as e:
        log.error(f"Error in purchase analysis: {str(e)}")
        raise

def run_demographics_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user demographics and platform usage."""
    try:
        query = f"""
            SELECT
                job_title,
                price_tier,
                product_name,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(AVG(CASE 
                    WHEN purchase_status = 'completed' THEN price 
                    ELSE 0 
                END), 2) as avg_purchase_value
            FROM {PRODUCT_SCHEMA}
            GROUP BY job_title, price_tier, product_name
            HAVING COUNT(*) > 10
            ORDER BY unique_users DESC;
        """
<<<<<<< Updated upstream
        return conn.execute(query).fetchdf()
=======
        result = con.execute(query).fetchdf()
        return result
>>>>>>> Stashed changes
    except Exception as e:
        log.error(f"Error in demographics analysis: {str(e)}")
        raise

def run_business_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze business metrics by job title."""
    try:
        return conn.sql(f"""
            SELECT
                job_title,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(
                    COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / 
                    COUNT(*),
                2) as conversion_rate
            FROM {PRODUCT_SCHEMA}
            GROUP BY job_title
            HAVING COUNT(DISTINCT user_id) > 5
            ORDER BY unique_users DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in business analysis: %s", str(e))
        raise

def run_engagement_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user engagement patterns."""
    try:
        return conn.sql(f"""
            SELECT
<<<<<<< Updated upstream
                DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP)) as hour,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(SUM(CASE 
                    WHEN purchase_status = 'completed' THEN price 
                    ELSE 0 
                END), 2) as revenue
            FROM {PRODUCT_SCHEMA}
            GROUP BY DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP))
=======
                DATE_TRUNC('hour', login_time) AS hour,
                COUNT(*) AS total_sessions,
                COUNT(DISTINCT user_id) AS unique_users,
                ROUND(
                    AVG(
                        COALESCE(session_duration_minutes, 0)), 
                        2) AS avg_session_duration,
                ROUND(SUM(COALESCE(price, 0)), 2) AS revenue
            FROM {PRODUCT_SCHEMA}
            WHERE purchase_status = 'completed'
            GROUP BY DATE_TRUNC('hour', login_time)
>>>>>>> Stashed changes
            ORDER BY total_sessions DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in engagement analysis: %s", str(e))
        raise

def run_churn_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user churn patterns."""
    try:
        return conn.sql(f"""
            SELECT
                DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP)) as cohort_month,
                COUNT(DISTINCT user_id) as cohort_size,
                ROUND(
<<<<<<< Updated upstream
                    COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0),
=======
                    COUNT(
                        CASE
                            WHEN account_deleted IS NOT NULL
                            THEN 1
                        END) * 100.0 /
                    NULLIF(
                        COUNT(*), 0),
>>>>>>> Stashed changes
                2) as churn_rate,
                ROUND(
                    AVG(CASE 
                        WHEN account_deleted IS NOT NULL THEN 
                            DATEDIFF('day', account_created, account_deleted)
                        ELSE NULL 
                    END),
                2) as avg_days_to_churn
            FROM {PRODUCT_SCHEMA}
            GROUP BY DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP))
            ORDER BY cohort_month;
        """).fetchdf()
    except Exception as e:
        log.error("Error in churn analysis: %s", str(e))
        raise

def run_session_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze session patterns."""
    try:
        return conn.sql(f"""
            SELECT
                user_id,
                first_name,
                last_name,
                COUNT(*) as total_sessions,
                ROUND(AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(DATEDIFF('hour', 
                    MIN(TRY_CAST(login_time AS TIMESTAMP)),
                    MAX(TRY_CAST(login_time AS TIMESTAMP))
                ) / 3600.0, 1) AS session_duration_hours
            FROM {PRODUCT_SCHEMA}
            WHERE TRY_CAST(login_time AS TIMESTAMP) IS NOT NULL
            GROUP BY user_id, first_name, last_name
            ORDER BY total_sessions DESC
            LIMIT 100;
        """).fetchdf()
    except Exception as e:
        log.error("Error in session analysis: %s", str(e))
        raise

def run_funnel_analysis(conn: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze conversion funnel metrics."""
    try:
        return conn.sql(f"""
            SELECT
                product_name,
                COUNT(*) as total_views,
                COUNT(DISTINCT user_id) as unique_viewers,
                ROUND(
<<<<<<< Updated upstream
                    COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0),
=======
                    COUNT(
                        CASE WHEN purchase_status = 'completed'
                        THEN 1 END) * 100.0 /
                        NULLIF(COUNT(*), 0),
>>>>>>> Stashed changes
                2) as conversion_rate
            FROM {PRODUCT_SCHEMA}
            GROUP BY product_name
            ORDER BY conversion_rate DESC;
        """).fetchdf()
    except Exception as e:
        log.error("Error in funnel analysis: %s", str(e))
        raise
    
def run_analysis(conn, query):
    try:
        return conn.sql(query).fetchdf()
    except Exception as e:
        if "does not exist" in str(e):
            log.error("Table not found. Please ensure the dbt models have been run.")
        else:
            log.error("Analysis error: %s", str(e))
        raise

def save_analysis_results(results: dict, reports_dir: Path) -> None:
    """Save analysis results to CSV files."""
    try:
        for name, df in results.items():
            output_path = reports_dir / f"{name}.csv"
            df.to_csv(output_path, index=False)
            log.info(f"Saved {name} to {output_path}")
    except Exception as e:
        log.error("Error saving analysis results: %s", str(e))
        raise

def main(conn: DuckDBPyConnection, reports_dir: Path) -> None:
    """Main function to execute all analytics queries in sequence."""
    try:
        # 1. Run lifecycle analysis
        lifecycle_df = run_lifecycle_analysis(conn)
        
        # 2. Run purchase analysis
        purchase_df = run_purchase_analysis(conn)
        
        # 3. Run demographics analysis
        demographics_df = run_demographics_analysis(conn)
        
        # 4. Run business analysis
        business_df = run_business_analysis(conn)
        
        # 5. Run engagement analysis
        engagement_df = run_engagement_analysis(conn)
        
        # 6. Run churn analysis
        churn_df = run_churn_analysis(conn)
        
        # 7. Run session analysis
        session_df = run_session_analysis(conn)
        
        # 8. Run funnel analysis
        funnel_df = run_funnel_analysis(conn)
        
        # Save all results
        results = {
            "lifecycle_analysis": lifecycle_df,
            "purchase_analysis": purchase_df,
            "demographics_analysis": demographics_df,
            "business_analysis": business_df,
            "engagement_analysis": engagement_df,
            "churn_analysis": churn_df,
            "session_analysis": session_df,
            "funnel_analysis": funnel_df
        }
        
        save_analysis_results(results, reports_dir)
        
    except Exception as e:
        log.error("Error in main analytics execution: %s", str(e))
        raise

if __name__ == "__main__":
    import duckdb
    from pathlib import Path
    
    # Initialize connection and reports directory
    conn = duckdb.connect('dbt_pipeline_demo.duckdb')
    reports_dir = Path('reports')
    reports_dir.mkdir(exist_ok=True)
    
    # Run main analytics
    main(conn, reports_dir)

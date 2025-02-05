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
REPORTS_DIR = Path('reports')


def run_lifecycle_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user lifecycle metrics."""
    try:
        query = f"""
            SELECT
                product_name,
                COUNT(*) AS total_sessions,
                AVG(session_duration_minutes) AS avg_session_duration,
                SUM(
                    CASE WHEN purchase_status = 'completed'
                    THEN 1
                    ELSE 0
                    END
                ) AS completed_purchases
            FROM {PRODUCT_SCHEMA}
            GROUP BY product_name
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in lifecycle analysis: %s", str(e))
        raise


def run_purchase_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze purchase patterns."""
    try:
        query = f"""
            SELECT
                price_tier,
                COUNT(*) AS total_purchases,
                AVG(price) AS avg_price
            FROM {PRODUCT_SCHEMA}
            WHERE purchase_status = 'completed'
            GROUP BY price_tier
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in purchase analysis: %s", str(e))
        raise


def run_demographics_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user demographics and platform usage."""
    try:
        query = f"""
            SELECT
                device_type,
                os,
                browser,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(
                    AVG(session_duration_minutes), 2
                    ) as avg_session_duration,
                ROUND(
                    AVG(
                        CASE
                            WHEN purchase_status = 'completed'
                            THEN price
                            ELSE 0
                        END), 2) as avg_purchase_value
            FROM {PRODUCT_SCHEMA}
            GROUP BY device_type, os, browser
            HAVING COUNT(*) > 10
            ORDER BY unique_users DESC;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in demographics analysis: %s", str(e))
        raise


def run_business_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze business metrics by job title."""
    try:
        query = f"""
            SELECT
                job_title,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(
                    AVG(
                        session_duration_minutes), 2
                        ) as avg_session_duration,
                ROUND(
                    COUNT(
                        CASE
                            WHEN purchase_status = 'completed'
                            THEN 1
                        END) * 100.0 /
                    COUNT(*),
                2) as conversion_rate
            FROM {PRODUCT_SCHEMA}
            GROUP BY job_title
            HAVING COUNT(DISTINCT user_id) > 5
            ORDER BY unique_users DESC;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in business analysis: %s", str(e))
        raise


def run_engagement_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user engagement patterns."""
    try:
        query = f"""
            SELECT
                DATE_TRUNC('hour',
                TRY_CAST(login_time AS TIMESTAMP)) as hour,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(
                    AVG(
                        session_duration_minutes), 2
                        ) as avg_session_duration,
                ROUND(
                    SUM(
                        CASE
                            WHEN purchase_status = 'completed'
                            THEN price
                            ELSE 0
                        END), 2) as revenue
            FROM {PRODUCT_SCHEMA}
            GROUP BY DATE_TRUNC('hour',
                     TRY_CAST(
                        login_time AS TIMESTAMP)
                        )
            ORDER BY total_sessions DESC;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in engagement analysis: %s", str(e))
        raise


def run_churn_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user churn patterns."""
    try:
        query = f"""
            SELECT
                DATE_TRUNC('month',
                           TRY_CAST(
                               account_created AS TIMESTAMP)
                               ) as cohort_month,
                COUNT(DISTINCT user_id) as cohort_size,
                ROUND(
                    COUNT(
                        CASE
                            WHEN account_deleted IS NOT NULL
                            THEN 1
                        END) * 100.0 /
                    NULLIF(
                        COUNT(*), 0),

                2) as churn_rate,
                ROUND(
                    AVG(CASE
                        WHEN account_deleted IS NOT NULL THEN
                            DATEDIFF('day',
                            account_created,
                            account_deleted)
                        ELSE NULL
                    END),
                2) as avg_days_to_churn
            FROM {PRODUCT_SCHEMA}
            GROUP BY
                DATE_TRUNC(
                        'month',
                        TRY_CAST(account_created
                        AS TIMESTAMP)
                        )
            ORDER BY cohort_month;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in churn analysis: %s", str(e))
        raise


def run_session_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze session patterns."""
    try:
        query = f"""
            SELECT
                user_id,
                first_name,
                last_name,
                COUNT(*) as total_sessions,
                ROUND(
                    AVG(session_duration_minutes),
                        2) as avg_session_duration,
                ROUND(DATEDIFF('hour',
                    MIN(TRY_CAST(login_time AS TIMESTAMP)),
                    MAX(TRY_CAST(login_time AS TIMESTAMP))
                ) / 3600.0, 1) AS session_duration_hours
            FROM {PRODUCT_SCHEMA}
            WHERE TRY_CAST(login_time AS TIMESTAMP) IS NOT NULL
            GROUP BY user_id, first_name, last_name
            ORDER BY total_sessions DESC
            LIMIT 100;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in session analysis: %s", str(e))
        raise


def run_funnel_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze conversion funnel metrics."""
    try:
        query = f"""
            SELECT
                product_name,
                COUNT(*) as total_views,
                COUNT(DISTINCT user_id) as unique_viewers,
                ROUND(

                    COUNT(
                        CASE WHEN purchase_status = 'completed'
                        THEN 1 END) * 100.0 /
                        NULLIF(COUNT(*), 0),
                2) as conversion_rate
            FROM {PRODUCT_SCHEMA}
            GROUP BY product_name
            ORDER BY conversion_rate DESC;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        log.error("Error in funnel analysis: %s", str(e))
        raise


def run_analysis(con, query):
    """Run an analysis query and return the result."""
    try:
        return con.sql(query).fetchdf()
    except Exception as e:
        if "does not exist" in str(e):
            log.error(
                "Table not found. Please ensure the dbt models have been run.")
        else:
            log.error("Analysis error: %s", str(e))
        raise


def save_analysis_results(results: dict, reports_dir: Path) -> None:
    """Save analysis results to CSV files."""
    try:
        for name, df in results.items():
            output_path = reports_dir / f"{name}.csv"
            df.to_csv(output_path, index=False)
            log.info("Saved %s to %s", name, output_path)
    except Exception as e:
        log.error("Error saving analysis results: %s", str(e))
        raise


def main(con: DuckDBPyConnection, reports_dir: Path) -> None:
    """Main function to execute all analytics queries in sequence."""
    try:
        lifecycle_df = run_lifecycle_analysis(con)
        purchase_df = run_purchase_analysis(con)
        demographics_df = run_demographics_analysis(con)
        business_df = run_business_analysis(con)
        engagement_df = run_engagement_analysis(con)
        churn_df = run_churn_analysis(con)
        session_df = run_session_analysis(con)
        funnel_df = run_funnel_analysis(con)

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

    # Initialize conection and reports directory
    db_connection = duckdb.connect('dbt_pipeline_demo.duckdb')
    REPORTS_DIR.mkdir(exist_ok=True)

    # Run main analytics
    main(db_connection, REPORTS_DIR)

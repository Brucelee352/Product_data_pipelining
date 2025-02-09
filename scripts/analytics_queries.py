"""
#----------------------------------------------------------------#

Analytical queries that run against dbt-created tables
in the main schema, then deployed as charts via streamlit.

Please refer to README.md file for more information.

All dependencies are installed via pyproject.toml.

#----------------------------------------------------------------#
"""


import logging
from pathlib import Path
import pandas as pd
from duckdb import DuckDBPyConnection
from constants import PRODUCT_SCHEMA, REPORTS_DIR, LOG


def run_lifecycle_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user lifecycle metrics by price tier."""
    try:
        query = f"""
                SELECT
                    os,
                    price_tier,
                    ROUND(SUM(price), 2) AS total_revenue,
                    COUNT(DISTINCT user_id) AS total_customers,
                    COUNT(*) AS total_purchases
                FROM {PRODUCT_SCHEMA}
                WHERE purchase_status = 'completed'
                GROUP BY price_tier, os
                """
        result = con.execute(query).fetchdf()
        return result
    except Exception as e:
        LOG.error("Error in lifecycle analysis: %s", str(e))
        raise


def run_purchase_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze purchase patterns."""
    try:
        query = f"""
            SELECT
                product_name,
                price_tier,
                COUNT(*) AS total_purchases,
                ROUND(AVG(TRY_CAST(price AS DECIMAL(10,2))), 2) AS avg_price,
                ROUND(
                    SUM(
                        TRY_CAST(price AS DECIMAL(10,2))), 2) AS total_revenue,
                COUNT(DISTINCT user_id) AS unique_customers,
                EXTRACT(MONTH FROM login_time) AS month
            FROM {PRODUCT_SCHEMA}
            WHERE purchase_status = 'completed'
            GROUP BY product_name, price_tier, month
            HAVING COUNT(*) > 0
            ORDER BY total_purchases DESC
        """
        result = con.execute(query).fetchdf()
        return result
    except Exception as e:
        LOG.error("Error in purchase analysis: %s", str(e))
        raise


def run_demographics_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user demographics and platform usage."""
    try:
        query = f"""
            SELECT
                job_title,
                price_tier,
                product_name,
                device_type,
                browser,
                os,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(
                    AVG(session_duration_minutes), 2) as avg_session_duration,
                ROUND(
                    AVG(CASE
                        WHEN purchase_status = 'completed' THEN price
                        ELSE 0
                    END), 2) as avg_purchase_value

            FROM {PRODUCT_SCHEMA}
            GROUP BY job_title, 
            price_tier, product_name, 
            device_type, browser, os
            HAVING COUNT(DISTINCT user_id) > 1
            ORDER BY unique_users DESC;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        LOG.error("Error in demographics analysis: %s", str(e))
        raise


def run_business_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze business metrics by device used."""
    try:
        query = f"""
            SELECT
                device_type,
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
            GROUP BY device_type
            HAVING COUNT(DISTINCT user_id) > 5
            ORDER BY unique_users DESC;
        """
        result = con.execute(query).fetchdf()
        return result

    except Exception as e:
        LOG.error("Error in business analysis: %s", str(e))
        raise


def run_engagement_analysis(con: DuckDBPyConnection) -> pd.DataFrame:
    """Analyze user engagement patterns."""
    try:
        query = f"""
            SELECT
                EXTRACT(HOUR FROM login_time) AS hour,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(
                    AVG(
                       COALESCE(session_duration_minutes, 0)),
                        2) AS avg_session_duration,
                ROUND(SUM(COALESCE(price, 0)), 2) AS revenue
            FROM {PRODUCT_SCHEMA}
            WHERE purchase_status = 'completed'
            GROUP BY EXTRACT(HOUR FROM login_time)
            ORDER BY hour ASC;
        """
        result = con.execute(query).fetchdf()
        return result
    except Exception as e:
        LOG.error("Error in engagement analysis: %s", str(e))
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
                        COUNT(*), 0),2) as churn_rate,
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
        LOG.error("Error in churn analysis: %s", str(e))
        raise


def run_analysis(con, query):
    """Run an analysis query and return the result."""
    try:
        return con.sql(query).fetchdf()
    except Exception as e:
        if "does not exist" in str(e):
            LOG.error(
                "Table not found. Please ensure the dbt models have been run.")
        else:
            LOG.error("Analysis error: %s", str(e))
        raise


def save_analysis_results(results: dict, reports_dir: Path) -> None:
    """Save analysis results to CSV files."""
    try:
        for name, df in results.items():
            output_path = reports_dir / f"{name}.csv"
            df.to_csv(output_path, index=False)
            LOG.info("Saved %s to %s", name, output_path)
    except Exception as e:
        LOG.error("Error saving analysis results: %s", str(e))
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

        # Saves all results
        results = {
            "lifecycle_analysis": lifecycle_df,
            "purchase_analysis": purchase_df,
            "demographics_analysis": demographics_df,
            "business_analysis": business_df,
            "engagement_analysis": engagement_df,
            "churn_analysis": churn_df,
        }

        save_analysis_results(results, reports_dir)

    except Exception as e:
        LOG.error("Error in main analytics execution: %s", str(e))
        raise


if __name__ == "__main__":
    import duckdb

    # Initializes conection and reports directory
    db_connection = duckdb.connect('dbt_pipeline_demo.duckdb')
    REPORTS_DIR.mkdir(exist_ok=True)

    # Runs main analytics
    main(db_connection, REPORTS_DIR)

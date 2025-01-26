"""Analytics queries module for product data analysis"""

import logging
from datetime import datetime as dt

log = logging.getLogger(__name__)


def run_lifecycle_analysis(conn):
    """Run customer lifecycle analysis queries."""
    try:
        lifecycle_analysis = conn.execute("""
            SELECT
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN is_active = 'yes' THEN 1 END) as active_accounts,
                ROUND(TRY_CAST(COUNT(CASE WHEN is_active = 'yes' THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as active_percentage,
                COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) as churned_accounts,
                ROUND(TRY_CAST(COUNT(CASE WHEN account_deleted IS NOT NULL THEN 1 END) * 100.0 / 
                    NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(AVG(DATEDIFF('days',
                    TRY_CAST(account_created AS TIMESTAMP),
                    COALESCE(TRY_CAST(account_deleted AS TIMESTAMP), CURRENT_TIMESTAMP)
                )), 2) as avg_account_lifetime_days
            FROM product_schema;
        """).fetchdf()
        log.info("\nCustomer Lifecycle Analysis:")
        log.info(lifecycle_analysis)
        return lifecycle_analysis
    except Exception as e:
        log.error("Error in lifecycle analysis: %s", str(e))
        raise


def run_purchase_analysis(conn):
    """Run purchase analysis queries."""
    try:
        purchase_analysis = conn.execute("""
            SELECT
                product_name,
                COUNT(*) as total_views,
                SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) as completed_purchases,
                ROUND(TRY_CAST(AVG(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) 
                    AS DECIMAL(10,2)), 2) as avg_purchase_value,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) 
                    AS DECIMAL(10,2)), 2) as total_revenue,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN 1 ELSE 0 END) * 100.0 
                    / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as conversion_rate
            FROM product_schema
            GROUP BY product_name
            ORDER BY total_revenue DESC;
        """).fetchdf()
        log.info("\nProduct Performance Analysis:")
        log.info(purchase_analysis)
        return purchase_analysis
    except Exception as e:
        log.error("Error in purchase analysis: %s", str(e))
        raise


def run_demographics_analysis(conn):
    """Run user demographics and behavior analysis."""
    try:
        demographics = conn.execute("""
            SELECT
                ps.device_type,
                ps.os,
                ps.browser,
                ROUND(TRY_CAST(AVG(ps.session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration,
                COUNT(DISTINCT ps.user_id) as unique_users,
                COUNT(*) as total_sessions,
                ROUND(TRY_CAST(COUNT(CASE WHEN ps.purchase_status = 'completed' THEN 1 END) * 100.0 
                    / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as conversion_rate,
                ROUND(TRY_CAST(AVG(CASE WHEN ps.purchase_status = 'completed' THEN ps.price ELSE 0 END) 
                    AS DECIMAL(10,2)), 2) as avg_purchase_value
            FROM product_schema ps
            GROUP BY ps.device_type, ps.os, ps.browser
            HAVING COUNT(*) > 10
            ORDER BY unique_users DESC;
        """).fetchdf()
        log.info("\nUser Demographics Analysis:")
        log.info(demographics)
        return demographics
    except Exception as e:
        log.error("Error in demographics analysis: %s", str(e))
        raise


def run_business_analysis(conn):
    """Run business impact analysis."""
    try:
        business = conn.execute("""
            SELECT
                du.job_title,
                COUNT(DISTINCT du.user_id) as unique_users,
                ROUND(TRY_CAST(AVG(ps.price) AS DECIMAL(10,2)), 2) as avg_cart_value,
                COUNT(CASE WHEN ps.purchase_status = 'completed' THEN 1 END) as completed_purchases,
                ROUND(TRY_CAST(SUM(CASE WHEN ps.purchase_status = 'completed' THEN ps.price ELSE 0 END) 
                    AS DECIMAL(10,2)), 2) as total_revenue,
                ROUND(TRY_CAST(COUNT(CASE WHEN ps.purchase_status = 'completed' THEN 1 END) * 100.0 
                    / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)), 2) as conversion_rate
            FROM dim_user as du
            JOIN product_schema AS ps on du.user_id = ps.user_id
            GROUP BY du.job_title
            HAVING COUNT(DISTINCT du.user_id) > 5
            ORDER BY total_revenue DESC
            LIMIT 10;
        """).fetchdf()
        log.info("\nBusiness Impact Analysis:")
        log.info(business)
        return business
    except Exception as e:
        log.error("Error in business analysis: %s", str(e))
        raise


def run_engagement_analysis(conn):
    """Run user engagement analysis."""
    try:
        engagement = conn.execute("""
            SELECT
                DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP)) as hour_of_day,
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(TRY_CAST(AVG(session_duration_minutes) AS DECIMAL(10,2)), 2) as avg_session_duration,
                COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as completed_purchases,
                ROUND(TRY_CAST(SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) 
                    AS DECIMAL(10,2)), 2) as revenue
            FROM product_schema
            GROUP BY DATE_TRUNC('hour', TRY_CAST(login_time AS TIMESTAMP))
            ORDER BY total_sessions DESC
            LIMIT 24;
        """).fetchdf()
        log.info("\nUser Engagement Analysis:")
        log.info(engagement)
        return engagement
    except Exception as e:
        log.error("Error in engagement analysis: %s", str(e))
        raise


def run_churn_analysis(conn):
    """Run comprehensive churn analysis."""
    try:
        # Time-based churn
        time_churn = conn.execute("""
            SELECT
                DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP)) as cohort_month,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100 
                    AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(CASE
                    WHEN is_active = 'no' THEN
                        DATEDIFF('days', TRY_CAST(account_created AS TIMESTAMP), 
                        TRY_CAST(account_deleted AS TIMESTAMP))
                    END) AS DECIMAL(10,2)), 2) as avg_days_to_churn
            FROM product_schema
            GROUP BY DATE_TRUNC('month', TRY_CAST(account_created AS TIMESTAMP))
            ORDER BY cohort_month;
        """).fetchdf()

        # Price-based churn
        price_churn = conn.execute("""
            SELECT
                CASE
                    WHEN price < 500 THEN 'Low (<$500)'
                    WHEN price BETWEEN 500 AND 1000 THEN 'Medium ($500-$1000)'
                    WHEN price BETWEEN 1001 AND 2500 THEN 'High ($1001-$2500)'
                    ELSE 'Premium (>$2500)'
                END as price_tier,
                COUNT(*) as total_users,
                SUM(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) as churned_users,
                ROUND(TRY_CAST(AVG(CASE WHEN is_active = 'no' THEN 1 ELSE 0 END) * 100
                    AS DECIMAL(10,2)), 2) as churn_rate,
                ROUND(TRY_CAST(AVG(price) AS DECIMAL(10,2)), 2) as avg_price
            FROM product_schema
            GROUP BY
                CASE
                    WHEN price < 500 THEN 'Low (<$500)'
                    WHEN price BETWEEN 500 AND 1000 THEN 'Medium ($500-$1000)'
                    WHEN price BETWEEN 1001 AND 2500 THEN 'High ($1001-$2500)'
                    ELSE 'Premium (>$2500)'
                END
            ORDER BY avg_price;
        """).fetchdf()

        log.info("\nChurn Analysis Results:")
        log.info("Time-based Churn:")
        log.info(time_churn)
        log.info("\nPrice-based Churn:")
        log.info(price_churn)

        return {
            'time_based_churn': time_churn,
            'price_based_churn': price_churn
        }
    except Exception as e:
        log.error("Error in churn analysis: %s", str(e))
        raise


def run_session_analysis(conn):
    """Run session duration analysis."""
    try:
        sessions = conn.execute("""
            SELECT
                concat(du.first_name, ' ', du.last_name) AS full_name,
                du.email,
                du.state,
                TRY_CAST(ps.login_time AS TIMESTAMP) as login_time,
                TRY_CAST(ps.logout_time AS TIMESTAMP) as logout_time,
                (TRY_CAST(ps.logout_time AS TIMESTAMP) - TRY_CAST(ps.login_time AS TIMESTAMP)) AS session_duration,
                ROUND(EXTRACT(EPOCH FROM (TRY_CAST(ps.logout_time AS TIMESTAMP) -
                    TRY_CAST(ps.login_time AS TIMESTAMP))) / 3600.0, 1) AS session_duration_hours
            FROM product_schema ps
            JOIN dim_user du on ps.user_id = du.user_id
            WHERE TRY_CAST(ps.login_time AS TIMESTAMP) <= TRY_CAST(ps.logout_time AS TIMESTAMP)
            ORDER BY du.state
            LIMIT 50;
        """).fetchdf()
        log.info("\nSession Duration Analysis:")
        log.info(sessions)
        return sessions
    except Exception as e:
        log.error("Error in session analysis: %s", str(e))
        raise


def run_funnel_analysis(conn):
    """Analyze the user conversion funnel from visit to purchase."""
    try:
        funnel_metrics = conn.execute("""
            WITH funnel_stages AS (
                SELECT
                    user_id,
                    COUNT(*) as total_visits,
                    COUNT(CASE WHEN session_duration_minutes > 5 THEN 1 END) as engaged_sessions,
                    COUNT(CASE WHEN purchase_status = 'pending' THEN 1 END) as cart_additions,
                    COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as purchases,
                    SUM(CASE WHEN purchase_status = 'completed' THEN price ELSE 0 END) as revenue
                FROM product_schema
                GROUP BY user_id
            )
            SELECT
                COUNT(DISTINCT user_id) as total_users,
                SUM(total_visits) as total_visits,
                SUM(engaged_sessions) as engaged_sessions,
                SUM(cart_additions) as cart_additions,
                SUM(purchases) as completed_purchases,
                ROUND(SUM(revenue), 2) as total_revenue,
                
                -- Conversion rates between stages
                ROUND(100.0 * SUM(engaged_sessions) / NULLIF(SUM(total_visits), 0), 2) as visit_to_engagement_rate,
                ROUND(100.0 * SUM(cart_additions) / NULLIF(SUM(engaged_sessions), 0), 2) as engagement_to_cart_rate,
                ROUND(100.0 * SUM(purchases) / NULLIF(SUM(cart_additions), 0), 2) as cart_to_purchase_rate,
                ROUND(100.0 * SUM(purchases) / NULLIF(SUM(total_visits), 0), 2) as overall_conversion_rate,
                
                -- Per-user metrics
                ROUND(AVG(total_visits), 2) as avg_visits_per_user,
                ROUND(AVG(engaged_sessions), 2) as avg_engaged_sessions_per_user,
                ROUND(AVG(cart_additions), 2) as avg_cart_adds_per_user,
                ROUND(AVG(purchases), 2) as avg_purchases_per_user,
                ROUND(AVG(CASE WHEN purchases > 0 THEN revenue/purchases ELSE 0 END), 2) as avg_order_value
            FROM funnel_stages;
        """).fetchdf()

        # Get stage-by-stage breakdown
        stage_breakdown = conn.execute("""
            SELECT
                product_name,
                COUNT(*) as total_views,
                COUNT(CASE WHEN session_duration_minutes > 5 THEN 1 END) as engaged_views,
                COUNT(CASE WHEN purchase_status = 'pending' THEN 1 END) as cart_adds,
                COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) as purchases,
                ROUND(100.0 * COUNT(CASE WHEN session_duration_minutes > 5 THEN 1 END) /
                    NULLIF(COUNT(*), 0), 2) as engagement_rate,
                ROUND(100.0 * COUNT(CASE WHEN purchase_status = 'completed' THEN 1 END) /
                    NULLIF(COUNT(*), 0), 2) as conversion_rate
            FROM product_schema
            GROUP BY product_name
            ORDER BY conversion_rate DESC;
        """).fetchdf()

        log.info("\nOverall Funnel Metrics:")
        log.info(funnel_metrics)
        log.info("\nProduct-wise Funnel Breakdown:")
        log.info(stage_breakdown)

        return {
            'overall_funnel': funnel_metrics,
            'product_breakdown': stage_breakdown
        }
    except Exception as e:
        log.error("Error in funnel analysis: %s", str(e))
        raise


def save_analysis_results(results_dict, reports_dir):
    """Save analysis results to files."""
    timestamp = dt.now().strftime('%Y%m%d_%H%M%S')

    for analysis_name, result in results_dict.items():
        # Handles nested dictionaries
        if isinstance(result, dict):
            for sub_name, df in result.items():
                filename = f"{analysis_name}_{sub_name}_{timestamp}.csv"
                filepath = reports_dir / filename
                df.to_csv(filepath, index=False)
                log.info("Saved %s - %s to %s",
                         analysis_name, sub_name, filepath)
        # Handles single DataFrames
        else:
            filename = f"{analysis_name}_{timestamp}.csv"
            filepath = reports_dir / filename
            result.to_csv(filepath, index=False)
            log.info("Saved %s to %s", analysis_name, filepath)

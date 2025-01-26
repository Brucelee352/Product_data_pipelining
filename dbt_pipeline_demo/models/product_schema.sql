/*
    Dimensional model with partitioning, clustering, and materialization strategies.
*/

{{ config(
    materialized='incremental',
    unique_key='session_id',
    schema='main',
    partition_by={
        'field': 'login_time',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['user_id', 'product_name']
) }}

-- Materialize frequently used dimension tables
{% set dim_tables = ['dim_user', 'dim_product', 'dim_platform'] %}
{% for table in dim_tables %}
    {{ config(materialized='table') }}
{% endfor %}

-- Optimized dimension tables using GROUP BY
WITH dim_product AS (
    SELECT 
        product_name,
        MAX(price) as price,
        MAX(CASE 
            WHEN price < 500 THEN 'Budget'
            WHEN price < 1000 THEN 'Standard'
            WHEN price < 2500 THEN 'Premium'
            ELSE 'Luxury'
        END) as price_tier
    FROM {{ ref('stg_product_schema') }}
    WHERE product_name IS NOT NULL
    GROUP BY product_name
),

dim_platform AS (
    SELECT DISTINCT
        user_agent,
        FIRST_VALUE(device_type) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as device_type,
        FIRST_VALUE(os) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as os,
        FIRST_VALUE(browser) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as browser
    FROM {{ ref('stg_product_schema') }}
    WHERE user_agent IS NOT NULL
),

-- Optimized fact table with partitioning
fact_user_activity AS (
    SELECT
        user_id,
        product_name,
        login_time,
        logout_time,
        session_duration_minutes,
        price,
        purchase_status,
        device_type,
        user_agent,
        is_active,
        account_created,
        account_updated,
        account_deleted,
        ip_address,
        {{ dbt_utils.generate_surrogate_key(['user_id', 'login_time']) }} as session_id,
        {{ dbt_utils.generate_surrogate_key(['user_id', 'login_time', 'product_name']) }} as transact_id
    FROM {{ source('raw_data', 'user_activity') }}
    {% if is_incremental() %}
        WHERE login_time > (SELECT max(login_time) FROM {{ this }})
    {% endif %}
),

row_counts AS (
    SELECT 
        COUNT(*) as fact_count
    FROM {{ source('raw_data', 'user_activity') }}
),

joined_diagnostics AS (
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT f.user_id) as distinct_users,
        COUNT(DISTINCT f.session_id) as distinct_sessions
    FROM fact_user_activity f
    LEFT JOIN {{ ref('dim_user') }} u 
        ON f.user_id = u.user_id
    LEFT JOIN dim_product p 
        ON f.product_name = p.product_name
    LEFT JOIN dim_platform pl 
        ON f.user_agent = pl.user_agent
)

-- Final optimized model
SELECT DISTINCT
    s.session_id,
    s.transact_id,
    s.login_time,
    s.logout_time,
    s.session_duration_minutes,
    s.purchase_status,
    s.price,
    s.is_active,
    s.account_created,
    s.account_updated,
    s.account_deleted,
    s.ip_address,
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    p.product_name,
    p.price_tier,
    pl.device_type,
    pl.os,
    pl.browser
FROM {{ ref('stg_product_schema') }} s
LEFT JOIN {{ ref('dim_user') }} u 
    ON s.user_id = u.user_id
LEFT JOIN dim_product p 
    ON s.product_name = p.product_name
LEFT JOIN dim_platform pl 
    ON s.user_agent = pl.user_agent
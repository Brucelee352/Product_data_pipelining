/*
    Optimized dimensional model with partitioning, clustering, and materialization strategies.
*/

{{ config(
    materialized='incremental',
    unique_key='session_id',
    partition_by={
        'field': 'login_time',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['user_id', 'product_name'],
    indexes=[
        {'columns': ['user_id']},
        {'columns': ['product_name']},
        {'columns': ['login_time']}
    ]
) }}

-- Materialize frequently used dimension tables
{% set dim_tables = ['dim_user', 'dim_product', 'dim_platform'] %}
{% for table in dim_tables %}
    {{ config(materialized='table') }}
{% endfor %}

-- Optimized dimension tables using GROUP BY
WITH dim_user AS (
    SELECT 
        user_id,
        MAX(first_name) as first_name,
        MAX(last_name) as last_name,
        MAX(email) as email,
        MAX(date_of_birth) as date_of_birth,
        MAX(address) as address,
        MAX(state) as state,
        MAX(country) as country,
        MAX(company) as company,
        MAX(job_title) as job_title,
        MAX(is_active) as is_active
    FROM {{ source('raw_data', 'user_activity') }}
    WHERE user_id IS NOT NULL
    AND is_active = TRUE
    {% if is_incremental() %}
        AND account_updated > (SELECT max(account_updated) FROM {{ this }})
    {% endif %}
    GROUP BY user_id
),

dim_product AS (
    SELECT 
        product_name,
        MAX(price) as price,
        MAX(CASE 
            WHEN price < 500 THEN 'Budget'
            WHEN price < 1000 THEN 'Standard'
            WHEN price < 2500 THEN 'Premium'
            ELSE 'Luxury'
        END) as price_tier
    FROM {{ source('raw_data', 'user_activity') }}
    WHERE product_name IS NOT NULL
    GROUP BY product_name
),

dim_platform AS (
    SELECT 
        user_agent,
        MAX(device_type) as device_type,
        MAX(os) as os,
        MAX(browser) as browser
    FROM {{ source('raw_data', 'user_activity') }}
    WHERE user_agent IS NOT NULL
    GROUP BY user_agent
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
)

-- Final optimized model
SELECT 
    f.session_id,
    f.transact_id,
    f.login_time,
    f.logout_time,
    f.session_duration_minutes,
    f.purchase_status,
    f.price,
    f.is_active,
    f.account_created,
    f.account_updated,
    f.account_deleted,
    f.ip_address,

    -- User dimensions
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    u.date_of_birth,
    u.state,
    u.country,
    u.company,
    u.job_title,
    
    -- Product dimensions with price tier
    p.product_name,
    p.price,
    p.price_tier,

    -- Platform dimensions
    pl.device_type,
    pl.os,
    pl.browser
FROM fact_user_activity f
LEFT JOIN dim_user u 
    ON f.user_id = u.user_id
LEFT JOIN dim_product p 
    ON f.product_name = p.product_name
LEFT JOIN dim_platform pl 
    ON f.device_type = pl.device_type
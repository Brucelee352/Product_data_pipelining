{{
    config(
        materialized='table'
    )
}}

-- Materialize frequently used dimension tables
{% set dim_tables = ['dim_user', 'dim_product', 'dim_platform'] %}


-- Final select statement to materialize queriable data 
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
    u.job_title,
    p.product_name,
    p.price_tier,
    pl.device_type,
    pl.os,
    pl.browser
FROM {{ ref('stg_product_schema') }} s
LEFT JOIN {{ ref('dim_user') }} u 
    ON s.user_id = u.user_id
LEFT JOIN {{ ref('dim_product') }} p 
    ON s.product_name = p.product_name
LEFT JOIN {{ ref('dim_platform') }} pl 
    ON s.user_agent = pl.user_agent
{% if is_incremental() %}
WHERE s.login_time > (SELECT MAX(login_time) FROM {{ this }})
{% endif %}

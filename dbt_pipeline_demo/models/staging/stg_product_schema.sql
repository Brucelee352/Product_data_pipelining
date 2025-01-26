{{ config(
    materialized='table',
    schema='main'
) }}

-- Basic transformations and cleaning
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
    device_type,
    os,
    browser,
    {{ dbt_utils.generate_surrogate_key(['user_id', 'login_time']) }} as session_id,
    {{ dbt_utils.generate_surrogate_key(['user_id', 'login_time', 'product_name']) }} as transact_id
FROM {{ source('raw_data', 'user_activity') }} 
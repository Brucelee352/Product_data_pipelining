{{
    config(
        materialized='view',
        indexes=[{'columns': ['user_id']}]
    )
}}

SELECT
    user_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    country,
    company,
    job_title,
    is_active,
    login_time,
    logout_time,
    session_duration_minutes,
    product_name,
    price,
    purchase_status,
    device_type,
    os,
    browser
FROM {{ source('raw_data', 'user_activity') }} 
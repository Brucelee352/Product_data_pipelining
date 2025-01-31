{{
    config(
        materialized='table',
        unique_key='transact_id'
    )
}}

WITH user_activity AS (
    SELECT DISTINCT
        du.user_key,
        ua.user_id,
        sps.transact_id,
        ua.login_time,
        ua.logout_time,
        ua.session_duration_minutes,
        sps.product_name,
        sps.price,
        sps.purchase_status,
        sps.device_type,
        sps.os,
        sps.browser
    FROM {{ ref('stg_user_activity') }} ua
    LEFT JOIN {{ ref('dim_user') }} du
        ON ua.user_id = du.user_id
    LEFT JOIN {{ ref('stg_product_schema') }} sps
        ON ua.user_id = sps.user_id
)

SELECT
    user_key,
    user_id,
    transact_id,
    login_time,
    logout_time,
    session_duration_minutes,
    product_name,
    price,
    purchase_status,
    device_type,
    os,
    browser
FROM user_activity 
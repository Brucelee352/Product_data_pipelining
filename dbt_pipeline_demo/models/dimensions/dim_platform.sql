-- If used on data that is larger outside of this project, this should be materialized
-- as an incremental table.

{{
    config(
        materialized='table',
        indexes=[{'columns': ['user_agent']}]
    )
}}

-- Optimizes the platform dimension table by grouping by user_agent and selecting the latest values
SELECT DISTINCT
    user_agent,
    FIRST_VALUE(device_type) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as device_type,
    FIRST_VALUE(os) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as os,
    FIRST_VALUE(browser) OVER (PARTITION BY user_agent ORDER BY login_time DESC) as browser
FROM {{ ref('stg_product_schema') }}
WHERE user_agent IS NOT NULL

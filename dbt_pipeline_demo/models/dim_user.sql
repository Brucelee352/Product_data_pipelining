-- models/dim_user.sql

{{ config(
    materialized='table',
    indexes=[{'columns': ['user_id']}]
) }}

-- Let's add a simple check to see if we're getting any data at all
WITH source_check AS (
    SELECT COUNT(*) as record_count
    FROM {{ source('raw_data', 'user_activity') }}
)
SELECT 
    user_id,
    MAX(first_name) AS first_name,
    MAX(last_name) AS last_name,
    MAX(email) AS email,
    MAX(date_of_birth) AS date_of_birth,
    MAX(address) AS address,
    MAX(state) AS state,
    MAX(country) AS country,
    MAX(company) AS company,
    MAX(job_title) AS job_title,
    MAX(is_active) AS is_active
FROM {{ source('raw_data', 'user_activity') }}
WHERE user_id IS NOT NULL
  AND is_active = 'yes'
{% if is_incremental() %}
  AND account_updated > (SELECT MAX(account_updated) FROM {{ this }})
{% endif %}
GROUP BY user_id
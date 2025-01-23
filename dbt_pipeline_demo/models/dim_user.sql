-- models/dim_user.sql
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
  AND is_active = TRUE
{% if is_incremental() %}
  AND account_updated > (SELECT MAX(account_updated) FROM {{ this }})
{% endif %}
GROUP BY user_id

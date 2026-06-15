SELECT
    location_id,
    name,
    chain,
    city,
    state,
    zip_code,
    latitude,
    longitude
FROM {{ ref('stg_locations') }}

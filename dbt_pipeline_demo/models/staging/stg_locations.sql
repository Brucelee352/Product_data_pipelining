SELECT
    location_id,
    name,
    chain,
    address_line1,
    city,
    state,
    zip_code,
    latitude,
    longitude,
    fetched_at
FROM {{ source('kroger_raw', 'locations') }}
WHERE location_id IS NOT NULL AND latitude IS NOT NULL

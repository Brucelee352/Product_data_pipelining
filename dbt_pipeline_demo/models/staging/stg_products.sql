SELECT
    product_id,
    description,
    brand,
    json_extract_string(categories, '$[0]') AS primary_category,
    categories,
    fetched_at
FROM {{ source('kroger_raw', 'products') }}
WHERE product_id IS NOT NULL

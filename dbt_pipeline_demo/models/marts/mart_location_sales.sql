WITH location_fulfillment AS (
    SELECT
        location_id,
        COUNT(DISTINCT CASE WHEN fulfillment_instore OR fulfillment_curbside THEN product_id END) AS physical_count,
        COUNT(DISTINCT CASE WHEN fulfillment_delivery OR fulfillment_shiptohome THEN product_id END) AS online_count,
        COUNT(DISTINCT product_id) AS total_products,
        ROUND(AVG(regular_price), 2) AS avg_price
    FROM {{ ref('fact_prices') }}
    GROUP BY location_id
)
SELECT
    dl.location_id,
    dl.name,
    dl.city,
    dl.state,
    dl.zip_code,
    dl.latitude,
    dl.longitude,
    lf.physical_count,
    lf.online_count,
    lf.total_products,
    lf.avg_price
FROM {{ ref('dim_locations') }} dl
JOIN location_fulfillment lf ON dl.location_id = lf.location_id

WITH location_fulfillment AS (
    SELECT
        location_id,
        SUM(CASE WHEN fulfillment_instore THEN 1 ELSE 0 END) AS instore_count,
        SUM(CASE WHEN fulfillment_delivery THEN 1 ELSE 0 END) AS delivery_count,
        SUM(CASE WHEN fulfillment_curbside THEN 1 ELSE 0 END) AS curbside_count,
        SUM(CASE WHEN fulfillment_shiptohome THEN 1 ELSE 0 END) AS shiptohome_count,
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
    dl.latitude,
    dl.longitude,
    lf.instore_count,
    lf.delivery_count,
    lf.curbside_count,
    lf.shiptohome_count,
    lf.total_products,
    lf.avg_price,
    GREATEST(lf.instore_count, lf.delivery_count, lf.curbside_count, lf.shiptohome_count) AS max_fulfillment_count,
    CASE
        WHEN lf.instore_count >= lf.delivery_count AND lf.instore_count >= lf.curbside_count
             AND lf.instore_count >= lf.shiptohome_count THEN 'In Store'
        WHEN lf.delivery_count >= lf.curbside_count AND lf.delivery_count >= lf.shiptohome_count THEN 'Delivery'
        WHEN lf.curbside_count >= lf.shiptohome_count THEN 'Curbside'
        ELSE 'Ship to Home'
    END AS dominant_fulfillment
FROM {{ ref('dim_locations') }} dl
JOIN location_fulfillment lf ON dl.location_id = lf.location_id

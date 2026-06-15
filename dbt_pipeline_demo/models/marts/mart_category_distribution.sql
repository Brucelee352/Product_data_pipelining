SELECT
    category,
    COUNT(DISTINCT product_id) AS product_count,
    ROUND(AVG(regular_price), 2) AS avg_regular_price,
    ROUND(AVG(promo_price), 2) AS avg_promo_price
FROM {{ ref('fact_prices') }}
WHERE category IS NOT NULL
GROUP BY category
ORDER BY product_count DESC

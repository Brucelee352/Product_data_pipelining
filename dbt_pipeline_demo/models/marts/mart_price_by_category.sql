SELECT
    category,
    effective_date,
    ROUND(AVG(regular_price), 2) AS avg_regular_price,
    ROUND(AVG(COALESCE(promo_price, regular_price)), 2) AS avg_promo_price,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    COUNT(DISTINCT product_id) AS product_count,
    MIN(regular_price) AS min_price,
    MAX(regular_price) AS max_price,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY regular_price) AS q1_price,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY regular_price) AS median_price,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY regular_price) AS q3_price
FROM {{ ref('fact_prices') }}
WHERE category IS NOT NULL AND regular_price > 0
GROUP BY category, effective_date
ORDER BY effective_date, category

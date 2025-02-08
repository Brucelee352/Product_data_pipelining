-- If used on data that is larger outside of this project, this should be materialized
-- as an incremental table.

{{
    config(
        materialized='table',
        indexes=[{'columns': ['product_name']}]
    )
}}

-- Optimize product dimension table by group by product_name

SELECT 
    product_name,
    MAX(price) as price,
    CASE 
        WHEN price <= 1 THEN 'Invalid'
        WHEN price <= 1000 THEN 'Budget'
        WHEN price <= 2500 THEN 'Standard'
        WHEN price <= 5000 THEN 'Premium'
        ELSE 'Luxury'
    END as price_tier
FROM {{ ref('stg_product_schema') }}
WHERE product_name IS NOT NULL
GROUP BY price_tier, product_name, price

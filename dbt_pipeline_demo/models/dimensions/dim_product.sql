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
    price_tier
FROM {{ ref('stg_product_schema') }}
WHERE product_name IS NOT NULL
GROUP BY product_name, price_tier

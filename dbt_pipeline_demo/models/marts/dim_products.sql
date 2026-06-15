SELECT DISTINCT
    p.product_id,
    p.description,
    p.brand,
    p.primary_category AS category,
    p.categories
FROM {{ ref('stg_products') }} p

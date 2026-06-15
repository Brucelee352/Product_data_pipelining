SELECT
    sp.product_id,
    sp.location_id,
    sp.item_id,
    dp.description,
    dp.brand,
    dp.category,
    dl.city,
    dl.state,
    dl.latitude,
    dl.longitude,
    sp.regular_price,
    sp.promo_price,
    sp.discount_amount,
    sp.discount_pct,
    sp.effective_date,
    sp.fulfillment_instore,
    sp.fulfillment_delivery,
    sp.fulfillment_curbside,
    sp.fulfillment_shiptohome,
    sp.stock_level
FROM {{ ref('stg_prices') }} sp
LEFT JOIN {{ ref('dim_products') }} dp ON sp.product_id = dp.product_id
LEFT JOIN {{ ref('dim_locations') }} dl ON sp.location_id = dl.location_id

SELECT
    pp.product_id,
    pp.location_id,
    pp.item_id,
    pp.size,
    pp.regular_price,
    pp.promo_price,
    pp.regular_price - COALESCE(pp.promo_price, pp.regular_price) AS discount_amount,
    CASE WHEN pp.promo_price IS NOT NULL AND pp.promo_price < pp.regular_price
         THEN ROUND((pp.regular_price - pp.promo_price) / pp.regular_price * 100, 2)
         ELSE 0 END AS discount_pct,
    TRY_CAST(pp.effective_date AS DATE) AS effective_date,
    TRY_CAST(pp.expiration_date AS DATE) AS expiration_date,
    pp.fulfillment_instore,
    pp.fulfillment_delivery,
    pp.fulfillment_curbside,
    pp.fulfillment_shiptohome,
    pp.stock_level,
    pp.fetched_at
FROM {{ source('kroger_raw', 'product_prices') }} pp
WHERE pp.regular_price > 0

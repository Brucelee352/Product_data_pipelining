{% docs __overview__ %}
# Kroger Pipeline — dbt Project

This dbt project transforms raw Kroger API data into analytics-ready tables backed by DuckDB.
Raw data is fetched from the Kroger Locations, Products, and Product Prices APIs, landed in the
`kroger_raw` schema, then layered through staging views and dimension/fact/mart tables.

## Layer summary

| Layer | Materialization | Schema | Purpose |
|---|---|---|---|
| Staging | View | `staging` | Clean and type-cast raw API responses |
| Dimensions | Table | `marts` | Deduplicated reference entities |
| Fact | Table | `marts` | Priced product–location combinations |
| Marts | Table | `marts` | Pre-aggregated analytics outputs |

## DAG

```
kroger_raw.locations ─┐
                       ├─ stg_locations ── dim_locations ──┐
                       │                                    ├─ fact_prices ──┬─ mart_category_distribution
kroger_raw.products ──┤                                    │                 ├─ mart_price_by_category
                       ├─ stg_products ── dim_products ────┘                 └─ mart_location_sales
                       │
kroger_raw.product_prices ── stg_prices ──────────────────┘
```

## Profile / target

- **Dev**: `databases/kroger_pipeline.duckdb` (single thread)
- **Prod**: `databases/prod.duckdb` (4 threads)

Run `dbt run --target dev --full-refresh` from inside `dbt_pipeline_demo/` to rebuild all models.
{% enddocs %}


{% docs stg_products %}
Cleaned product catalog sourced from `kroger_raw.products`.

`primary_category` is extracted from the `categories` JSON array using DuckDB's
`json_extract_string(categories, '$[0]')`. Rows with a null `product_id` are dropped.
The full `categories` JSON string is preserved for downstream consumers that need the
complete category hierarchy.
{% enddocs %}


{% docs stg_locations %}
Cleaned Kroger store locations sourced from `kroger_raw.locations`.

Rows are filtered to records where both `location_id` and `latitude` are non-null,
ensuring every downstream location join and geospatial query has valid coordinates.
All address fields and the raw `fetched_at` timestamp are passed through unchanged.
{% enddocs %}


{% docs stg_prices %}
Cleaned product pricing sourced from `kroger_raw.product_prices`.

Two derived discount fields are computed here and nowhere else:

- `discount_amount` = `regular_price − COALESCE(promo_price, regular_price)` — zero when
  no promo price exists.
- `discount_pct` = percentage off regular price, rounded to 2 decimal places, zero when
  `promo_price` is null or not lower than `regular_price`.

`effective_date` and `expiration_date` are cast with `TRY_CAST(... AS DATE)` to handle
malformed API strings without erroring. Rows where `regular_price ≤ 0` are excluded.

All four fulfillment flags (`fulfillment_instore`, `fulfillment_delivery`,
`fulfillment_curbside`, `fulfillment_shiptohome`) and `stock_level` are passed through
for use in `fact_prices` and `mart_location_sales`.
{% enddocs %}


{% docs dim_products %}
Deduplicated product dimension built from `stg_products`.

A `SELECT DISTINCT` ensures one row per `product_id`. `primary_category` is aliased to
`category` for consistency across all downstream models. The raw `categories` JSON string
is retained for ad-hoc category hierarchy queries.
{% enddocs %}


{% docs dim_locations %}
Store location dimension built from `stg_locations`.

Drops `address_line1` and `fetched_at` (operational fields not needed in analytics) and
retains the full geographic grain: `city`, `state`, `zip_code`, `latitude`, `longitude`.
{% enddocs %}


{% docs fact_prices %}
Central fact table joining price observations with product and location context.

Grain: one row per (`product_id`, `location_id`, `item_id`) price record. Products and
locations are LEFT JOINed from their respective dimensions so price records are never lost
even if a dimension row is missing.

All four fulfillment flags, `stock_level`, `discount_amount`, `discount_pct`, and
`effective_date` from `stg_prices` are included so mart models can filter and aggregate
without re-joining to staging.
{% enddocs %}


{% docs mart_category_distribution %}
Product count and average pricing rolled up by category.

Aggregates `fact_prices` to one row per category. `product_count` uses `COUNT DISTINCT`
on `product_id` so a product sold at multiple locations is counted once. Rows where
`category` is null are excluded. Results are ordered by `product_count` descending.
{% enddocs %}


{% docs mart_price_by_category %}
Full price distribution statistics by category and effective date.

Provides the full five-number summary (min, Q1, median, Q3, max) plus average regular and
effective promo prices, average discount percentage, and distinct product count. Grain is
`(category, effective_date)`. Null-category rows and zero-price records are excluded.

`avg_promo_price` uses `COALESCE(promo_price, regular_price)` so non-promoted products
contribute their regular price rather than a null gap.
{% enddocs %}


{% docs mart_location_sales %}
Per-store fulfillment breakdown and product totals.

Classifies each product–location combination as physical (instore OR curbside) or online
(delivery OR shiptohome) using `COUNT DISTINCT ... CASE WHEN` logic, then joins to
`dim_locations` to attach store name, city, state, zip, and coordinates. Grain is one row
per `location_id`.
{% enddocs %}

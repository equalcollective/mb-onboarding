# Metabase Cards - SQL Queries

Create these cards in Metabase under a new collection (e.g., "Data Pipeline - Raw Data").

---

## Card 1: Raw Business Report Data

**Name**: `[Pipeline] Raw Business Report Data`

**Type**: Native Query

**Variables to create**:
- `seller_name`: Field Filter → `orange_schema.sellers.name`
- `granularity`: Text (default: `weekly`)
- `start_date`: Date
- `end_date`: Date

```sql
-- Raw Business Report data for pipeline consumption
-- Returns child-level sales/traffic data with seller info

SELECT
    s.id AS seller_id,
    s.name AS seller_name,
    s.seller_id AS amazon_seller_id,
    br.child_asin,
    br.parent_asin,
    br.period_start_date,
    br.period_end_date,
    br.period_granularity,
    COALESCE(br.ordered_product_sales, 0) AS ordered_product_sales,
    COALESCE(br.sessions_total, 0) AS sessions_total,
    COALESCE(br.units_ordered, 0) AS units_ordered,
    COALESCE(br.buy_box_percentage, 0) AS buy_box_percentage,
    COALESCE(br.page_views_total, 0) AS page_views_total,
    COALESCE(br.units_refunded, 0) AS units_refunded
FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child br
INNER JOIN orange_schema.sellers s ON br.seller_id = s.seller_id
WHERE 1=1
    [[AND {{seller_name}}]]
    [[AND br.period_granularity = {{granularity}}]]
    [[AND br.period_start_date >= {{start_date}}]]
    [[AND br.period_start_date <= {{end_date}}]]
ORDER BY s.name, br.period_start_date DESC, br.child_asin
```

---

## Card 2: Raw Ads Report Data

**Name**: `[Pipeline] Raw Ads Report Data`

**Type**: Native Query

**Variables to create**:
- `seller_name`: Field Filter → `orange_schema.sellers.name`
- `start_date`: Date
- `end_date`: Date

```sql
-- Raw Advertising data for pipeline consumption
-- Returns daily ad performance by child ASIN

SELECT
    s.id AS seller_id,
    s.name AS seller_name,
    s.seller_id AS amazon_seller_id,
    ar.advertised_asin AS child_asin,
    ar.record_date,
    ar.campaign_name,
    ar.ad_group_name,
    COALESCE(ar.impressions, 0) AS impressions,
    COALESCE(ar.clicks, 0) AS clicks,
    COALESCE(ar.spend, 0) AS spend,
    COALESCE(ar.seven_day_total_sales, 0) AS seven_day_total_sales,
    COALESCE(ar.seven_day_total_orders, 0) AS seven_day_total_orders,
    COALESCE(ar.seven_day_total_units, 0) AS seven_day_total_units
FROM orange_schema.rpt_sponsored_products_advertised_product ar
INNER JOIN orange_schema.sellers s ON ar.seller_id = s.seller_id
WHERE 1=1
    [[AND {{seller_name}}]]
    [[AND ar.record_date >= {{start_date}}]]
    [[AND ar.record_date <= {{end_date}}]]
ORDER BY s.name, ar.record_date DESC, ar.advertised_asin
```

---

## Card 3: ASIN Mapping

**Name**: `[Pipeline] ASIN Mapping`

**Type**: Native Query

**Variables to create**:
- `seller_name`: Field Filter → `orange_schema.mv_asin_details.seller_name`

```sql
-- ASIN mapping from child to parent to normalized name
-- Used for joining and aggregation

SELECT
    asin AS child_asin,
    parent_asin,
    adjusted_parent_asin,
    normalized_name,
    adjusted_normalized_name,
    variant_name,
    adjusted_variant_name,
    title,
    asin_brand,
    asin_link,
    category_root,
    category_sub,
    search_rank_current,
    subcategory_rank,
    rank_top_percent,
    seller_id,
    seller_name,
    seller_marketplace,
    amazon_seller_id
FROM orange_schema.mv_asin_details
WHERE 1=1
    [[AND {{seller_name}}]]
ORDER BY seller_name, adjusted_normalized_name, variant_name
```

---

## Card 4: Sellers List

**Name**: `[Pipeline] Sellers List`

**Type**: Native Query

**Variables**: None

```sql
-- List of all sellers
-- Used for dropdowns and validation

SELECT DISTINCT
    s.id AS seller_id,
    s.name AS seller_name,
    s.seller_id AS amazon_seller_id,
    s.marketplace,
    COUNT(DISTINCT mv.asin) AS asin_count
FROM orange_schema.sellers s
LEFT JOIN orange_schema.mv_asin_details mv ON s.id = mv.seller_id
GROUP BY s.id, s.name, s.seller_id, s.marketplace
ORDER BY s.name
```

---

## Card 5: Data Gaps - Weekly

**Name**: `[Pipeline] Data Gaps - Weekly`

**Type**: Native Query

**Variables to create**:
- `seller_name`: Field Filter → `orange_schema.sellers.name`

```sql
-- Identify missing weeks in business report data
-- Compares expected weeks against actual data

WITH date_bounds AS (
    SELECT
        MIN(period_start_date) AS min_date,
        MAX(period_start_date) AS max_date
    FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child
    WHERE period_granularity = 'weekly'
),
sellers_with_data AS (
    SELECT DISTINCT
        s.id AS seller_id,
        s.name AS seller_name
    FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child br
    INNER JOIN orange_schema.sellers s ON br.seller_id = s.seller_id
    WHERE br.period_granularity = 'weekly'
),
expected_weeks AS (
    SELECT
        generate_series(
            (SELECT min_date FROM date_bounds),
            (SELECT max_date FROM date_bounds),
            '1 week'::interval
        )::date AS expected_week_start
),
seller_expected AS (
    SELECT
        swd.seller_id,
        swd.seller_name,
        ew.expected_week_start
    FROM sellers_with_data swd
    CROSS JOIN expected_weeks ew
),
actual_weeks AS (
    SELECT DISTINCT
        s.id AS seller_id,
        br.period_start_date AS actual_week_start
    FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child br
    INNER JOIN orange_schema.sellers s ON br.seller_id = s.seller_id
    WHERE br.period_granularity = 'weekly'
)
SELECT
    se.seller_id,
    se.seller_name,
    se.expected_week_start AS missing_week_start,
    se.expected_week_start + INTERVAL '6 days' AS missing_week_end,
    'weekly' AS granularity
FROM seller_expected se
LEFT JOIN actual_weeks aw
    ON se.seller_id = aw.seller_id
    AND se.expected_week_start = aw.actual_week_start
WHERE aw.actual_week_start IS NULL
    [[AND {{seller_name}}]]
ORDER BY se.seller_name, se.expected_week_start
```

---

## Card 6: Data Gaps - Monthly

**Name**: `[Pipeline] Data Gaps - Monthly`

**Type**: Native Query

**Variables to create**:
- `seller_name`: Field Filter → `orange_schema.sellers.name`

```sql
-- Identify missing months in business report data
-- Compares expected months against actual data

WITH date_bounds AS (
    SELECT
        DATE_TRUNC('month', MIN(period_start_date))::date AS min_date,
        DATE_TRUNC('month', MAX(period_start_date))::date AS max_date
    FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child
    WHERE period_granularity = 'monthly'
),
sellers_with_data AS (
    SELECT DISTINCT
        s.id AS seller_id,
        s.name AS seller_name
    FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child br
    INNER JOIN orange_schema.sellers s ON br.seller_id = s.seller_id
    WHERE br.period_granularity = 'monthly'
),
expected_months AS (
    SELECT
        generate_series(
            (SELECT min_date FROM date_bounds),
            (SELECT max_date FROM date_bounds),
            '1 month'::interval
        )::date AS expected_month_start
),
seller_expected AS (
    SELECT
        swd.seller_id,
        swd.seller_name,
        em.expected_month_start
    FROM sellers_with_data swd
    CROSS JOIN expected_months em
),
actual_months AS (
    SELECT DISTINCT
        s.id AS seller_id,
        br.period_start_date AS actual_month_start
    FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child br
    INNER JOIN orange_schema.sellers s ON br.seller_id = s.seller_id
    WHERE br.period_granularity = 'monthly'
)
SELECT
    se.seller_id,
    se.seller_name,
    se.expected_month_start AS missing_month_start,
    (se.expected_month_start + INTERVAL '1 month' - INTERVAL '1 day')::date AS missing_month_end,
    TO_CHAR(se.expected_month_start, 'FMMonth YYYY') AS missing_month_name,
    'monthly' AS granularity
FROM seller_expected se
LEFT JOIN actual_months am
    ON se.seller_id = am.seller_id
    AND se.expected_month_start = am.actual_month_start
WHERE am.actual_month_start IS NULL
    [[AND {{seller_name}}]]
ORDER BY se.seller_name, se.expected_month_start
```

---

## How to Create These Cards

1. Go to Metabase → **New** → **Question** → **Native query**
2. Select database: **Jeff Azure Db Public**
3. Paste the SQL
4. For variables (like `{{seller_name}}`):
   - Click the variable in the sidebar
   - Set the type (Field Filter, Text, or Date)
   - Map to the correct field if it's a Field Filter
5. Save to a collection (create "Data Pipeline - Raw Data" collection)
6. Note down the Card IDs after saving (visible in the URL)

---

## Card IDs (Fill in after creating)

After creating each card, note the ID from the URL (e.g., `/question/123`):

```
Card 1 (Business Report): ______
Card 2 (Ads Report):      ______
Card 3 (ASIN Mapping):    ______
Card 4 (Sellers List):    ______
Card 5 (Gaps Weekly):     ______
Card 6 (Gaps Monthly):    ______
```

Share these IDs with me once created, and I'll configure them in the Python app.

-- ============================================================================
-- ADS REPORT: Sponsored Products Advertised Product with ASIN Mapping
-- ============================================================================
-- Purpose: Raw advertising data at child ASIN level with parent/normalized mapping
--
-- Filters (Metabase Template Tags):
--   - seller_name: Text
--   - start_date: Date
--   - end_date: Date
--
-- Note: Using CTE to avoid Metabase alias issues with filters
-- Note: Only includes SELF-managed sellers (agency_name = 'SELF')
-- ============================================================================

WITH asin_details AS (
    SELECT
        asin,
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
        rank_subcategory_name,
        seller_id,
        seller_name,
        seller_marketplace
    FROM orange_schema.mv_asin_details
    WHERE 1=1
        AND agency_name = 'SELF'
        [[AND seller_name = {{seller_name}}]]
)

SELECT
    -- Identifiers
    ar.id AS report_row_id,
    ar.seller_id AS amazon_seller_id,
    ad.seller_id,
    ad.seller_name,
    ad.seller_marketplace,

    -- ASIN Hierarchy
    ar.advertised_asin AS child_asin,
    ad.parent_asin,
    ad.adjusted_parent_asin,
    ad.normalized_name,
    ad.adjusted_normalized_name,
    ad.variant_name,
    ad.adjusted_variant_name,

    -- Product Info
    ad.title,
    ad.asin_brand,
    ad.asin_link,
    ad.category_root,
    ad.category_sub,

    -- Rankings
    ad.search_rank_current,
    ad.subcategory_rank,
    ad.rank_top_percent,
    ad.rank_subcategory_name,

    -- Time
    ar.record_date,
    ar.report_date,

    -- Campaign Info
    ar.campaign_name,
    ar.ad_group_name,
    ar.portfolio_name,
    ar.advertised_sku,

    -- Core Metrics
    COALESCE(ar.impressions, 0) AS impressions,
    COALESCE(ar.clicks, 0) AS clicks,
    COALESCE(ar.spend, 0) AS spend,
    COALESCE(ar.seven_day_total_sales, 0) AS seven_day_total_sales,
    COALESCE(ar.seven_day_total_orders, 0) AS seven_day_total_orders,
    COALESCE(ar.seven_day_total_units, 0) AS seven_day_total_units,

    -- Calculated Metrics (from source)
    ar.ctr,
    ar.cpc,
    ar.total_acos,
    ar.total_roas,
    ar.seven_day_conversion_rate,

    -- SKU Breakdown
    ar.seven_day_advertised_sku_units,
    ar.seven_day_other_sku_units,
    ar.seven_day_advertised_sku_sales,
    ar.seven_day_other_sku_sales,

    -- Metadata
    ar.currency,
    ar.country,
    ar.marketplace

FROM orange_schema.rpt_sponsored_products_advertised_product ar
INNER JOIN asin_details ad
    ON ar.advertised_asin = ad.asin

WHERE 1=1
    [[AND ar.record_date >= {{start_date}}]]
    [[AND ar.record_date <= {{end_date}}]]

ORDER BY ad.seller_name, ar.record_date DESC, ad.adjusted_normalized_name

-- ============================================================================
-- ASIN MAPPING: Child to Parent to Normalized Name Hierarchy
-- ============================================================================
-- Purpose: ASIN lookup table for mapping and aggregation
--
-- Filters (Metabase Template Tags):
--   - seller_name: Field Filter → mv_asin_details.seller_name
--   - agency_name: Field Filter → mv_asin_details.agency_name
--
-- Note: Only includes SELF-managed sellers (agency_name = 'SELF')
--
-- Source: mv_asin_details (materialized view)
-- ============================================================================

SELECT
    -- ASIN Hierarchy
    asin AS child_asin,
    parent_asin,
    adjusted_parent_asin,
    normalized_name,
    adjusted_normalized_name,
    variant_name,
    adjusted_variant_name,

    -- Product Info
    title,
    asin_brand,
    asin_link,

    -- Categories
    category_root,
    category_sub,
    category_tree,

    -- Rankings
    search_rank_current,
    subcategory_rank,
    rank_top_percent,
    rank_subcategory_name,

    -- Brand Info
    brand_id,
    brand_canonical_name,
    brand_url,
    brand_aliases,

    -- Seller Info
    seller_id,
    seller_name,
    seller_marketplace,
    amazon_seller_id,

    -- Agency
    agency_id,
    agency_name,

    -- Locale
    locale,

    -- Timestamps
    asin_created_at,
    asin_updated_at

FROM orange_schema.mv_asin_details

WHERE 1=1
    AND agency_name = 'SELF'
    [[AND {{seller_name}}]]

ORDER BY seller_name, adjusted_normalized_name, adjusted_variant_name

-- ============================================================================
-- BUSINESS REPORT: Detail Page Sales & Traffic by Child ASIN
-- ============================================================================
-- Purpose: Raw business report data at child ASIN level with parent/normalized mapping
--
-- Filters (Metabase Template Tags):
--   - seller_name: Text
--   - start_date: Date
--   - end_date: Date
--   - granularity: Text (weekly/monthly)
--
-- Note: Using CTE to avoid Metabase alias issues with filters
-- Note: Only includes SELF-managed sellers (agency_name = 'SELF')
-- ============================================================================

WITH asin_details AS (
    SELECT
        asin,
        parent_asin AS mapped_parent_asin,
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
    br.id AS report_row_id,
    br.seller_id AS amazon_seller_id,
    ad.seller_id,
    ad.seller_name,
    ad.seller_marketplace,

    -- ASIN Hierarchy
    br.child_asin,
    br.parent_asin,
    ad.adjusted_parent_asin,
    ad.normalized_name,
    ad.adjusted_normalized_name,
    ad.variant_name,
    ad.adjusted_variant_name,

    -- Product Info
    br.product_title,
    ad.title AS mapped_title,
    ad.asin_brand,
    ad.asin_link,
    ad.category_root,
    ad.category_sub,

    -- Rankings
    ad.search_rank_current,
    ad.subcategory_rank,
    ad.rank_top_percent,
    ad.rank_subcategory_name,

    -- Time Period
    br.period_granularity,
    br.period_start_date,
    br.period_end_date,
    br.report_date,

    -- Sessions (B2C)
    COALESCE(br.sessions_total, 0) AS sessions_total,
    COALESCE(br.sessions_browser, 0) AS sessions_browser,
    COALESCE(br.sessions_mobile_app, 0) AS sessions_mobile_app,
    br.session_percentage_total,
    br.session_percentage_browser,
    br.session_percentage_mobile_app,

    -- Sessions (B2B)
    COALESCE(br.sessions_total_b2b, 0) AS sessions_total_b2b,
    COALESCE(br.sessions_browser_b2b, 0) AS sessions_browser_b2b,
    COALESCE(br.sessions_mobile_app_b2b, 0) AS sessions_mobile_app_b2b,
    br.session_percentage_total_b2b,
    br.session_percentage_browser_b2b,
    br.session_percentage_mobile_app_b2b,

    -- Page Views (B2C)
    COALESCE(br.page_views_total, 0) AS page_views_total,
    COALESCE(br.page_views_browser, 0) AS page_views_browser,
    COALESCE(br.page_views_mobile_app, 0) AS page_views_mobile_app,
    br.page_views_percentage_total,
    br.page_views_percentage_browser,
    br.page_views_percentage_mobile_app,

    -- Page Views (B2B)
    COALESCE(br.page_views_total_b2b, 0) AS page_views_total_b2b,
    COALESCE(br.page_views_browser_b2b, 0) AS page_views_browser_b2b,
    COALESCE(br.page_views_mobile_app_b2b, 0) AS page_views_mobile_app_b2b,
    br.page_views_percentage_total_b2b,
    br.page_views_percentage_browser_b2b,
    br.page_views_percentage_mobile_app_b2b,

    -- Buy Box
    br.buy_box_percentage,
    br.buy_box_percentage_b2b,

    -- Units Ordered (B2C + B2B)
    COALESCE(br.units_ordered, 0) AS units_ordered,
    COALESCE(br.units_ordered_b2b, 0) AS units_ordered_b2b,
    COALESCE(br.units_ordered, 0) + COALESCE(br.units_ordered_b2b, 0) AS units_ordered_total,

    -- Unit Session Percentage (Conversion Rate)
    br.unit_session_percentage,
    br.unit_session_percentage_b2b,

    -- Ordered Product Sales (B2C + B2B)
    COALESCE(br.ordered_product_sales, 0) AS ordered_product_sales,
    COALESCE(br.ordered_product_sales_b2b, 0) AS ordered_product_sales_b2b,
    COALESCE(br.ordered_product_sales, 0) + COALESCE(br.ordered_product_sales_b2b, 0) AS ordered_product_sales_total,

    -- Total Order Items (B2C + B2B)
    COALESCE(br.total_order_items, 0) AS total_order_items,
    COALESCE(br.total_order_items_b2b, 0) AS total_order_items_b2b,
    COALESCE(br.total_order_items, 0) + COALESCE(br.total_order_items_b2b, 0) AS total_order_items_total,

    -- Refunds (B2C + B2B)
    COALESCE(br.units_refunded, 0) AS units_refunded,
    COALESCE(br.units_refunded_b2b, 0) AS units_refunded_b2b,
    br.refund_rate,
    br.refund_rate_b2b,

    -- Metadata
    br.currency,
    br.marketplace,
    br.created_at,
    br.updated_at

FROM orange_schema.rpt_br_detail_page_sales_traffic_by_child br
INNER JOIN asin_details ad
    ON br.child_asin = ad.asin

WHERE 1=1
    [[AND br.period_start_date >= {{start_date}}]]
    [[AND br.period_start_date <= {{end_date}}]]
    [[AND br.period_granularity = {{granularity}}]]

ORDER BY ad.seller_name, br.period_start_date DESC, ad.adjusted_normalized_name

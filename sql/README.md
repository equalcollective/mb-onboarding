# SQL Queries Registry

This folder contains all SQL queries used in the MB Onboarding pipeline.

## Query Files

| File | Metabase Card | Purpose |
|------|---------------|---------|
| `ads_report.sql` | TBD | Raw advertising data with ASIN mapping |
| `business_report.sql` | TBD | Raw business report data with ASIN mapping |
| `asin_mapping.sql` | TBD | ASIN hierarchy (child → parent → normalized) |
| `sellers_list.sql` | TBD | List of all sellers |
| `data_gaps_weekly.sql` | TBD | Missing weekly data periods |
| `data_gaps_monthly.sql` | TBD | Missing monthly data periods |

## Common Patterns

### Filters (Template Tags)

All queries use Metabase template tags for filtering:

```sql
[[AND {{seller_name}}]]           -- Field filter on seller name
[[AND ar.record_date >= {{start_date}}]]  -- Date filter
[[AND ar.record_date <= {{end_date}}]]    -- Date filter
```

### ASIN Mapping Join

All data queries join with `mv_asin_details` for the ASIN hierarchy:

```sql
LEFT JOIN orange_schema.mv_asin_details mv
    ON ar.advertised_asin = mv.asin
```

## Dependencies

```
mv_asin_details (materialized view)
├── Used by: ads_report.sql, business_report.sql
└── Provides: parent_asin, normalized_name, variant_name, seller info

sellers (table)
├── Used by: sellers_list.sql, data_gaps queries
└── Provides: seller_id, seller_name mapping
```

## Updating Queries

When updating queries:
1. Update the `.sql` file in this folder
2. Update the corresponding Metabase card
3. Test with `test_metabase.py`
4. Update card ID in `.env` if changed

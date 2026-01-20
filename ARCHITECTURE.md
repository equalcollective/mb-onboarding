# Amazon Seller Analytics Pipeline

A data processing pipeline that fetches Amazon seller data from Metabase, processes metrics in Python, and exposes them via REST API and Claude integration.

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export METABASE_URL=https://metabase.example.com
export METABASE_API_KEY=mb_xxx...

# Run the API server
uvicorn app.main:app --reload

# Test the API
curl http://localhost:8000/api/sellers
```

---

## Project Structure

```
mb_onboarding/
├── ARCHITECTURE.md              # This document
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables (not committed)
├── test_pivot.py                # Test script for pivot tables
│
├── app/
│   ├── __init__.py
│   ├── main.py                  # FastAPI application entry point
│   ├── config.py                # Configuration (Metabase URL, card IDs)
│   │
│   ├── metabase/
│   │   ├── __init__.py
│   │   └── client.py            # Metabase API client
│   │
│   ├── data/
│   │   ├── __init__.py
│   │   ├── processor.py         # Data gap detection, coverage analysis
│   │   ├── metrics_engine.py    # Core metrics aggregation engine
│   │   └── pivot.py             # Pivot table builder
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py            # All API endpoints
│   │
│   └── claude/
│       ├── __init__.py
│       ├── tools.py             # Claude tool definitions & schemas
│       ├── executor.py          # Tool execution against API
│       └── mcp_server.py        # MCP server for Claude Code
│
├── sql/
│   └── README.md                # SQL reference for Metabase cards
│
└── metabase_cards_sql.md        # SQL queries used in Metabase cards
```

---

## Data Sources (Metabase Cards)

| Card ID | Name | Purpose |
|---------|------|---------|
| 665 | Ads Report | Sponsored products advertising data |
| 666 | ASIN Mapping | Child ASIN → Parent ASIN → Product mappings |
| 681 | Business Report | Sales, sessions, conversions |

### Data Hierarchy

```
Seller (e.g., "AttakPik", "Vandel")
  └── Parent ASIN / Normalized Name (product grouping)
       └── Child ASIN / Variant Name (individual SKU)
```

---

## Core Components

### MetricsEngine (`app/data/metrics_engine.py`)

The heart of the analytics system. Handles:
- ASIN hierarchy management
- Flexible metric aggregation at child/parent/account levels
- Time range filtering (date ranges, specific weeks/months)
- Period-over-period comparisons (WoW, MoM)
- Year-over-Year comparisons

**Key Methods:**
```python
engine = MetricsEngine(asin_df, business_df, ads_df)

# Get metrics at different aggregation levels
engine.get_metrics(
    seller_id=None,
    asin_selection=ASINSelection(parent_asins=["Widget Pro"]),
    time_range=TimeRange(start_date=date(2025, 1, 1)),
    aggregation_level="parent",  # child | parent | account | custom
    granularity="weekly",        # weekly | monthly
    include_comparison=True      # adds WoW/MoM change columns
)

# Get cumulative totals
engine.get_cumulative_metrics(...)

# Get YoY comparison
engine.get_yoy_comparison(seller_id=None, month=date(2025, 1, 1))

# Get ASIN hierarchy
engine.get_asin_hierarchy()
```

### PivotBuilder (`app/data/pivot.py`)

Transforms time-series metrics into spreadsheet-style pivot tables.

**Features:**
- Date-labeled columns (e.g., `Jan_11_total_sales`, `Jan_04_cvr_pct`)
- Metric presets for common use cases
- TOTAL row aggregation
- CSV export

**Metric Presets:**
| Preset | Metrics |
|--------|---------|
| `sales_overview` | total_sales, sessions, units, cvr_pct |
| `advertising` | ad_spend, ad_sales, roas, acos_pct, impressions, clicks, ctr_pct |
| `conversion` | sessions, page_views, total_order_items, cvr_pct, unit_session_pct |
| `traffic` | sessions, page_views, page_views_per_session |
| `all` | All available metrics |

---

## API Endpoints

Base URL: `http://localhost:8000/api`

### Sellers

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sellers` | GET | List all sellers |
| `/seller/{name}/asins` | GET | Get ASIN hierarchy for seller |

### Metrics

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/seller/{name}/metrics` | POST | Get detailed metrics (filterable) |
| `/seller/{name}/cumulative` | POST | Get aggregated totals |
| `/seller/{name}/pivot` | POST | Get pivot table with date columns |
| `/seller/{name}/yoy` | POST | Year-over-Year comparison |

### Data Quality

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/seller/{name}/coverage` | GET | Data coverage summary |
| `/seller/{name}/gaps` | GET | Missing data periods |

### Export

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/seller/{name}/export/csv` | POST | Download pivot as CSV |

### Claude Integration

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/claude/tools` | GET | Get Claude tool definitions |
| `/claude/execute` | POST | Execute a Claude tool call |

### System

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/filters` | GET | Available filter options |
| `/health` | GET | Health check |

---

## Request/Response Examples

### Get Pivot Table

```bash
curl -X POST http://localhost:8000/api/seller/AttakPik/pivot \
  -H "Content-Type: application/json" \
  -d '{
    "aggregation_level": "parent",
    "granularity": "weekly",
    "metric_preset": "sales_overview",
    "include_totals": true
  }'
```

**Response:**
```json
{
  "seller_name": "AttakPik",
  "aggregation_level": "parent",
  "granularity": "weekly",
  "periods": ["2026-01-11", "2026-01-04", "2025-12-28"],
  "metrics": ["total_sales", "sessions", "units", "cvr_pct"],
  "columns": ["seller_id", "adjusted_normalized_name", "Jan_11_total_sales", ...],
  "data": [...],
  "count": 7
}
```

### Get YoY Comparison

```bash
curl -X POST http://localhost:8000/api/seller/AttakPik/yoy \
  -H "Content-Type: application/json" \
  -d '{
    "month": "2025-12-01",
    "aggregation_level": "account"
  }'
```

**Response:**
```json
{
  "seller_name": "AttakPik",
  "current_month": "2025-12-01",
  "prior_year_month": "2024-12-01",
  "aggregation_level": "account",
  "columns": ["total_sales_current", "total_sales_prior", "total_sales_yoy_change", "total_sales_yoy_pct", ...],
  "data": [...],
  "count": 1
}
```

---

## Claude Integration

### Available Tools

| Tool | Description |
|------|-------------|
| `list_sellers` | List all Amazon sellers |
| `get_seller_asins` | Get ASIN hierarchy |
| `get_metrics` | Get detailed metrics with filtering |
| `get_cumulative_metrics` | Get aggregated totals |
| `get_pivot_table` | Get date-column pivot tables |
| `get_yoy_comparison` | Year-over-Year comparison |
| `get_data_coverage` | Check data availability |
| `get_data_gaps` | Detect missing periods |
| `get_filter_options` | Get available filters |

### Using with Claude Code (MCP)

Add to `.claude/config.json`:

```json
{
  "mcpServers": {
    "amazon-analytics": {
      "command": "python",
      "args": ["-m", "app.claude.mcp_server"],
      "cwd": "/path/to/mb_onboarding"
    }
  }
}
```

### Direct API Usage

```bash
# Get tool definitions
curl http://localhost:8000/api/claude/tools

# Execute a tool
curl -X POST http://localhost:8000/api/claude/execute \
  -H "Content-Type: application/json" \
  -d '{
    "tool_name": "list_sellers",
    "parameters": {}
  }'
```

---

## Metrics Reference

### Sales & Traffic (from Business Report)

| Metric | Column | Description |
|--------|--------|-------------|
| Total Sales | `total_sales` | Revenue from all orders |
| Total Units | `total_units` | Units ordered |
| Sessions | `sessions` | Unique visits |
| Page Views | `page_views` | Total page views |
| Conversion Rate | `cvr_pct` | Units / Sessions × 100 |
| Unit Session % | `unit_session_pct` | Units per session percentage |
| Average Price | `avg_price` | Total Sales / Total Units |

### Advertising (from Ads Report)

| Metric | Column | Description |
|--------|--------|-------------|
| Ad Spend | `ad_spend` | Advertising cost |
| Ad Sales | `ad_sales` | Revenue attributed to ads |
| ROAS | `roas` | Ad Sales / Ad Spend |
| ACOS % | `acos_pct` | Ad Spend / Ad Sales × 100 |
| Impressions | `impressions` | Ad views |
| Clicks | `clicks` | Ad clicks |
| CTR % | `ctr_pct` | Clicks / Impressions × 100 |
| CPC | `cpc` | Ad Spend / Clicks |

### Combined/Derived

| Metric | Column | Description |
|--------|--------|-------------|
| Organic Sales | `organic_sales` | Total Sales - Ad Sales |
| TACoS % | `tacos_pct` | Ad Spend / Total Sales × 100 |
| Ad Sales % | `ad_sales_pct` | Ad Sales / Total Sales × 100 |

---

## Configuration

### Environment Variables

```bash
# Required
METABASE_URL=https://metabase.example.com
METABASE_API_KEY=mb_xxx...

# Card IDs (configured in app/config.py)
CARD_ID_ADS_REPORT=665
CARD_ID_ASIN_MAPPING=666
CARD_ID_BUSINESS_REPORT=681
```

### Settings (`app/config.py`)

```python
class Settings(BaseSettings):
    metabase_url: str
    metabase_api_key: str
    card_id_ads_report: int = 665
    card_id_asin_mapping: int = 666
    card_id_business_report: int = 681
```

---

## Development

### Running Tests

```bash
# Run pivot table tests
python test_pivot.py

# Run with pytest (if tests exist)
pytest tests/
```

### Adding New Metrics

1. Add calculation in `MetricsEngine._calculate_metrics()`
2. Add to relevant preset in `PivotBuilder.METRIC_PRESETS`
3. Update tool descriptions in `app/claude/tools.py`

### Adding New Endpoints

1. Add route in `app/api/routes.py`
2. Add Pydantic request model if needed
3. Add corresponding Claude tool in `app/claude/tools.py`
4. Add executor function in `app/claude/executor.py`

---

## Troubleshooting

### Common Issues

**"Invalid values provided for operator: :string/="**
- Metabase dimension filters require value as a list: `"value": [seller_name]`

**"Out of range float values are not JSON compliant"**
- NaN/inf values in DataFrame - use `clean_record()` function to replace with None

**Empty data returned**
- Check seller_name spelling (case-sensitive)
- Verify date range has data
- Check Metabase card filters

### Debugging

```python
# Enable detailed error traces
import traceback
try:
    result = engine.get_metrics(...)
except Exception as e:
    print(traceback.format_exc())
```

---

## Future Enhancements

- [ ] Caching layer for Metabase responses
- [ ] Background data refresh
- [ ] WebSocket for real-time updates
- [ ] Excel export with formatting
- [ ] Email report scheduling

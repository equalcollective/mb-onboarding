"""Claude tool definitions for the Amazon Seller Analytics API.

This module defines the tools and schemas that Claude can use to interact
with the seller analytics endpoints.
"""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field
from datetime import date


# =============================================================================
# Tool Parameter Schemas
# =============================================================================

class ListSellersParams(BaseModel):
    """No parameters required - lists all available sellers."""
    pass


class GetSellerASINsParams(BaseModel):
    """Parameters for getting seller ASIN hierarchy."""
    seller_name: str = Field(..., description="Name of the seller (e.g., 'AttakPik')")


class GetMetricsParams(BaseModel):
    """Parameters for getting detailed metrics data."""
    seller_name: str = Field(..., description="Name of the seller")
    parent_asins: Optional[List[str]] = Field(
        None,
        description="List of parent/product names to include (auto-expands to all child ASINs)"
    )
    child_asins: Optional[List[str]] = Field(
        None,
        description="List of specific child ASINs to include"
    )
    start_date: Optional[str] = Field(
        None,
        description="Start date in YYYY-MM-DD format"
    )
    end_date: Optional[str] = Field(
        None,
        description="End date in YYYY-MM-DD format"
    )
    aggregation_level: Literal["child", "parent", "account", "custom"] = Field(
        "account",
        description="How to group the data: 'child' (by ASIN), 'parent' (by product), 'account' (total), or 'custom' (single aggregated row)"
    )
    granularity: Literal["weekly", "monthly"] = Field(
        "weekly",
        description="Time granularity for the data"
    )
    include_comparison: bool = Field(
        False,
        description="Include week-over-week or month-over-month change columns"
    )


class GetCumulativeMetricsParams(BaseModel):
    """Parameters for getting cumulative (total) metrics."""
    seller_name: str = Field(..., description="Name of the seller")
    parent_asins: Optional[List[str]] = Field(None, description="Parent/product names to filter")
    child_asins: Optional[List[str]] = Field(None, description="Specific child ASINs to filter")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    aggregation_level: Literal["child", "parent", "account", "custom"] = Field("account")
    granularity: Literal["weekly", "monthly"] = Field("weekly")


class GetPivotTableParams(BaseModel):
    """Parameters for getting pivot table with date-labeled columns."""
    seller_name: str = Field(..., description="Name of the seller")
    parent_asins: Optional[List[str]] = Field(None, description="Parent/product names to filter")
    child_asins: Optional[List[str]] = Field(None, description="Specific child ASINs to filter")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    aggregation_level: Literal["child", "parent", "account"] = Field(
        "parent",
        description="Row grouping level"
    )
    granularity: Literal["weekly", "monthly"] = Field("weekly")
    metric_preset: Optional[str] = Field(
        None,
        description="Preset metric group: 'sales_overview', 'advertising', 'conversion', 'traffic', or 'all'"
    )
    include_totals: bool = Field(True, description="Include a TOTAL row at the bottom")


class GetYoYComparisonParams(BaseModel):
    """Parameters for Year-over-Year comparison."""
    seller_name: str = Field(..., description="Name of the seller")
    month: str = Field(
        ...,
        description="Month to compare (first day, e.g., '2025-01-01' for January 2025)"
    )
    parent_asins: Optional[List[str]] = Field(None, description="Parent/product names to filter")
    child_asins: Optional[List[str]] = Field(None, description="Specific child ASINs to filter")
    aggregation_level: Literal["child", "parent", "account"] = Field("account")


class GetDataCoverageParams(BaseModel):
    """Parameters for getting data coverage summary."""
    seller_name: str = Field(..., description="Name of the seller")


class GetDataGapsParams(BaseModel):
    """Parameters for detecting data gaps."""
    seller_name: str = Field(..., description="Name of the seller")
    granularity: Literal["weekly", "monthly"] = Field("weekly")


# =============================================================================
# Tool Definitions (Claude Tool-Use Format)
# =============================================================================

CLAUDE_TOOLS = [
    {
        "name": "list_sellers",
        "description": """List all available Amazon sellers in the system.

Returns seller information including:
- seller_id: Unique identifier
- seller_name: Display name (use this for other API calls)
- marketplace: Amazon marketplace (e.g., US, UK)
- asin_count: Number of child ASINs
- parent_count: Number of parent ASINs
- product_count: Number of unique products

Use this first to discover which sellers are available for analysis.""",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_seller_asins",
        "description": """Get the ASIN hierarchy for a seller.

Returns a list of parent products with their child ASINs:
- parent_name: Product/parent name
- child_count: Number of child variants
- children: List of {child_asin, variant_name, title}

Use this to understand a seller's product catalog before querying metrics.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller (e.g., 'AttakPik')"
                }
            },
            "required": ["seller_name"]
        }
    },
    {
        "name": "get_metrics",
        "description": """Get detailed metrics data with flexible filtering and aggregation.

Returns time-series data with metrics including:
- Sales: total_sales, total_units, avg_price
- Traffic: sessions, page_views, page_views_per_session
- Conversion: total_order_items, cvr_pct (conversion rate), unit_session_pct
- Advertising: ad_spend, ad_sales, roas, acos_pct, impressions, clicks, ctr_pct, cpc

Use aggregation_level to control grouping:
- 'account': Single row per period for entire seller
- 'parent': Group by product (parent ASIN)
- 'child': Group by individual ASIN
- 'custom': Single aggregated row across all selected data

Set include_comparison=true to get WoW/MoM change columns.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller"
                },
                "parent_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Parent/product names to include (expands to all child ASINs)"
                },
                "child_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific child ASINs to include"
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)"
                },
                "aggregation_level": {
                    "type": "string",
                    "enum": ["child", "parent", "account", "custom"],
                    "description": "Data grouping level",
                    "default": "account"
                },
                "granularity": {
                    "type": "string",
                    "enum": ["weekly", "monthly"],
                    "description": "Time period granularity",
                    "default": "weekly"
                },
                "include_comparison": {
                    "type": "boolean",
                    "description": "Include period-over-period change columns",
                    "default": False
                }
            },
            "required": ["seller_name"]
        }
    },
    {
        "name": "get_cumulative_metrics",
        "description": """Get cumulative (total) metrics for selected periods.

Returns a single aggregated record with totals across all selected time periods.
Useful for getting overall performance summaries.

All metrics are summed/averaged appropriately:
- Sums: total_sales, total_units, sessions, ad_spend, impressions, clicks
- Averages: cvr_pct, roas, acos_pct, ctr_pct, avg_price""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller"
                },
                "parent_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Parent/product names to filter"
                },
                "child_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific child ASINs to filter"
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)"
                },
                "aggregation_level": {
                    "type": "string",
                    "enum": ["child", "parent", "account", "custom"],
                    "default": "account"
                },
                "granularity": {
                    "type": "string",
                    "enum": ["weekly", "monthly"],
                    "default": "weekly"
                }
            },
            "required": ["seller_name"]
        }
    },
    {
        "name": "get_pivot_table",
        "description": """Get a pivot table with date-labeled columns.

Returns data formatted for analysis and reporting:
- Rows: ASINs or products (based on aggregation_level)
- Columns: Date_Metric format (e.g., 'Jan_11_total_sales', 'Jan_04_cvr_pct')

Metric presets:
- 'sales_overview': total_sales, total_units, avg_price, cvr_pct
- 'advertising': ad_spend, ad_sales, roas, acos_pct, impressions, clicks, ctr_pct
- 'conversion': sessions, page_views, total_order_items, cvr_pct, unit_session_pct
- 'traffic': sessions, page_views, page_views_per_session
- 'all': All available metrics

Includes TOTAL row by default.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller"
                },
                "parent_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Parent/product names to filter"
                },
                "child_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific child ASINs to filter"
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)"
                },
                "aggregation_level": {
                    "type": "string",
                    "enum": ["child", "parent", "account"],
                    "description": "Row grouping level",
                    "default": "parent"
                },
                "granularity": {
                    "type": "string",
                    "enum": ["weekly", "monthly"],
                    "default": "weekly"
                },
                "metric_preset": {
                    "type": "string",
                    "enum": ["sales_overview", "advertising", "conversion", "traffic", "all"],
                    "description": "Preset group of metrics to include"
                },
                "include_totals": {
                    "type": "boolean",
                    "description": "Include a TOTAL row",
                    "default": True
                }
            },
            "required": ["seller_name"]
        }
    },
    {
        "name": "get_yoy_comparison",
        "description": """Get Year-over-Year comparison for a specific month.

Compares the requested month with the same month from the previous year.
For example, January 2025 vs January 2024.

Returns columns for each metric:
- {metric}_current: Current period value
- {metric}_prior: Same month last year
- {metric}_yoy_change: Absolute difference
- {metric}_yoy_pct: Percentage change

Useful for understanding seasonal trends and year-over-year growth.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller"
                },
                "month": {
                    "type": "string",
                    "description": "Month to compare (first day, e.g., '2025-01-01')"
                },
                "parent_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Parent/product names to filter"
                },
                "child_asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific child ASINs to filter"
                },
                "aggregation_level": {
                    "type": "string",
                    "enum": ["child", "parent", "account"],
                    "default": "account"
                }
            },
            "required": ["seller_name", "month"]
        }
    },
    {
        "name": "get_data_coverage",
        "description": """Get data coverage summary for a seller.

Returns information about available data:
- Date ranges for business report and advertising data
- Total rows and unique ASINs in each dataset
- Any overlaps or gaps in coverage

Use this to understand what data is available before querying metrics.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller"
                }
            },
            "required": ["seller_name"]
        }
    },
    {
        "name": "get_data_gaps",
        "description": """Detect data gaps for a seller.

Returns periods where data is missing or incomplete:
- Missing weeks/months in business report
- Missing weeks/months in advertising data
- Partial data periods

Use this to identify data quality issues before analysis.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "seller_name": {
                    "type": "string",
                    "description": "Name of the seller"
                },
                "granularity": {
                    "type": "string",
                    "enum": ["weekly", "monthly"],
                    "default": "weekly"
                }
            },
            "required": ["seller_name"]
        }
    },
    {
        "name": "get_filter_options",
        "description": """Get available filter options.

Returns all available:
- Aggregation levels (child, parent, account)
- Granularities (weekly, monthly)
- Metric presets (sales_overview, advertising, etc.)
- Individual metrics

Use this to understand available filtering options.""",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]


# =============================================================================
# System Prompt for Claude
# =============================================================================

SYSTEM_PROMPT = """You are an Amazon Seller Analytics assistant with access to real-time seller performance data.

## Available Data
You can analyze data for Amazon sellers including:
- **Sales Metrics**: Total sales, units sold, average price, revenue trends
- **Traffic Metrics**: Sessions, page views, page views per session
- **Conversion Metrics**: Conversion rate (CVR), unit session percentage, order items
- **Advertising Metrics**: Ad spend, ad sales, ROAS, ACOS, impressions, clicks, CTR, CPC

## Analysis Workflow
1. **Start by listing sellers** to see what accounts are available
2. **Get data coverage** to understand the date range and data availability
3. **Explore the ASIN hierarchy** to understand the product catalog
4. **Query metrics** with appropriate filters and aggregation

## Aggregation Levels
- **account**: High-level seller performance (one row per period)
- **parent**: Performance by product/parent ASIN
- **child**: Granular performance by individual ASIN variant

## Time Granularity
- **weekly**: Week-level data (periods start on Monday)
- **monthly**: Month-level data (periods are calendar months)

## Tips for Analysis
- Use `get_pivot_table` for spreadsheet-style reports with date columns
- Use `get_metrics` with `include_comparison=true` for trend analysis
- Use `get_yoy_comparison` for year-over-year seasonal analysis
- Use `get_cumulative_metrics` for total performance summaries

## Metric Presets
When querying pivot tables, use presets to get relevant metric groups:
- `sales_overview`: Sales, units, price, conversion
- `advertising`: All ad metrics (spend, sales, ROAS, ACOS, etc.)
- `conversion`: Traffic to sales conversion metrics
- `traffic`: Session and page view metrics
- `all`: Every available metric

Always explain your findings in business terms and provide actionable insights."""


# =============================================================================
# Helper Functions
# =============================================================================

def get_tool_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Get a tool definition by name."""
    for tool in CLAUDE_TOOLS:
        if tool["name"] == name:
            return tool
    return None


def get_all_tool_names() -> List[str]:
    """Get list of all tool names."""
    return [tool["name"] for tool in CLAUDE_TOOLS]


def validate_tool_params(tool_name: str, params: Dict[str, Any]) -> bool:
    """Validate parameters against tool schema."""
    tool = get_tool_by_name(tool_name)
    if not tool:
        return False

    schema = tool["input_schema"]
    required = schema.get("required", [])

    # Check all required params are present
    for req in required:
        if req not in params:
            return False

    return True

"""Tool executor for Claude integration.

This module executes Claude tool calls against the analytics API.
"""

from typing import Any, Dict, Optional
from datetime import date

from ..api.routes import (
    get_engine,
    get_all_sellers,
    _pivot_builder,
)
from ..data.metrics_engine import ASINSelection, TimeRange
from ..data.processor import DataProcessor
import pandas as pd
import numpy as np


def _clean_dataframe_for_json(df: pd.DataFrame) -> list:
    """Clean DataFrame for JSON serialization."""
    if df.empty:
        return []

    # Replace inf/nan with None
    df = df.replace([float('inf'), float('-inf')], None)
    df = df.where(pd.notna(df), None)

    # Convert dates
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: x.isoformat() if hasattr(x, "isoformat") else x
            )

    records = df.to_dict("records")

    # Final cleanup for any remaining nan/inf
    def clean_record(record):
        cleaned = {}
        for k, v in record.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                cleaned[k] = None
            else:
                cleaned[k] = v
        return cleaned

    return [clean_record(r) for r in records]


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse date string to date object."""
    if not date_str:
        return None
    return date.fromisoformat(date_str)


def execute_tool(tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a Claude tool call and return the result.

    Args:
        tool_name: Name of the tool to execute
        params: Tool parameters

    Returns:
        Tool execution result as a dictionary
    """
    try:
        if tool_name == "list_sellers":
            return _list_sellers()

        elif tool_name == "get_seller_asins":
            return _get_seller_asins(params["seller_name"])

        elif tool_name == "get_metrics":
            return _get_metrics(params)

        elif tool_name == "get_cumulative_metrics":
            return _get_cumulative_metrics(params)

        elif tool_name == "get_pivot_table":
            return _get_pivot_table(params)

        elif tool_name == "get_yoy_comparison":
            return _get_yoy_comparison(params)

        elif tool_name == "get_data_coverage":
            return _get_data_coverage(params["seller_name"])

        elif tool_name == "get_data_gaps":
            return _get_data_gaps(params)

        elif tool_name == "get_filter_options":
            return _get_filter_options()

        else:
            return {"error": f"Unknown tool: {tool_name}"}

    except Exception as e:
        return {"error": str(e)}


def _list_sellers() -> Dict[str, Any]:
    """List all available sellers."""
    df = get_all_sellers()
    if df.empty:
        return {"sellers": [], "count": 0}

    sellers = [
        {
            "seller_id": int(row["seller_id"]),
            "seller_name": row["seller_name"],
            "marketplace": row.get("marketplace"),
            "asin_count": int(row.get("asin_count", 0)),
            "parent_count": int(row.get("parent_count", 0)),
            "product_count": int(row.get("product_count", 0))
        }
        for _, row in df.iterrows()
    ]

    return {"sellers": sellers, "count": len(sellers)}


def _get_seller_asins(seller_name: str) -> Dict[str, Any]:
    """Get ASIN hierarchy for a seller."""
    engine = get_engine(seller_name)
    hierarchy = engine.get_asin_hierarchy()

    result = []
    for parent_name, data in hierarchy.items():
        children = [
            {
                "child_asin": c.get("child_asin", ""),
                "variant_name": c.get("adjusted_variant_name"),
                "title": c.get("title")
            }
            for c in data["children"]
        ]
        result.append({
            "parent_name": parent_name,
            "child_count": data["child_count"],
            "children": children
        })

    result.sort(key=lambda x: x["parent_name"])
    return {"asins": result, "count": len(result)}


def _get_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Get metrics with filtering and aggregation."""
    seller_name = params["seller_name"]
    engine = get_engine(seller_name)

    # Build ASIN selection
    asin_selection = None
    if params.get("parent_asins") or params.get("child_asins"):
        asin_selection = ASINSelection(
            parent_asins=params.get("parent_asins", []),
            child_asins=params.get("child_asins", [])
        )

    # Build time range
    time_range = None
    start_date = _parse_date(params.get("start_date"))
    end_date = _parse_date(params.get("end_date"))
    if start_date or end_date:
        time_range = TimeRange(start_date=start_date, end_date=end_date)

    # Get metrics
    result = engine.get_metrics(
        seller_id=None,
        asin_selection=asin_selection,
        time_range=time_range,
        aggregation_level=params.get("aggregation_level", "account"),
        granularity=params.get("granularity", "weekly"),
        include_comparison=params.get("include_comparison", False)
    )

    data = _clean_dataframe_for_json(result)

    return {
        "seller_name": seller_name,
        "aggregation_level": params.get("aggregation_level", "account"),
        "granularity": params.get("granularity", "weekly"),
        "data": data,
        "count": len(data),
        "columns": list(result.columns) if not result.empty else []
    }


def _get_cumulative_metrics(params: Dict[str, Any]) -> Dict[str, Any]:
    """Get cumulative metrics."""
    seller_name = params["seller_name"]
    engine = get_engine(seller_name)

    # Build ASIN selection
    asin_selection = None
    if params.get("parent_asins") or params.get("child_asins"):
        asin_selection = ASINSelection(
            parent_asins=params.get("parent_asins", []),
            child_asins=params.get("child_asins", [])
        )

    # Build time range
    time_range = None
    start_date = _parse_date(params.get("start_date"))
    end_date = _parse_date(params.get("end_date"))
    if start_date or end_date:
        time_range = TimeRange(start_date=start_date, end_date=end_date)

    # Get cumulative metrics
    result = engine.get_cumulative_metrics(
        seller_id=None,
        asin_selection=asin_selection,
        time_range=time_range,
        aggregation_level=params.get("aggregation_level", "account"),
        granularity=params.get("granularity", "weekly")
    )

    if result.empty:
        return {"data": None}

    # Convert to single record
    data = _clean_dataframe_for_json(result)

    return {
        "seller_name": seller_name,
        "aggregation_level": params.get("aggregation_level", "account"),
        "granularity": params.get("granularity", "weekly"),
        "data": data[0] if data else None
    }


def _get_pivot_table(params: Dict[str, Any]) -> Dict[str, Any]:
    """Get pivot table with date-labeled columns."""
    seller_name = params["seller_name"]
    engine = get_engine(seller_name)

    # Build ASIN selection
    asin_selection = None
    if params.get("parent_asins") or params.get("child_asins"):
        asin_selection = ASINSelection(
            parent_asins=params.get("parent_asins", []),
            child_asins=params.get("child_asins", [])
        )

    # Build time range
    time_range = None
    start_date = _parse_date(params.get("start_date"))
    end_date = _parse_date(params.get("end_date"))
    if start_date or end_date:
        time_range = TimeRange(start_date=start_date, end_date=end_date)

    # Get metrics
    metrics_df = engine.get_metrics(
        seller_id=None,
        asin_selection=asin_selection,
        time_range=time_range,
        aggregation_level=params.get("aggregation_level", "parent"),
        granularity=params.get("granularity", "weekly"),
        include_comparison=False
    )

    if metrics_df.empty:
        return {"data": [], "columns": [], "count": 0}

    # Drop redundant column
    if 'period_start' in metrics_df.columns:
        metrics_df = metrics_df.drop(columns=['period_start'])

    # Determine metrics
    metrics_list = None
    metric_preset = params.get("metric_preset")
    if metric_preset and metric_preset in _pivot_builder.METRIC_PRESETS:
        metrics_list = _pivot_builder.METRIC_PRESETS[metric_preset]

    # Build pivot
    pivot_df = _pivot_builder.build_pivot(
        metrics_df,
        level=params.get("aggregation_level", "parent"),
        granularity=params.get("granularity", "weekly"),
        metrics=metrics_list,
        include_totals=params.get("include_totals", True)
    )

    saved_periods = pivot_df.attrs.get("periods", [])
    saved_metrics = pivot_df.attrs.get("metrics", [])

    data = _clean_dataframe_for_json(pivot_df)
    periods = [p.isoformat() if hasattr(p, "isoformat") else p for p in saved_periods]

    return {
        "seller_name": seller_name,
        "aggregation_level": params.get("aggregation_level", "parent"),
        "granularity": params.get("granularity", "weekly"),
        "periods": periods,
        "metrics": saved_metrics,
        "columns": list(pivot_df.columns),
        "data": data,
        "count": len(data)
    }


def _get_yoy_comparison(params: Dict[str, Any]) -> Dict[str, Any]:
    """Get Year-over-Year comparison."""
    seller_name = params["seller_name"]
    engine = get_engine(seller_name)

    month = _parse_date(params["month"])
    if not month:
        return {"error": "Invalid month format"}

    # Build ASIN selection
    asin_selection = None
    if params.get("parent_asins") or params.get("child_asins"):
        asin_selection = ASINSelection(
            parent_asins=params.get("parent_asins", []),
            child_asins=params.get("child_asins", [])
        )

    result = engine.get_yoy_comparison(
        seller_id=None,
        month=month,
        asin_selection=asin_selection,
        aggregation_level=params.get("aggregation_level", "account")
    )

    data = _clean_dataframe_for_json(result)

    return {
        "seller_name": seller_name,
        "current_month": month.isoformat(),
        "prior_year_month": date(month.year - 1, month.month, 1).isoformat(),
        "aggregation_level": params.get("aggregation_level", "account"),
        "columns": list(result.columns) if not result.empty else [],
        "data": data,
        "count": len(data)
    }


def _get_data_coverage(seller_name: str) -> Dict[str, Any]:
    """Get data coverage summary."""
    engine = get_engine(seller_name)

    processor = DataProcessor()
    coverage = processor.get_data_coverage_summary(
        engine.business_df,
        engine.ads_df
    )

    if coverage.empty:
        return {"coverage": None}

    data = _clean_dataframe_for_json(coverage)

    return {
        "seller_name": seller_name,
        "coverage": data[0] if data else None
    }


def _get_data_gaps(params: Dict[str, Any]) -> Dict[str, Any]:
    """Get data gaps for a seller."""
    seller_name = params["seller_name"]
    engine = get_engine(seller_name)

    processor = DataProcessor()
    gaps = processor.detect_data_gaps(
        engine.business_df,
        engine.ads_df,
        granularity=params.get("granularity", "weekly")
    )

    data = _clean_dataframe_for_json(gaps)

    return {
        "seller_name": seller_name,
        "granularity": params.get("granularity", "weekly"),
        "gaps": data,
        "count": len(data)
    }


def _get_filter_options() -> Dict[str, Any]:
    """Get available filter options."""
    return _pivot_builder.get_available_filters()

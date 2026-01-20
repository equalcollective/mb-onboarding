"""API routes for the data pipeline."""

from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from typing import Any, Dict, List, Literal, Optional
from datetime import date
from pydantic import BaseModel
import pandas as pd
from io import StringIO

from ..config import get_settings
from ..metabase.client import MetabaseClient
from ..data.metrics_engine import MetricsEngine, ASINSelection, TimeRange
from ..data.pivot import PivotBuilder


router = APIRouter(prefix="/api", tags=["data"])
_pivot_builder = PivotBuilder()


# ============================================================================
# Request/Response Models
# ============================================================================

class SellerInfo(BaseModel):
    seller_id: int
    seller_name: str
    marketplace: Optional[str] = None
    asin_count: int = 0
    parent_count: int = 0
    product_count: int = 0


class ASINChild(BaseModel):
    child_asin: str
    variant_name: Optional[str] = None
    title: Optional[str] = None


class ASINParent(BaseModel):
    parent_name: str
    child_count: int
    children: List[ASINChild]


class MetricsRequest(BaseModel):
    """Request body for metrics endpoint."""
    parent_asins: Optional[List[str]] = None
    child_asins: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    specific_weeks: Optional[List[date]] = None
    specific_months: Optional[List[date]] = None
    aggregation_level: Literal["child", "parent", "account", "custom"] = "account"
    granularity: Literal["weekly", "monthly"] = "weekly"
    include_comparison: bool = False


class CumulativeRequest(BaseModel):
    """Request body for cumulative metrics endpoint."""
    parent_asins: Optional[List[str]] = None
    child_asins: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    specific_weeks: Optional[List[date]] = None
    specific_months: Optional[List[date]] = None
    aggregation_level: Literal["child", "parent", "account", "custom"] = "account"
    granularity: Literal["weekly", "monthly"] = "weekly"


class PivotRequest(BaseModel):
    """Request body for pivot table endpoint."""
    parent_asins: Optional[List[str]] = None
    child_asins: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    aggregation_level: Literal["child", "parent", "account"] = "parent"
    granularity: Literal["weekly", "monthly"] = "weekly"
    metrics: Optional[List[str]] = None  # None = all metrics
    metric_preset: Optional[str] = None  # e.g., "sales_overview", "advertising"
    include_totals: bool = True
    period_order: Literal["recent_first", "oldest_first"] = "recent_first"


class CSVExportRequest(BaseModel):
    """Request body for CSV export endpoint."""
    parent_asins: Optional[List[str]] = None
    child_asins: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    aggregation_level: Literal["child", "parent", "account"] = "parent"
    granularity: Literal["weekly", "monthly"] = "weekly"
    metrics: Optional[List[str]] = None
    metric_preset: Optional[str] = None
    include_totals: bool = True
    filename: Optional[str] = None  # Custom filename


class YoYRequest(BaseModel):
    """Request body for Year-over-Year comparison endpoint."""
    month: date  # First day of month to compare (e.g., 2025-01-01)
    parent_asins: Optional[List[str]] = None
    child_asins: Optional[List[str]] = None
    aggregation_level: Literal["child", "parent", "account"] = "account"


# ============================================================================
# Data Loading
# ============================================================================

_engine: Optional[MetricsEngine] = None


def _fetch_card(card_id: int, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Fetch data from a Metabase card."""
    settings = get_settings()
    client = MetabaseClient(settings.metabase_url, settings.metabase_api_key)

    try:
        # client.fetch_card already returns a DataFrame
        return client.fetch_card(card_id, params)
    finally:
        client.close()


def get_engine(seller_name: str) -> MetricsEngine:
    """Get MetricsEngine loaded with seller data."""
    settings = get_settings()
    client = MetabaseClient(settings.metabase_url, settings.metabase_api_key)

    try:
        # Card 666 (ASIN mapping) uses dimension/field filter
        asin_params = [{
            "type": "string/=",
            "target": ["dimension", ["template-tag", "seller_name"]],
            "value": [seller_name]
        }]
        response = client._client.post(f"/api/card/{settings.card_id_asin_mapping}/query/json", json={"parameters": asin_params})
        response.raise_for_status()
        asin_df = pd.DataFrame(response.json()) if isinstance(response.json(), list) else pd.DataFrame()

        # Card 681 (business report) uses category/variable filter
        biz_params = [{
            "type": "category",
            "target": ["variable", ["template-tag", "seller_name"]],
            "value": seller_name
        }]
        response = client._client.post(f"/api/card/{settings.card_id_business_report}/query/json", json={"parameters": biz_params})
        response.raise_for_status()
        biz_df = pd.DataFrame(response.json()) if isinstance(response.json(), list) else pd.DataFrame()

        # Card 665 (ads report) uses category/variable filter
        ads_params = [{
            "type": "category",
            "target": ["variable", ["template-tag", "seller_name"]],
            "value": seller_name
        }]
        response = client._client.post(f"/api/card/{settings.card_id_ads_report}/query/json", json={"parameters": ads_params})
        response.raise_for_status()
        ads_df = pd.DataFrame(response.json()) if isinstance(response.json(), list) else pd.DataFrame()

        return MetricsEngine(asin_df, biz_df, ads_df)
    finally:
        client.close()


def get_all_sellers() -> pd.DataFrame:
    """Get all sellers from ASIN data."""
    settings = get_settings()
    # Fetch all ASIN data (no filter)
    asin_df = _fetch_card(settings.card_id_asin_mapping)

    if asin_df.empty:
        return pd.DataFrame()

    return asin_df.groupby(
        ["seller_id", "seller_name", "seller_marketplace"],
        as_index=False
    ).agg({
        "child_asin": "nunique",
        "adjusted_parent_asin": "nunique",
        "adjusted_normalized_name": "nunique"
    }).rename(columns={
        "child_asin": "asin_count",
        "adjusted_parent_asin": "parent_count",
        "adjusted_normalized_name": "product_count",
        "seller_marketplace": "marketplace"
    })


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/sellers", response_model=List[SellerInfo])
async def list_sellers():
    """List all available sellers."""
    try:
        df = get_all_sellers()
        if df.empty:
            return []

        return [
            SellerInfo(
                seller_id=int(row["seller_id"]),
                seller_name=row["seller_name"],
                marketplace=row.get("marketplace"),
                asin_count=int(row.get("asin_count", 0)),
                parent_count=int(row.get("parent_count", 0)),
                product_count=int(row.get("product_count", 0))
            )
            for _, row in df.iterrows()
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/seller/{seller_name}/asins", response_model=List[ASINParent])
async def get_seller_asins(seller_name: str):
    """Get ASIN hierarchy for a seller (for selection UI)."""
    try:
        engine = get_engine(seller_name)
        hierarchy = engine.get_asin_hierarchy()

        result = []
        for parent_name, data in hierarchy.items():
            children = [
                ASINChild(
                    child_asin=c.get("child_asin", ""),
                    variant_name=c.get("adjusted_variant_name"),
                    title=c.get("title")
                )
                for c in data["children"]
            ]
            result.append(ASINParent(
                parent_name=parent_name,
                child_count=data["child_count"],
                children=children
            ))

        # Sort by parent name
        result.sort(key=lambda x: x.parent_name)
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seller/{seller_name}/metrics")
async def get_metrics(seller_name: str, request: MetricsRequest = Body(...)):
    """Get metrics with flexible filtering and aggregation.

    Request body:
    - parent_asins: List of parent/product names to include (auto-expands to children)
    - child_asins: List of specific child ASINs to include
    - start_date/end_date: Date range filter
    - specific_weeks/specific_months: Specific periods to include
    - aggregation_level: child, parent, account, or custom (single aggregated row)
    - granularity: weekly or monthly
    - include_comparison: Include WoW/MoM change columns
    """
    try:
        engine = get_engine(seller_name)

        # Build ASIN selection
        asin_selection = None
        if request.parent_asins or request.child_asins:
            asin_selection = ASINSelection(
                parent_asins=request.parent_asins or [],
                child_asins=request.child_asins or []
            )

        # Build time range
        time_range = None
        if any([request.start_date, request.end_date,
                request.specific_weeks, request.specific_months]):
            time_range = TimeRange(
                start_date=request.start_date,
                end_date=request.end_date,
                specific_weeks=request.specific_weeks or [],
                specific_months=request.specific_months or []
            )

        # Get metrics
        result = engine.get_metrics(
            seller_id=None,  # Will be determined from data
            asin_selection=asin_selection,
            time_range=time_range,
            aggregation_level=request.aggregation_level,
            granularity=request.granularity,
            include_comparison=request.include_comparison
        )

        if result.empty:
            return {"data": [], "count": 0}

        # Replace NaN/inf values with None for JSON compatibility
        result = result.replace([float('inf'), float('-inf')], None)
        result = result.where(pd.notna(result), None)

        # Handle date serialization
        for col in result.columns:
            if result[col].dtype == "object":
                result[col] = result[col].apply(
                    lambda x: x.isoformat() if hasattr(x, "isoformat") else x
                )

        data = result.to_dict("records")

        return {
            "seller_name": seller_name,
            "aggregation_level": request.aggregation_level,
            "granularity": request.granularity,
            "data": data,
            "count": len(data),
            "columns": list(result.columns)
        }

    except Exception as e:
        import traceback
        error_detail = f"{str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.post("/seller/{seller_name}/cumulative")
async def get_cumulative_metrics(seller_name: str, request: CumulativeRequest = Body(...)):
    """Get cumulative (aggregated) metrics for selected periods.

    Returns a single row with totals across all selected periods and ASINs.
    """
    try:
        engine = get_engine(seller_name)

        # Build ASIN selection
        asin_selection = None
        if request.parent_asins or request.child_asins:
            asin_selection = ASINSelection(
                parent_asins=request.parent_asins or [],
                child_asins=request.child_asins or []
            )

        # Build time range
        time_range = None
        if any([request.start_date, request.end_date,
                request.specific_weeks, request.specific_months]):
            time_range = TimeRange(
                start_date=request.start_date,
                end_date=request.end_date,
                specific_weeks=request.specific_weeks or [],
                specific_months=request.specific_months or []
            )

        # Get cumulative metrics
        result = engine.get_cumulative_metrics(
            seller_id=None,
            asin_selection=asin_selection,
            time_range=time_range,
            aggregation_level=request.aggregation_level,
            granularity=request.granularity
        )

        if result.empty:
            return {"data": None}

        # Replace NaN/inf with None
        result = result.replace([float('inf'), float('-inf')], None)
        result = result.where(pd.notna(result), None)

        # Convert to single record
        record = result.iloc[0].to_dict()

        # Handle date serialization
        for key, value in record.items():
            if hasattr(value, "isoformat"):
                record[key] = value.isoformat()

        return {
            "seller_name": seller_name,
            "aggregation_level": request.aggregation_level,
            "granularity": request.granularity,
            "data": record
        }

    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"{str(e)}\n{traceback.format_exc()}")


@router.get("/seller/{seller_name}/gaps")
async def get_data_gaps(
    seller_name: str,
    granularity: Literal["weekly", "monthly"] = Query("weekly")
):
    """Get data gaps for a seller (missing periods in business or ads data)."""
    try:
        engine = get_engine(seller_name)

        # Use the processor's gap detection
        from ..data.processor import DataProcessor
        processor = DataProcessor()

        gaps = processor.detect_data_gaps(
            engine.business_df,
            engine.ads_df,
            granularity=granularity
        )

        if gaps.empty:
            return {"gaps": [], "count": 0}

        # Convert dates
        for col in ["period_start", "period_end"]:
            if col in gaps.columns:
                gaps[col] = gaps[col].apply(
                    lambda x: x.isoformat() if hasattr(x, "isoformat") else x
                )

        return {
            "seller_name": seller_name,
            "granularity": granularity,
            "gaps": gaps.to_dict("records"),
            "count": len(gaps)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/seller/{seller_name}/coverage")
async def get_data_coverage(seller_name: str):
    """Get data coverage summary for a seller."""
    try:
        engine = get_engine(seller_name)

        from ..data.processor import DataProcessor
        processor = DataProcessor()

        coverage = processor.get_data_coverage_summary(
            engine.business_df,
            engine.ads_df
        )

        if coverage.empty:
            return {"coverage": None}

        record = coverage.iloc[0].to_dict()

        # Handle date serialization
        for key, value in record.items():
            if hasattr(value, "isoformat"):
                record[key] = value.isoformat()

        return {
            "seller_name": seller_name,
            "coverage": record
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seller/{seller_name}/pivot")
async def get_pivot_table(seller_name: str, request: PivotRequest = Body(...)):
    """Get pivot table with date-labeled columns.

    Rows: ASINs (parent or child level)
    Columns: Date_Metric (e.g., Jan_11_total_sales, Jan_11_cvr_pct, Jan_04_total_sales, ...)

    Returns JSON with pivot data. Use /export/csv for CSV download.
    """
    try:
        engine = get_engine(seller_name)

        # Build ASIN selection
        asin_selection = None
        if request.parent_asins or request.child_asins:
            asin_selection = ASINSelection(
                parent_asins=request.parent_asins or [],
                child_asins=request.child_asins or []
            )

        # Build time range
        time_range = None
        if request.start_date or request.end_date:
            time_range = TimeRange(
                start_date=request.start_date,
                end_date=request.end_date
            )

        # Get metrics data
        metrics_df = engine.get_metrics(
            seller_id=None,
            asin_selection=asin_selection,
            time_range=time_range,
            aggregation_level=request.aggregation_level,
            granularity=request.granularity,
            include_comparison=False
        )

        if metrics_df.empty:
            return {"data": [], "columns": [], "count": 0}

        # Drop redundant period_start column (we use period_start_date)
        if 'period_start' in metrics_df.columns:
            metrics_df = metrics_df.drop(columns=['period_start'])

        # Determine which metrics to include
        metrics_list = request.metrics
        if request.metric_preset and request.metric_preset in _pivot_builder.METRIC_PRESETS:
            metrics_list = _pivot_builder.METRIC_PRESETS[request.metric_preset]

        # Build pivot table
        pivot_df = _pivot_builder.build_pivot(
            metrics_df,
            level=request.aggregation_level,
            granularity=request.granularity,
            metrics=metrics_list,
            include_totals=request.include_totals
        )

        # Reorder if needed
        if request.period_order == "oldest_first":
            pivot_df = _pivot_builder.reorder_columns(
                pivot_df,
                period_order="oldest_first"
            )

        # Save attrs before replacing NaN (which creates a copy)
        saved_periods = pivot_df.attrs.get("periods", [])
        saved_metrics = pivot_df.attrs.get("metrics", [])

        # Replace NaN/inf values for JSON compatibility
        # Handle all numeric columns
        numeric_cols = pivot_df.select_dtypes(include=['number']).columns
        pivot_df[numeric_cols] = pivot_df[numeric_cols].fillna(0)
        pivot_df[numeric_cols] = pivot_df[numeric_cols].replace([float('inf'), float('-inf')], 0)

        # Handle object columns (might have numpy NaN or need date conversion)
        import numpy as np
        object_cols = pivot_df.select_dtypes(include=['object']).columns
        for col in object_cols:
            def clean_value(x):
                # Handle NaN/None
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return None
                if pd.isna(x):
                    return None
                # Handle dates
                if hasattr(x, "isoformat"):
                    return x.isoformat()
                return x
            # Apply and keep as object dtype to preserve None values
            pivot_df[col] = pivot_df[col].apply(clean_value).astype(object)

        data = pivot_df.to_dict("records")

        # Final cleanup: replace any remaining nan/inf with None
        def clean_record(record):
            cleaned = {}
            for k, v in record.items():
                if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                    cleaned[k] = None
                else:
                    cleaned[k] = v
            return cleaned

        data = [clean_record(r) for r in data]
        periods = [p.isoformat() if hasattr(p, "isoformat") else p
                   for p in saved_periods]

        return {
            "seller_name": seller_name,
            "aggregation_level": request.aggregation_level,
            "granularity": request.granularity,
            "periods": periods,
            "metrics": saved_metrics,
            "columns": list(pivot_df.columns),
            "data": data,
            "count": len(data)
        }

    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"{str(e)}\n{traceback.format_exc()}")


@router.post("/seller/{seller_name}/export/csv")
async def export_csv(seller_name: str, request: CSVExportRequest = Body(...)):
    """Export pivot table as CSV download.

    All rows fully populated (no empty grouping cells) - usable for VLOOKUP, pivot tables, etc.
    """
    try:
        engine = get_engine(seller_name)

        # Build ASIN selection
        asin_selection = None
        if request.parent_asins or request.child_asins:
            asin_selection = ASINSelection(
                parent_asins=request.parent_asins or [],
                child_asins=request.child_asins or []
            )

        # Build time range
        time_range = None
        if request.start_date or request.end_date:
            time_range = TimeRange(
                start_date=request.start_date,
                end_date=request.end_date
            )

        # Get metrics data
        metrics_df = engine.get_metrics(
            seller_id=None,
            asin_selection=asin_selection,
            time_range=time_range,
            aggregation_level=request.aggregation_level,
            granularity=request.granularity,
            include_comparison=False
        )

        if metrics_df.empty:
            return StreamingResponse(
                iter(["No data available"]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=no_data.csv"}
            )

        # Determine which metrics to include
        metrics_list = request.metrics
        if request.metric_preset and request.metric_preset in _pivot_builder.METRIC_PRESETS:
            metrics_list = _pivot_builder.METRIC_PRESETS[request.metric_preset]

        # Build pivot table
        pivot_df = _pivot_builder.build_pivot(
            metrics_df,
            level=request.aggregation_level,
            granularity=request.granularity,
            metrics=metrics_list,
            include_totals=request.include_totals
        )

        # Generate CSV
        csv_content = _pivot_builder.to_csv(pivot_df)

        # Generate filename
        if request.filename:
            filename = request.filename
            if not filename.endswith(".csv"):
                filename += ".csv"
        else:
            date_str = date.today().strftime("%Y%m%d")
            filename = f"{seller_name}_{request.aggregation_level}_{request.granularity}_{date_str}.csv"

        # Return as streaming response
        return StreamingResponse(
            iter([csv_content]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seller/{seller_name}/yoy")
async def get_yoy_comparison(seller_name: str, request: YoYRequest = Body(...)):
    """Get Year-over-Year comparison for a specific month.

    Compares the requested month with the same month from the previous year.
    For example, January 2025 vs January 2024.

    Request body:
    - month: The month to compare (first day, e.g., 2025-01-01 for January 2025)
    - parent_asins: Optional list of parent/product names to filter
    - child_asins: Optional list of specific child ASINs to filter
    - aggregation_level: account, parent, or child

    Returns columns like:
    - total_sales_current, total_sales_prior, total_sales_yoy_change, total_sales_yoy_pct
    - sessions_current, sessions_prior, sessions_yoy_change, sessions_yoy_pct
    - etc.
    """
    try:
        engine = get_engine(seller_name)

        # Build ASIN selection
        asin_selection = None
        if request.parent_asins or request.child_asins:
            asin_selection = ASINSelection(
                parent_asins=request.parent_asins or [],
                child_asins=request.child_asins or []
            )

        # Get YoY comparison
        result = engine.get_yoy_comparison(
            seller_id=None,
            month=request.month,
            asin_selection=asin_selection,
            aggregation_level=request.aggregation_level
        )

        if result.empty:
            return {
                "data": [],
                "count": 0,
                "message": "No data available for the requested month"
            }

        # Replace NaN/inf values with None for JSON compatibility
        result = result.replace([float('inf'), float('-inf')], None)
        result = result.where(pd.notna(result), None)

        # Handle date serialization
        for col in result.columns:
            if result[col].dtype == "object":
                result[col] = result[col].apply(
                    lambda x: x.isoformat() if hasattr(x, "isoformat") else x
                )

        data = result.to_dict("records")

        return {
            "seller_name": seller_name,
            "current_month": request.month.isoformat(),
            "prior_year_month": date(request.month.year - 1, request.month.month, 1).isoformat(),
            "aggregation_level": request.aggregation_level,
            "columns": list(result.columns),
            "data": data,
            "count": len(data)
        }

    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"{str(e)}\n{traceback.format_exc()}")


@router.get("/filters")
async def get_available_filters():
    """Get all available filter options for the frontend."""
    return _pivot_builder.get_available_filters()


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    settings = get_settings()
    client = MetabaseClient(settings.metabase_url, settings.metabase_api_key)

    try:
        is_connected = client.test_connection()
        client.close()

        return {
            "status": "healthy" if is_connected else "unhealthy",
            "metabase_connected": is_connected,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "metabase_connected": False,
            "error": str(e),
        }


# ============================================================================
# Claude Integration Endpoints
# ============================================================================

class ToolCallRequest(BaseModel):
    """Request body for executing a Claude tool."""
    tool_name: str
    parameters: Dict[str, Any] = {}


@router.get("/claude/tools")
async def get_claude_tools():
    """Get all available Claude tools with their schemas.

    Returns the tool definitions in Claude tool-use format.
    """
    from ..claude.tools import CLAUDE_TOOLS, SYSTEM_PROMPT
    return {
        "tools": CLAUDE_TOOLS,
        "system_prompt": SYSTEM_PROMPT
    }


@router.post("/claude/execute")
async def execute_claude_tool(request: ToolCallRequest):
    """Execute a Claude tool call.

    This endpoint allows Claude to execute analytics tools.

    Request body:
    - tool_name: Name of the tool to execute
    - parameters: Tool parameters as a dictionary

    Returns the tool execution result.
    """
    from ..claude.executor import execute_tool
    from ..claude.tools import get_tool_by_name

    # Validate tool exists
    tool = get_tool_by_name(request.tool_name)
    if not tool:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown tool: {request.tool_name}"
        )

    # Execute tool
    try:
        result = execute_tool(request.tool_name, request.parameters)

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"{str(e)}\n{traceback.format_exc()}"
        )

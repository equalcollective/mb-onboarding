"""Pydantic schemas for API requests and responses."""

from pydantic import BaseModel, Field
from typing import Literal, Any, List, Optional
from datetime import date


class SellerResponse(BaseModel):
    """Seller information."""

    seller_id: int
    seller_name: str
    amazon_seller_id: Optional[str] = None
    marketplace: Optional[str] = None
    asin_count: Optional[int] = None


class SellersListResponse(BaseModel):
    """List of sellers response."""

    sellers: List[SellerResponse]
    count: int


class MetricsSummary(BaseModel):
    """Summary metrics for a period."""

    period_start: date
    total_ordered_sales: Optional[float] = None
    total_sessions: Optional[int] = None
    total_units_ordered: Optional[int] = None
    conversion_rate_pct: Optional[float] = None
    total_impressions: Optional[int] = None
    total_clicks: Optional[int] = None
    total_ad_spend: Optional[float] = None
    total_ad_sales: Optional[float] = None
    roas: Optional[float] = None
    acos_pct: Optional[float] = None
    organic_sales: Optional[float] = None
    tacos_pct: Optional[float] = None


class SellerSummaryResponse(BaseModel):
    """Seller account-level summary."""

    seller_id: int
    seller_name: str
    granularity: Literal["weekly", "monthly"]
    periods: List[MetricsSummary]


class ParentASINMetrics(BaseModel):
    """Metrics for a parent ASIN."""

    parent_asin: str
    normalized_name: Optional[str] = None
    total_ordered_sales: Optional[float] = None
    total_sessions: Optional[int] = None
    total_units_ordered: Optional[int] = None
    conversion_rate_pct: Optional[float] = None
    total_ad_spend: Optional[float] = None
    total_ad_sales: Optional[float] = None
    roas: Optional[float] = None
    tacos_pct: Optional[float] = None


class ParentASINListResponse(BaseModel):
    """List of parent ASINs with metrics."""

    seller_id: int
    seller_name: str
    granularity: Literal["weekly", "monthly"]
    period_start: date
    period_end: date
    parents: List[ParentASINMetrics]
    count: int


class PivotTableResponse(BaseModel):
    """Pivot table response."""

    seller_id: int
    seller_name: str
    level: Literal["parent", "child"]
    granularity: Literal["weekly", "monthly"]
    periods: List[date]
    columns: List[str]
    data: List[Any]
    count: int


class DataGap(BaseModel):
    """A missing data period."""

    missing_start: date
    missing_end: date
    granularity: Literal["weekly", "monthly"]


class DataGapsResponse(BaseModel):
    """Data gaps for a seller."""

    seller_id: int
    seller_name: str
    gaps: List[DataGap]
    count: int


class ErrorResponse(BaseModel):
    """Error response."""

    error: str
    detail: Optional[str] = None

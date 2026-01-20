"""Pivot table builder for time-series metrics."""

import pandas as pd
import numpy as np
from datetime import date
from typing import Literal, List, Optional, Dict, Any
from io import StringIO


class PivotBuilder:
    """Builds pivot tables with ASINs in rows and time-period metrics in columns."""

    # All available metrics with display info
    ALL_METRICS = [
        {"key": "total_sales", "label": "Sales", "format": "currency"},
        {"key": "sessions", "label": "Sessions", "format": "number"},
        {"key": "units", "label": "Units", "format": "number"},
        {"key": "cvr_pct", "label": "CVR%", "format": "percent"},
        {"key": "ad_spend", "label": "Ad_Spend", "format": "currency"},
        {"key": "ad_sales", "label": "Ad_Sales", "format": "currency"},
        {"key": "roas", "label": "ROAS", "format": "decimal"},
        {"key": "acos_pct", "label": "ACOS%", "format": "percent"},
        {"key": "organic_sales", "label": "Organic_Sales", "format": "currency"},
        {"key": "tacos_pct", "label": "TACoS%", "format": "percent"},
        {"key": "page_views", "label": "Page_Views", "format": "number"},
        {"key": "impressions", "label": "Impressions", "format": "number"},
        {"key": "clicks", "label": "Clicks", "format": "number"},
        {"key": "ctr_pct", "label": "CTR%", "format": "percent"},
        {"key": "organic_pct", "label": "Organic%", "format": "percent"},
        {"key": "ad_sales_pct", "label": "Ad_Sales%", "format": "percent"},
    ]

    # Preset metric groups for common use cases
    METRIC_PRESETS = {
        "sales_overview": ["total_sales", "sessions", "units", "cvr_pct"],
        "advertising": ["ad_spend", "ad_sales", "roas", "acos_pct", "impressions", "clicks", "ctr_pct"],
        "organic_vs_paid": ["total_sales", "ad_sales", "organic_sales", "organic_pct", "tacos_pct"],
        "traffic": ["sessions", "page_views", "impressions", "clicks", "cvr_pct", "ctr_pct"],
        "all": [m["key"] for m in ALL_METRICS],
    }

    def _format_date_label(self, d: date, granularity: str = "weekly") -> str:
        """Format date as column label (e.g., 'Jan_11' or 'Jan_2025')."""
        if granularity == "monthly":
            return d.strftime("%b_%Y")  # Jan_2025
        else:
            return d.strftime("%b_%d")  # Jan_11

    def build_pivot(
        self,
        df: pd.DataFrame,
        level: Literal["parent", "child", "account"] = "parent",
        granularity: Literal["weekly", "monthly"] = "weekly",
        metrics: Optional[List[str]] = None,
        include_totals: bool = True,
    ) -> pd.DataFrame:
        """Build a pivot table with date-labeled columns.

        Args:
            df: DataFrame with metrics by ASIN and period
            level: 'parent', 'child', or 'account' - determines row grouping
            granularity: 'weekly' or 'monthly' for date labeling
            metrics: List of metric keys to include (default: all available)
            include_totals: Whether to add a totals row at the bottom

        Returns:
            Pivot table DataFrame with every row fully populated (CSV-friendly)
        """
        if df.empty:
            return pd.DataFrame()

        # Determine period column
        period_col = "period_start_date"
        if period_col not in df.columns and "period_start" in df.columns:
            period_col = "period_start"

        if period_col not in df.columns:
            raise ValueError("No period column found in data")

        # Determine available metrics
        available_metrics = [m["key"] for m in self.ALL_METRICS if m["key"] in df.columns]
        if metrics:
            available_metrics = [m for m in metrics if m in available_metrics]

        if not available_metrics:
            raise ValueError("No metrics available in data")

        # Get sorted periods (most recent first)
        periods = sorted(df[period_col].unique(), reverse=True)

        # Determine row grouping columns
        if level == "account":
            group_cols = ["seller_id", "seller_name"]
        elif level == "parent":
            group_cols = ["seller_id", "seller_name", "adjusted_normalized_name"]
        else:  # child
            group_cols = ["seller_id", "seller_name", "adjusted_normalized_name", "child_asin", "adjusted_variant_name"]

        # Filter to available columns
        group_cols = [c for c in group_cols if c in df.columns]

        if not group_cols:
            raise ValueError("No grouping columns available")

        # Build pivot rows
        pivot_rows = []

        for group_key, group_df in df.groupby(group_cols, dropna=False):
            # Create row with all identifying columns filled
            if isinstance(group_key, tuple):
                row = dict(zip(group_cols, group_key))
            else:
                row = {group_cols[0]: group_key}

            # Add metrics for each period
            for period in periods:
                period_data = group_df[group_df[period_col] == period]
                date_label = self._format_date_label(period, granularity)

                for metric in available_metrics:
                    col_name = f"{date_label}_{metric}"
                    if not period_data.empty and metric in period_data.columns:
                        value = period_data[metric].iloc[0]
                        row[col_name] = value if pd.notna(value) else 0
                    else:
                        row[col_name] = 0

            pivot_rows.append(row)

        result = pd.DataFrame(pivot_rows)

        # Sort by first metric of most recent period (descending)
        if periods and available_metrics:
            first_metric_col = f"{self._format_date_label(periods[0], granularity)}_{available_metrics[0]}"
            if first_metric_col in result.columns:
                result = result.sort_values(first_metric_col, ascending=False)

        # Add totals row if requested
        if include_totals and len(result) > 1:
            result = self._add_totals_row(result, group_cols, periods, available_metrics, granularity)

        # Store metadata
        result.attrs["periods"] = periods
        result.attrs["metrics"] = available_metrics
        result.attrs["granularity"] = granularity
        result.attrs["level"] = level

        return result

    def _add_totals_row(
        self,
        df: pd.DataFrame,
        group_cols: List[str],
        periods: List[date],
        metrics: List[str],
        granularity: str
    ) -> pd.DataFrame:
        """Add a totals row at the bottom."""
        # Filter out the existing TOTAL row if any (prevent double totals)
        df = df[df.get("adjusted_normalized_name", df.get("seller_name", "")) != "TOTAL"].copy()

        totals = {}

        # Set identifier columns
        for col in group_cols:
            if col in ["seller_name", "adjusted_normalized_name", "adjusted_variant_name"]:
                totals[col] = "TOTAL"
            else:
                totals[col] = None

        # Sum/average metrics appropriately
        sum_metrics = ["total_sales", "sessions", "units", "ad_spend", "ad_sales",
                       "organic_sales", "page_views", "impressions", "clicks"]

        for period in periods:
            date_label = self._format_date_label(period, granularity)
            for metric in metrics:
                col_name = f"{date_label}_{metric}"
                if col_name in df.columns:
                    if metric in sum_metrics:
                        totals[col_name] = df[col_name].sum()
                    else:
                        # Initialize ratio metrics - will recalculate below
                        totals[col_name] = 0

        # Calculate ratio metrics from summed values
        for period in periods:
            date_label = self._format_date_label(period, granularity)

            # Get base values for calculations
            sessions_col = f"{date_label}_sessions"
            units_col = f"{date_label}_units"
            ad_spend_col = f"{date_label}_ad_spend"
            ad_sales_col = f"{date_label}_ad_sales"
            total_sales_col = f"{date_label}_total_sales"
            organic_col = f"{date_label}_organic_sales"
            impressions_col = f"{date_label}_impressions"
            clicks_col = f"{date_label}_clicks"

            sessions = totals.get(sessions_col, 0) or 0
            units = totals.get(units_col, 0) or 0
            ad_spend = totals.get(ad_spend_col, 0) or 0
            ad_sales = totals.get(ad_sales_col, 0) or 0
            total_sales = totals.get(total_sales_col, 0) or 0
            organic = totals.get(organic_col, 0) or 0
            impressions = totals.get(impressions_col, 0) or 0
            clicks = totals.get(clicks_col, 0) or 0

            # CVR% = units / sessions * 100
            cvr_col = f"{date_label}_cvr_pct"
            if cvr_col in totals:
                totals[cvr_col] = round(units / sessions * 100, 2) if sessions > 0 else 0

            # ROAS = ad_sales / ad_spend
            roas_col = f"{date_label}_roas"
            if roas_col in totals:
                totals[roas_col] = round(ad_sales / ad_spend, 2) if ad_spend > 0 else 0

            # ACOS% = ad_spend / ad_sales * 100
            acos_col = f"{date_label}_acos_pct"
            if acos_col in totals:
                totals[acos_col] = round(ad_spend / ad_sales * 100, 1) if ad_sales > 0 else 0

            # TACoS% = ad_spend / total_sales * 100
            tacos_col = f"{date_label}_tacos_pct"
            if tacos_col in totals:
                totals[tacos_col] = round(ad_spend / total_sales * 100, 1) if total_sales > 0 else 0

            # Organic% = organic_sales / total_sales * 100
            organic_pct_col = f"{date_label}_organic_pct"
            if organic_pct_col in totals:
                totals[organic_pct_col] = round(organic / total_sales * 100, 1) if total_sales > 0 else 0

            # CTR% = clicks / impressions * 100
            ctr_col = f"{date_label}_ctr_pct"
            if ctr_col in totals:
                totals[ctr_col] = round(clicks / impressions * 100, 2) if impressions > 0 else 0

        # Append totals row
        totals_df = pd.DataFrame([totals])
        return pd.concat([df, totals_df], ignore_index=True)

    def to_csv(self, pivot_df: pd.DataFrame) -> str:
        """Export pivot table to CSV string.

        Args:
            pivot_df: Pivot table DataFrame

        Returns:
            CSV string with all columns populated (no empty grouping cells)
        """
        if pivot_df.empty:
            return ""

        # The DataFrame should already have all columns filled
        # Just ensure no NaN display issues
        df = pivot_df.fillna("")

        return df.to_csv(index=False)

    def get_available_filters(self) -> Dict[str, Any]:
        """Return available filter options for the frontend."""
        return {
            "metrics": self.ALL_METRICS,
            "metric_presets": self.METRIC_PRESETS,
            "aggregation_levels": ["account", "parent", "child"],
            "granularities": ["weekly", "monthly"],
        }

    def filter_columns(
        self,
        pivot_df: pd.DataFrame,
        metrics: Optional[List[str]] = None,
        periods: Optional[List[date]] = None,
    ) -> pd.DataFrame:
        """Filter pivot table to specific metrics and/or periods.

        Args:
            pivot_df: Full pivot table
            metrics: List of metric keys to keep
            periods: List of period dates to keep

        Returns:
            Filtered pivot table
        """
        if pivot_df.empty:
            return pivot_df

        result = pivot_df.copy()
        stored_periods = pivot_df.attrs.get("periods", [])
        stored_metrics = pivot_df.attrs.get("metrics", [])
        granularity = pivot_df.attrs.get("granularity", "weekly")

        # Get identifier columns (non-metric columns)
        id_cols = [c for c in result.columns if not any(
            f"_{m}" in c for m in stored_metrics
        )]

        # Determine which metric columns to keep
        keep_cols = id_cols.copy()

        target_periods = periods if periods else stored_periods
        target_metrics = metrics if metrics else stored_metrics

        for period in target_periods:
            if period in stored_periods:
                date_label = self._format_date_label(period, granularity)
                for metric in target_metrics:
                    col_name = f"{date_label}_{metric}"
                    if col_name in result.columns:
                        keep_cols.append(col_name)

        # Filter to keep columns
        keep_cols = [c for c in keep_cols if c in result.columns]
        result = result[keep_cols]

        # Update metadata
        result.attrs["periods"] = target_periods
        result.attrs["metrics"] = target_metrics
        result.attrs["granularity"] = granularity

        return result

    def reorder_columns(
        self,
        pivot_df: pd.DataFrame,
        metric_order: Optional[List[str]] = None,
        period_order: Literal["recent_first", "oldest_first"] = "recent_first"
    ) -> pd.DataFrame:
        """Reorder pivot columns by metric or period priority.

        Args:
            pivot_df: Pivot table
            metric_order: Order of metrics (cycles through all periods for each metric)
            period_order: 'recent_first' or 'oldest_first'

        Returns:
            Reordered pivot table
        """
        if pivot_df.empty:
            return pivot_df

        stored_metrics = pivot_df.attrs.get("metrics", [])
        stored_periods = pivot_df.attrs.get("periods", [])
        granularity = pivot_df.attrs.get("granularity", "weekly")

        # Get identifier columns
        id_cols = [c for c in pivot_df.columns if not any(
            f"_{m}" in c for m in stored_metrics
        )]

        # Order periods
        if period_order == "oldest_first":
            ordered_periods = sorted(stored_periods)
        else:
            ordered_periods = sorted(stored_periods, reverse=True)

        # Order metrics
        if metric_order:
            ordered_metrics = [m for m in metric_order if m in stored_metrics]
            ordered_metrics += [m for m in stored_metrics if m not in ordered_metrics]
        else:
            ordered_metrics = stored_metrics

        # Build ordered column list
        ordered_cols = id_cols.copy()
        for period in ordered_periods:
            date_label = self._format_date_label(period, granularity)
            for metric in ordered_metrics:
                col_name = f"{date_label}_{metric}"
                if col_name in pivot_df.columns:
                    ordered_cols.append(col_name)

        return pivot_df[ordered_cols]

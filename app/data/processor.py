"""Data processor - joins, aggregates, and calculates metrics."""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Literal


class DataProcessor:
    """Processes raw data into aggregated metrics."""

    @staticmethod
    def get_week_start(d: date) -> date:
        """Get the Sunday start of the week for a given date.

        Args:
            d: Input date

        Returns:
            Sunday of that week
        """
        # weekday() returns 0=Monday, 6=Sunday
        # We want Sunday as start, so:
        days_since_sunday = (d.weekday() + 1) % 7
        return d - timedelta(days=days_since_sunday)

    @staticmethod
    def get_month_start(d: date) -> date:
        """Get the first day of the month for a given date.

        Args:
            d: Input date

        Returns:
            First day of that month
        """
        return d.replace(day=1)

    def join_with_asin_mapping(
        self,
        df: pd.DataFrame,
        mapping_df: pd.DataFrame,
        asin_column: str = "child_asin",
    ) -> pd.DataFrame:
        """Join data with ASIN mapping to get parent/normalized names.

        Args:
            df: DataFrame with child ASINs
            mapping_df: ASIN mapping DataFrame
            asin_column: Name of the ASIN column in df

        Returns:
            DataFrame with added parent/normalized columns
        """
        # Select only needed columns from mapping
        mapping_cols = [
            "child_asin",
            "adjusted_parent_asin",
            "adjusted_normalized_name",
            "adjusted_variant_name",
            "title",
        ]
        available_cols = [c for c in mapping_cols if c in mapping_df.columns]
        mapping_subset = mapping_df[available_cols].drop_duplicates(subset=["child_asin"])

        # Merge
        result = df.merge(
            mapping_subset,
            left_on=asin_column,
            right_on="child_asin",
            how="left",
        )

        # Fill missing with original values
        if "adjusted_parent_asin" in result.columns:
            result["adjusted_parent_asin"] = result["adjusted_parent_asin"].fillna(result[asin_column])
        if "adjusted_normalized_name" in result.columns:
            result["adjusted_normalized_name"] = result["adjusted_normalized_name"].fillna(result[asin_column])

        return result

    def aggregate_ads_to_period(
        self,
        ads_df: pd.DataFrame,
        granularity: Literal["weekly", "monthly"] = "weekly",
    ) -> pd.DataFrame:
        """Aggregate daily ads data to weekly or monthly.

        Args:
            ads_df: Daily advertising data
            granularity: 'weekly' or 'monthly'

        Returns:
            Aggregated DataFrame
        """
        if ads_df.empty:
            return ads_df

        df = ads_df.copy()

        # Calculate period start
        if granularity == "weekly":
            df["period_start"] = df["record_date"].apply(self.get_week_start)
        else:
            df["period_start"] = df["record_date"].apply(self.get_month_start)

        # Group and aggregate
        group_cols = ["seller_id", "seller_name", "child_asin", "period_start"]
        if "adjusted_parent_asin" in df.columns:
            group_cols.append("adjusted_parent_asin")
        if "adjusted_normalized_name" in df.columns:
            group_cols.append("adjusted_normalized_name")

        agg_dict = {
            "impressions": "sum",
            "clicks": "sum",
            "spend": "sum",
            "seven_day_total_sales": "sum",
            "seven_day_total_orders": "sum",
            "seven_day_total_units": "sum",
        }

        # Only aggregate columns that exist
        agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}

        result = df.groupby(group_cols, as_index=False).agg(agg_dict)

        return result

    def aggregate_to_parent(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate child-level data to parent ASIN level.

        Args:
            df: Child-level DataFrame with metrics

        Returns:
            Parent-level aggregated DataFrame
        """
        if df.empty:
            return df

        # Determine group columns
        group_cols = ["seller_id", "seller_name"]
        if "adjusted_parent_asin" in df.columns:
            group_cols.append("adjusted_parent_asin")
        if "adjusted_normalized_name" in df.columns:
            group_cols.append("adjusted_normalized_name")
        if "period_start_date" in df.columns:
            group_cols.append("period_start_date")
        elif "period_start" in df.columns:
            group_cols.append("period_start")

        # Metrics to sum
        sum_cols = [
            "ordered_product_sales",
            "sessions_total",
            "units_ordered",
            "page_views_total",
            "units_refunded",
            "impressions",
            "clicks",
            "spend",
            "seven_day_total_sales",
            "seven_day_total_orders",
            "seven_day_total_units",
        ]

        agg_dict = {col: "sum" for col in sum_cols if col in df.columns}

        if not agg_dict:
            return df

        return df.groupby(group_cols, as_index=False).agg(agg_dict)

    def aggregate_to_account(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate data to account (seller) level.

        Args:
            df: DataFrame with metrics (child or parent level)

        Returns:
            Account-level aggregated DataFrame
        """
        if df.empty:
            return df

        # Determine group columns
        group_cols = ["seller_id", "seller_name"]
        if "period_start_date" in df.columns:
            group_cols.append("period_start_date")
        elif "period_start" in df.columns:
            group_cols.append("period_start")

        # Metrics to sum
        sum_cols = [
            "ordered_product_sales",
            "sessions_total",
            "units_ordered",
            "page_views_total",
            "units_refunded",
            "impressions",
            "clicks",
            "spend",
            "seven_day_total_sales",
            "seven_day_total_orders",
            "seven_day_total_units",
        ]

        agg_dict = {col: "sum" for col in sum_cols if col in df.columns}

        if not agg_dict:
            return df

        return df.groupby(group_cols, as_index=False).agg(agg_dict)

    def calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics (conversion rate, ROAS, etc.).

        Args:
            df: DataFrame with base metrics

        Returns:
            DataFrame with additional calculated columns
        """
        if df.empty:
            return df

        result = df.copy()

        # Rename for consistency
        col_map = {
            "ordered_product_sales": "total_ordered_sales",
            "sessions_total": "total_sessions",
            "units_ordered": "total_units_ordered",
            "spend": "total_ad_spend",
            "seven_day_total_sales": "total_ad_sales",
            "seven_day_total_orders": "total_ad_orders",
            "seven_day_total_units": "total_ad_units",
            "impressions": "total_impressions",
            "clicks": "total_clicks",
        }

        for old_name, new_name in col_map.items():
            if old_name in result.columns and new_name not in result.columns:
                result[new_name] = result[old_name]

        # Conversion rate
        if "total_sessions" in result.columns and "total_units_ordered" in result.columns:
            result["conversion_rate_pct"] = np.where(
                result["total_sessions"] > 0,
                (result["total_units_ordered"] / result["total_sessions"] * 100).round(2),
                None,
            )

        # CTR
        if "total_impressions" in result.columns and "total_clicks" in result.columns:
            result["ctr_pct"] = np.where(
                result["total_impressions"] > 0,
                (result["total_clicks"] / result["total_impressions"] * 100).round(2),
                None,
            )

        # Ad conversion rate
        if "total_clicks" in result.columns and "total_ad_orders" in result.columns:
            result["ad_conversion_rate_pct"] = np.where(
                result["total_clicks"] > 0,
                (result["total_ad_orders"] / result["total_clicks"] * 100).round(2),
                None,
            )

        # ROAS
        if "total_ad_spend" in result.columns and "total_ad_sales" in result.columns:
            result["roas"] = np.where(
                result["total_ad_spend"] > 0,
                (result["total_ad_sales"] / result["total_ad_spend"]).round(2),
                None,
            )

        # ACOS
        if "total_ad_spend" in result.columns and "total_ad_sales" in result.columns:
            result["acos_pct"] = np.where(
                result["total_ad_sales"] > 0,
                (result["total_ad_spend"] / result["total_ad_sales"] * 100).round(2),
                None,
            )

        # Organic sales
        if "total_ordered_sales" in result.columns and "total_ad_sales" in result.columns:
            result["organic_sales"] = result["total_ordered_sales"] - result["total_ad_sales"].fillna(0)

            result["ad_sales_pct"] = np.where(
                result["total_ordered_sales"] > 0,
                (result["total_ad_sales"].fillna(0) / result["total_ordered_sales"] * 100).round(2),
                None,
            )

            result["organic_sales_pct"] = np.where(
                result["total_ordered_sales"] > 0,
                (result["organic_sales"] / result["total_ordered_sales"] * 100).round(2),
                None,
            )

        # TACoS
        if "total_ad_spend" in result.columns and "total_ordered_sales" in result.columns:
            result["tacos_pct"] = np.where(
                result["total_ordered_sales"] > 0,
                (result["total_ad_spend"].fillna(0) / result["total_ordered_sales"] * 100).round(2),
                None,
            )

        return result

    def combine_business_and_ads(
        self,
        business_df: pd.DataFrame,
        ads_df: pd.DataFrame,
        join_level: Literal["child", "parent", "account"] = "child",
    ) -> pd.DataFrame:
        """Combine business report and ads report data.

        Args:
            business_df: Business report DataFrame
            ads_df: Ads report DataFrame (already aggregated to same period)
            join_level: Level to join at ('child', 'parent', or 'account')

        Returns:
            Combined DataFrame
        """
        if business_df.empty and ads_df.empty:
            return pd.DataFrame()

        if business_df.empty:
            return self.calculate_derived_metrics(ads_df)

        if ads_df.empty:
            return self.calculate_derived_metrics(business_df)

        # Determine join columns based on level
        join_cols = ["seller_id"]

        if join_level == "child":
            join_cols.append("child_asin")
        elif join_level == "parent":
            join_cols.append("adjusted_parent_asin")

        # Add period column
        period_col = "period_start_date" if "period_start_date" in business_df.columns else "period_start"
        if period_col in business_df.columns:
            join_cols.append(period_col)

        # Prepare ads columns (avoid duplicates)
        ads_cols_to_use = [c for c in ads_df.columns if c not in business_df.columns or c in join_cols]
        ads_subset = ads_df[ads_cols_to_use]

        # Merge
        result = business_df.merge(
            ads_subset,
            on=join_cols,
            how="outer",
        )

        return self.calculate_derived_metrics(result)

    def detect_data_gaps(
        self,
        business_df: pd.DataFrame,
        ads_df: pd.DataFrame,
        granularity: Literal["weekly", "monthly"] = "weekly",
    ) -> pd.DataFrame:
        """Detect missing periods in business and ads data.

        Analyzes both reports to find:
        - Missing periods in business report
        - Missing periods in ads report
        - Periods with data in one report but not the other

        Args:
            business_df: Business report DataFrame with period_start_date
            ads_df: Ads report DataFrame with record_date
            granularity: 'weekly' or 'monthly' for analysis

        Returns:
            DataFrame with gap analysis per seller
        """
        gaps = []

        # Get unique sellers from both reports
        biz_sellers = set()
        ads_sellers = set()

        if not business_df.empty and "seller_id" in business_df.columns:
            biz_sellers = set(business_df["seller_id"].unique())
        if not ads_df.empty and "seller_id" in ads_df.columns:
            ads_sellers = set(ads_df["seller_id"].unique())

        all_sellers = biz_sellers | ads_sellers

        for seller_id in all_sellers:
            seller_gaps = self._analyze_seller_gaps(
                business_df, ads_df, seller_id, granularity
            )
            gaps.extend(seller_gaps)

        if not gaps:
            return pd.DataFrame(columns=[
                "seller_id", "seller_name", "period_start", "period_end",
                "granularity", "gap_type", "has_business_data", "has_ads_data"
            ])

        return pd.DataFrame(gaps)

    def _analyze_seller_gaps(
        self,
        business_df: pd.DataFrame,
        ads_df: pd.DataFrame,
        seller_id: str,
        granularity: Literal["weekly", "monthly"],
    ) -> list:
        """Analyze gaps for a single seller.

        Args:
            business_df: Business report DataFrame
            ads_df: Ads report DataFrame
            seller_id: Seller to analyze
            granularity: 'weekly' or 'monthly'

        Returns:
            List of gap dictionaries
        """
        gaps = []

        # Filter to seller
        biz_seller = business_df[business_df["seller_id"] == seller_id] if not business_df.empty else pd.DataFrame()
        ads_seller = ads_df[ads_df["seller_id"] == seller_id] if not ads_df.empty else pd.DataFrame()

        # Get seller name
        seller_name = None
        if not biz_seller.empty and "seller_name" in biz_seller.columns:
            seller_name = biz_seller["seller_name"].iloc[0]
        elif not ads_seller.empty and "seller_name" in ads_seller.columns:
            seller_name = ads_seller["seller_name"].iloc[0]

        # Get business report periods (already at correct granularity)
        biz_periods = set()
        if not biz_seller.empty and "period_start_date" in biz_seller.columns:
            # Filter by granularity if column exists
            if "period_granularity" in biz_seller.columns:
                biz_seller = biz_seller[biz_seller["period_granularity"] == granularity]
            biz_periods = set(pd.to_datetime(biz_seller["period_start_date"]).dt.date.unique())

        # Get ads report periods (aggregate daily to granularity)
        ads_periods = set()
        if not ads_seller.empty and "record_date" in ads_seller.columns:
            ads_dates = pd.to_datetime(ads_seller["record_date"]).dt.date
            if granularity == "weekly":
                ads_periods = set(ads_dates.apply(self.get_week_start).unique())
            else:
                ads_periods = set(ads_dates.apply(self.get_month_start).unique())

        # Combine all periods to find date range
        all_periods = biz_periods | ads_periods
        if not all_periods:
            return gaps

        min_date = min(all_periods)
        max_date = max(all_periods)

        # Generate expected periods
        expected_periods = self._generate_expected_periods(min_date, max_date, granularity)

        # Find gaps
        for period_start in expected_periods:
            has_biz = period_start in biz_periods
            has_ads = period_start in ads_periods

            # Calculate period end
            if granularity == "weekly":
                period_end = period_start + timedelta(days=6)
            else:
                # Last day of month
                if period_start.month == 12:
                    period_end = date(period_start.year + 1, 1, 1) - timedelta(days=1)
                else:
                    period_end = date(period_start.year, period_start.month + 1, 1) - timedelta(days=1)

            # Determine gap type
            if not has_biz and not has_ads:
                gap_type = "missing_both"
            elif not has_biz:
                gap_type = "missing_business"
            elif not has_ads:
                gap_type = "missing_ads"
            else:
                continue  # No gap

            gaps.append({
                "seller_id": seller_id,
                "seller_name": seller_name,
                "period_start": period_start,
                "period_end": period_end,
                "granularity": granularity,
                "gap_type": gap_type,
                "has_business_data": has_biz,
                "has_ads_data": has_ads,
            })

        return gaps

    def _generate_expected_periods(
        self,
        min_date: date,
        max_date: date,
        granularity: Literal["weekly", "monthly"],
    ) -> list:
        """Generate list of expected period start dates.

        Args:
            min_date: Start of date range
            max_date: End of date range
            granularity: 'weekly' or 'monthly'

        Returns:
            List of period start dates
        """
        periods = []
        current = min_date

        if granularity == "weekly":
            # Align to week start (Sunday)
            current = self.get_week_start(current)
            while current <= max_date:
                periods.append(current)
                current = current + timedelta(days=7)
        else:
            # Align to month start
            current = self.get_month_start(current)
            while current <= max_date:
                periods.append(current)
                # Move to next month
                if current.month == 12:
                    current = date(current.year + 1, 1, 1)
                else:
                    current = date(current.year, current.month + 1, 1)

        return periods

    def get_data_coverage_summary(
        self,
        business_df: pd.DataFrame,
        ads_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Get summary of data coverage per seller.

        Args:
            business_df: Business report DataFrame
            ads_df: Ads report DataFrame

        Returns:
            DataFrame with coverage summary per seller
        """
        summaries = []

        # Get unique sellers
        biz_sellers = set()
        ads_sellers = set()

        if not business_df.empty and "seller_id" in business_df.columns:
            biz_sellers = set(business_df["seller_id"].unique())
        if not ads_df.empty and "seller_id" in ads_df.columns:
            ads_sellers = set(ads_df["seller_id"].unique())

        all_sellers = biz_sellers | ads_sellers

        for seller_id in all_sellers:
            biz_seller = business_df[business_df["seller_id"] == seller_id] if not business_df.empty else pd.DataFrame()
            ads_seller = ads_df[ads_df["seller_id"] == seller_id] if not ads_df.empty else pd.DataFrame()

            # Get seller name
            seller_name = None
            if not biz_seller.empty and "seller_name" in biz_seller.columns:
                seller_name = biz_seller["seller_name"].iloc[0]
            elif not ads_seller.empty and "seller_name" in ads_seller.columns:
                seller_name = ads_seller["seller_name"].iloc[0]

            summary = {
                "seller_id": seller_id,
                "seller_name": seller_name,
            }

            # Business report coverage
            if not biz_seller.empty and "period_start_date" in biz_seller.columns:
                biz_dates = pd.to_datetime(biz_seller["period_start_date"])
                summary["biz_min_date"] = biz_dates.min().date()
                summary["biz_max_date"] = biz_dates.max().date()
                summary["biz_period_count"] = biz_seller["period_start_date"].nunique()

                if "period_granularity" in biz_seller.columns:
                    summary["biz_weekly_periods"] = len(biz_seller[biz_seller["period_granularity"] == "weekly"]["period_start_date"].unique())
                    summary["biz_monthly_periods"] = len(biz_seller[biz_seller["period_granularity"] == "monthly"]["period_start_date"].unique())
            else:
                summary["biz_min_date"] = None
                summary["biz_max_date"] = None
                summary["biz_period_count"] = 0

            # Ads report coverage
            if not ads_seller.empty and "record_date" in ads_seller.columns:
                ads_dates = pd.to_datetime(ads_seller["record_date"])
                summary["ads_min_date"] = ads_dates.min().date()
                summary["ads_max_date"] = ads_dates.max().date()
                summary["ads_day_count"] = ads_seller["record_date"].nunique()
            else:
                summary["ads_min_date"] = None
                summary["ads_max_date"] = None
                summary["ads_day_count"] = 0

            summaries.append(summary)

        if not summaries:
            return pd.DataFrame()

        return pd.DataFrame(summaries).sort_values("seller_name")

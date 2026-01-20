"""Metrics Engine - flexible filtering, aggregation, and comparisons."""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import List, Optional, Literal, Dict, Any
from dataclasses import dataclass


@dataclass
class ASINSelection:
    """ASIN selection with parent-to-child cascade."""
    parent_asins: List[str] = None  # Normalized names (auto-includes all children)
    child_asins: List[str] = None   # Specific child ASINs

    def __post_init__(self):
        self.parent_asins = self.parent_asins or []
        self.child_asins = self.child_asins or []


@dataclass
class TimeRange:
    """Time range filter."""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    specific_weeks: List[date] = None   # Specific week start dates
    specific_months: List[date] = None  # Specific month start dates

    def __post_init__(self):
        self.specific_weeks = self.specific_weeks or []
        self.specific_months = self.specific_months or []


class MetricsEngine:
    """Engine for flexible metrics aggregation and analysis."""

    def __init__(self, asin_df: pd.DataFrame, business_df: pd.DataFrame, ads_df: pd.DataFrame):
        """Initialize with data from Metabase cards.

        Args:
            asin_df: ASIN mapping data (card 666)
            business_df: Business report data (card 681)
            ads_df: Ads report data (card 665)
        """
        self.asin_df = asin_df.copy()
        self.business_df = business_df.copy()
        self.ads_df = ads_df.copy()

        # Ensure date columns are proper types
        self._prepare_data()

        # Build ASIN hierarchy lookup
        self._build_asin_hierarchy()

    def _prepare_data(self):
        """Prepare and convert data types."""
        # Convert dates
        if 'period_start_date' in self.business_df.columns:
            self.business_df['period_start_date'] = pd.to_datetime(
                self.business_df['period_start_date']
            ).dt.date

        if 'record_date' in self.ads_df.columns:
            self.ads_df['record_date'] = pd.to_datetime(
                self.ads_df['record_date']
            ).dt.date

        # Convert numeric columns
        biz_numeric = [
            'ordered_product_sales', 'ordered_product_sales_total',
            'sessions_total', 'units_ordered', 'units_ordered_total',
            'page_views_total', 'units_refunded', 'buy_box_percentage'
        ]
        for col in biz_numeric:
            if col in self.business_df.columns:
                self.business_df[col] = pd.to_numeric(
                    self.business_df[col], errors='coerce'
                ).fillna(0)

        ads_numeric = [
            'spend', 'seven_day_total_sales', 'impressions', 'clicks',
            'seven_day_total_orders', 'seven_day_total_units'
        ]
        for col in ads_numeric:
            if col in self.ads_df.columns:
                self.ads_df[col] = pd.to_numeric(
                    self.ads_df[col], errors='coerce'
                ).fillna(0)

    def _build_asin_hierarchy(self):
        """Build lookup from parent to children."""
        self.parent_to_children: Dict[str, List[str]] = {}
        self.child_to_parent: Dict[str, str] = {}

        if self.asin_df.empty:
            return

        for _, row in self.asin_df.iterrows():
            parent = row.get('adjusted_normalized_name', '')
            child = row.get('child_asin', '')

            if parent and child:
                if parent not in self.parent_to_children:
                    self.parent_to_children[parent] = []
                self.parent_to_children[parent].append(child)
                self.child_to_parent[child] = parent

    def get_sellers(self) -> pd.DataFrame:
        """Get list of sellers with ASIN counts."""
        if self.asin_df.empty:
            return pd.DataFrame()

        return self.asin_df.groupby(
            ['seller_id', 'seller_name', 'seller_marketplace'],
            as_index=False
        ).agg({
            'child_asin': 'nunique',
            'adjusted_parent_asin': 'nunique',
            'adjusted_normalized_name': 'nunique'
        }).rename(columns={
            'child_asin': 'asin_count',
            'adjusted_parent_asin': 'parent_asin_count',
            'adjusted_normalized_name': 'product_count'
        })

    def get_asin_hierarchy(self, seller_id: Optional[int] = None) -> Dict[str, Any]:
        """Get ASIN hierarchy for selection UI.

        Args:
            seller_id: Optional seller filter

        Returns:
            Dict with parents and their children
        """
        df = self.asin_df
        if seller_id and 'seller_id' in df.columns:
            df = df[df['seller_id'] == seller_id]

        hierarchy = {}
        for parent, children in self.parent_to_children.items():
            # Filter children to this seller if applicable
            if seller_id:
                children = [c for c in children if c in df['child_asin'].values]

            if children:
                # Get child details
                child_details = df[df['child_asin'].isin(children)][
                    ['child_asin', 'adjusted_variant_name', 'title']
                ].drop_duplicates().to_dict('records')

                hierarchy[parent] = {
                    'children': child_details,
                    'child_count': len(child_details)
                }

        return hierarchy

    def _expand_asin_selection(self, selection: ASINSelection) -> List[str]:
        """Expand parent selections to include all children.

        Args:
            selection: ASIN selection with parents and/or children

        Returns:
            List of all child ASINs to include
        """
        child_asins = set(selection.child_asins)

        # Expand parents to children
        for parent in selection.parent_asins:
            if parent in self.parent_to_children:
                child_asins.update(self.parent_to_children[parent])

        return list(child_asins)

    def _filter_by_time(
        self,
        df: pd.DataFrame,
        time_range: TimeRange,
        date_col: str,
        granularity_col: Optional[str] = None,
        granularity: Optional[str] = None
    ) -> pd.DataFrame:
        """Filter dataframe by time range.

        Args:
            df: DataFrame to filter
            time_range: Time range specification
            date_col: Name of date column
            granularity_col: Column containing granularity (for business report)
            granularity: Filter to specific granularity

        Returns:
            Filtered DataFrame
        """
        if df.empty:
            return df

        result = df.copy()

        # Filter by granularity if specified
        if granularity_col and granularity and granularity_col in result.columns:
            result = result[result[granularity_col] == granularity]

        # Filter by date range
        if time_range.start_date:
            result = result[result[date_col] >= time_range.start_date]
        if time_range.end_date:
            result = result[result[date_col] <= time_range.end_date]

        # Filter by specific weeks
        if time_range.specific_weeks:
            result = result[result[date_col].isin(time_range.specific_weeks)]

        # Filter by specific months
        if time_range.specific_months:
            result['_month_start'] = result[date_col].apply(
                lambda d: d.replace(day=1) if d else None
            )
            result = result[result['_month_start'].isin(time_range.specific_months)]
            result = result.drop(columns=['_month_start'])

        return result

    def _aggregate_ads_to_period(
        self,
        df: pd.DataFrame,
        granularity: Literal['weekly', 'monthly']
    ) -> pd.DataFrame:
        """Aggregate daily ads to weekly or monthly."""
        if df.empty:
            return df

        result = df.copy()

        if granularity == 'weekly':
            result['period_start'] = result['record_date'].apply(self._get_week_start)
        else:
            result['period_start'] = result['record_date'].apply(
                lambda d: d.replace(day=1) if d else None
            )

        group_cols = ['seller_id', 'child_asin', 'period_start']
        group_cols = [c for c in group_cols if c in result.columns]

        agg_cols = {
            'impressions': 'sum',
            'clicks': 'sum',
            'spend': 'sum',
            'seven_day_total_sales': 'sum',
            'seven_day_total_orders': 'sum',
            'seven_day_total_units': 'sum'
        }
        agg_cols = {k: v for k, v in agg_cols.items() if k in result.columns}

        return result.groupby(group_cols, as_index=False).agg(agg_cols)

    @staticmethod
    def _get_week_start(d: date) -> date:
        """Get Sunday start of week."""
        if not d:
            return None
        days_since_sunday = (d.weekday() + 1) % 7
        return d - timedelta(days=days_since_sunday)

    def get_metrics(
        self,
        seller_id: Optional[int] = None,
        asin_selection: Optional[ASINSelection] = None,
        time_range: Optional[TimeRange] = None,
        aggregation_level: Literal['child', 'parent', 'account', 'custom'] = 'account',
        granularity: Literal['weekly', 'monthly'] = 'weekly',
        include_comparison: bool = False
    ) -> pd.DataFrame:
        """Get metrics with flexible filtering and aggregation.

        Args:
            seller_id: Seller to get metrics for (optional if data already filtered)
            asin_selection: Optional ASIN filter (parents and/or children)
            time_range: Optional time range filter
            aggregation_level: How to aggregate results
            granularity: weekly or monthly
            include_comparison: Include WoW/MoM comparison columns

        Returns:
            DataFrame with metrics
        """
        # Filter business report by seller if provided
        if seller_id is not None:
            biz = self.business_df[self.business_df['seller_id'] == seller_id].copy()
            ads = self.ads_df[self.ads_df['seller_id'] == seller_id].copy()
        else:
            # Use all data (assuming already filtered by seller in Metabase query)
            biz = self.business_df.copy()
            ads = self.ads_df.copy()

        if biz.empty:
            return pd.DataFrame()

        # Apply time range filter
        if time_range:
            biz = self._filter_by_time(
                biz, time_range, 'period_start_date',
                'period_granularity', granularity
            )
            ads = self._filter_by_time(ads, time_range, 'record_date')
        else:
            # Default to specified granularity
            if 'period_granularity' in biz.columns:
                biz = biz[biz['period_granularity'] == granularity]

        # Apply ASIN filter
        if asin_selection and (asin_selection.parent_asins or asin_selection.child_asins):
            selected_children = self._expand_asin_selection(asin_selection)
            if selected_children:
                biz = biz[biz['child_asin'].isin(selected_children)]
                ads = ads[ads['child_asin'].isin(selected_children)]

        if biz.empty:
            return pd.DataFrame()

        # Aggregate ads to match business report granularity
        if not ads.empty:
            ads = self._aggregate_ads_to_period(ads, granularity)

        # Determine aggregation
        if aggregation_level == 'custom':
            # Single row with all selected ASINs combined
            result = self._aggregate_custom(biz, ads, granularity)
        elif aggregation_level == 'account':
            result = self._aggregate_to_account(biz, ads, granularity)
        elif aggregation_level == 'parent':
            result = self._aggregate_to_parent(biz, ads, granularity)
        else:  # child
            result = self._aggregate_to_child(biz, ads, granularity)

        # Calculate derived metrics
        result = self._calculate_derived_metrics(result)

        # Add comparison columns if requested
        if include_comparison and aggregation_level != 'custom':
            result = self._add_comparisons(result, granularity)

        return result

    def _aggregate_custom(
        self,
        biz: pd.DataFrame,
        ads: pd.DataFrame,
        granularity: str
    ) -> pd.DataFrame:
        """Aggregate all selected ASINs into single row per period."""
        date_col = 'period_start_date'

        biz_agg = biz.groupby([date_col], as_index=False).agg({
            'ordered_product_sales_total': 'sum',
            'sessions_total': 'sum',
            'units_ordered_total': 'sum',
            'page_views_total': 'sum'
        })

        if not ads.empty:
            ads_agg = ads.groupby(['period_start'], as_index=False).agg({
                'spend': 'sum',
                'seven_day_total_sales': 'sum',
                'impressions': 'sum',
                'clicks': 'sum',
                'seven_day_total_orders': 'sum'
            })

            # Merge
            result = biz_agg.merge(
                ads_agg,
                left_on=date_col,
                right_on='period_start',
                how='left'
            )
        else:
            result = biz_agg

        result['aggregation'] = 'custom_selection'
        return result

    def _aggregate_to_account(
        self,
        biz: pd.DataFrame,
        ads: pd.DataFrame,
        granularity: str
    ) -> pd.DataFrame:
        """Aggregate to account level per period."""
        date_col = 'period_start_date'
        group_cols = ['seller_id', 'seller_name', date_col]
        group_cols = [c for c in group_cols if c in biz.columns]

        biz_cols = {
            'ordered_product_sales_total': 'sum',
            'sessions_total': 'sum',
            'units_ordered_total': 'sum',
            'page_views_total': 'sum'
        }
        biz_cols = {k: v for k, v in biz_cols.items() if k in biz.columns}

        biz_agg = biz.groupby(group_cols, as_index=False).agg(biz_cols)

        if not ads.empty:
            ads_group = ['seller_id', 'period_start']
            ads_group = [c for c in ads_group if c in ads.columns]

            ads_cols = {
                'spend': 'sum',
                'seven_day_total_sales': 'sum',
                'impressions': 'sum',
                'clicks': 'sum',
                'seven_day_total_orders': 'sum'
            }
            ads_cols = {k: v for k, v in ads_cols.items() if k in ads.columns}

            ads_agg = ads.groupby(ads_group, as_index=False).agg(ads_cols)

            result = biz_agg.merge(
                ads_agg,
                left_on=['seller_id', date_col],
                right_on=['seller_id', 'period_start'],
                how='left'
            )
        else:
            result = biz_agg

        return result

    def _aggregate_to_parent(
        self,
        biz: pd.DataFrame,
        ads: pd.DataFrame,
        granularity: str
    ) -> pd.DataFrame:
        """Aggregate to parent (normalized name) level per period."""
        date_col = 'period_start_date'
        group_cols = ['seller_id', 'seller_name', 'adjusted_normalized_name', date_col]
        group_cols = [c for c in group_cols if c in biz.columns]

        biz_cols = {
            'ordered_product_sales_total': 'sum',
            'sessions_total': 'sum',
            'units_ordered_total': 'sum',
            'page_views_total': 'sum'
        }
        biz_cols = {k: v for k, v in biz_cols.items() if k in biz.columns}

        biz_agg = biz.groupby(group_cols, as_index=False).agg(biz_cols)

        if not ads.empty:
            # Map ads child ASINs to parents
            ads = ads.copy()
            ads['adjusted_normalized_name'] = ads['child_asin'].map(self.child_to_parent)

            ads_group = ['seller_id', 'adjusted_normalized_name', 'period_start']
            ads_group = [c for c in ads_group if c in ads.columns]

            ads_cols = {
                'spend': 'sum',
                'seven_day_total_sales': 'sum',
                'impressions': 'sum',
                'clicks': 'sum',
                'seven_day_total_orders': 'sum'
            }
            ads_cols = {k: v for k, v in ads_cols.items() if k in ads.columns}

            ads_agg = ads.groupby(ads_group, as_index=False).agg(ads_cols)

            result = biz_agg.merge(
                ads_agg,
                left_on=['seller_id', 'adjusted_normalized_name', date_col],
                right_on=['seller_id', 'adjusted_normalized_name', 'period_start'],
                how='left'
            )
        else:
            result = biz_agg

        return result

    def _aggregate_to_child(
        self,
        biz: pd.DataFrame,
        ads: pd.DataFrame,
        granularity: str
    ) -> pd.DataFrame:
        """Keep at child ASIN level per period."""
        date_col = 'period_start_date'
        group_cols = [
            'seller_id', 'seller_name', 'child_asin',
            'adjusted_normalized_name', 'adjusted_variant_name', date_col
        ]
        group_cols = [c for c in group_cols if c in biz.columns]

        biz_cols = {
            'ordered_product_sales_total': 'sum',
            'sessions_total': 'sum',
            'units_ordered_total': 'sum',
            'page_views_total': 'sum'
        }
        biz_cols = {k: v for k, v in biz_cols.items() if k in biz.columns}

        biz_agg = biz.groupby(group_cols, as_index=False).agg(biz_cols)

        if not ads.empty:
            ads_group = ['seller_id', 'child_asin', 'period_start']
            ads_group = [c for c in ads_group if c in ads.columns]

            ads_cols = {
                'spend': 'sum',
                'seven_day_total_sales': 'sum',
                'impressions': 'sum',
                'clicks': 'sum',
                'seven_day_total_orders': 'sum'
            }
            ads_cols = {k: v for k, v in ads_cols.items() if k in ads.columns}

            ads_agg = ads.groupby(ads_group, as_index=False).agg(ads_cols)

            result = biz_agg.merge(
                ads_agg,
                left_on=['seller_id', 'child_asin', date_col],
                right_on=['seller_id', 'child_asin', 'period_start'],
                how='left'
            )
        else:
            result = biz_agg

        return result

    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics."""
        if df.empty:
            return df

        result = df.copy()

        # Standardize column names
        rename_map = {
            'ordered_product_sales_total': 'total_sales',
            'sessions_total': 'sessions',
            'units_ordered_total': 'units',
            'page_views_total': 'page_views',
            'seven_day_total_sales': 'ad_sales',
            'spend': 'ad_spend',
            'seven_day_total_orders': 'ad_orders'
        }
        result = result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns})

        # Fill NaN in ads columns with 0
        for col in ['ad_sales', 'ad_spend', 'impressions', 'clicks', 'ad_orders']:
            if col in result.columns:
                result[col] = result[col].fillna(0)

        # Organic sales
        if 'total_sales' in result.columns and 'ad_sales' in result.columns:
            result['organic_sales'] = result['total_sales'] - result['ad_sales']

        # Conversion rate (units / sessions)
        if 'sessions' in result.columns and 'units' in result.columns:
            result['cvr_pct'] = np.where(
                result['sessions'] > 0,
                (result['units'] / result['sessions'] * 100).round(2),
                0
            )

        # CTR (clicks / impressions)
        if 'impressions' in result.columns and 'clicks' in result.columns:
            result['ctr_pct'] = np.where(
                result['impressions'] > 0,
                (result['clicks'] / result['impressions'] * 100).round(2),
                0
            )

        # ROAS (ad sales / ad spend)
        if 'ad_sales' in result.columns and 'ad_spend' in result.columns:
            result['roas'] = np.where(
                result['ad_spend'] > 0,
                (result['ad_sales'] / result['ad_spend']).round(2),
                0
            )

        # ACOS (ad spend / ad sales)
        if 'ad_spend' in result.columns and 'ad_sales' in result.columns:
            result['acos_pct'] = np.where(
                result['ad_sales'] > 0,
                (result['ad_spend'] / result['ad_sales'] * 100).round(1),
                0
            )

        # TACoS (ad spend / total sales)
        if 'ad_spend' in result.columns and 'total_sales' in result.columns:
            result['tacos_pct'] = np.where(
                result['total_sales'] > 0,
                (result['ad_spend'] / result['total_sales'] * 100).round(1),
                0
            )

        # Organic %
        if 'organic_sales' in result.columns and 'total_sales' in result.columns:
            result['organic_pct'] = np.where(
                result['total_sales'] > 0,
                (result['organic_sales'] / result['total_sales'] * 100).round(1),
                0
            )

        # Ad sales %
        if 'ad_sales' in result.columns and 'total_sales' in result.columns:
            result['ad_sales_pct'] = np.where(
                result['total_sales'] > 0,
                (result['ad_sales'] / result['total_sales'] * 100).round(1),
                0
            )

        return result

    def _add_comparisons(
        self,
        df: pd.DataFrame,
        granularity: str
    ) -> pd.DataFrame:
        """Add WoW or MoM comparison columns."""
        if df.empty:
            return df

        result = df.copy()
        date_col = 'period_start_date'

        if date_col not in result.columns:
            return result

        # Sort by date
        result = result.sort_values(date_col)

        # Metrics to compare
        compare_cols = ['total_sales', 'sessions', 'units', 'ad_spend', 'ad_sales', 'organic_sales']
        compare_cols = [c for c in compare_cols if c in result.columns]

        # Determine grouping (for parent/child level, group within each entity)
        group_cols = []
        if 'adjusted_normalized_name' in result.columns:
            group_cols.append('adjusted_normalized_name')
        if 'child_asin' in result.columns:
            group_cols.append('child_asin')

        prefix = 'wow' if granularity == 'weekly' else 'mom'

        for col in compare_cols:
            if group_cols:
                result[f'{prefix}_{col}_prev'] = result.groupby(group_cols)[col].shift(1)
            else:
                result[f'{prefix}_{col}_prev'] = result[col].shift(1)

            result[f'{prefix}_{col}_change'] = result[col] - result[f'{prefix}_{col}_prev']
            result[f'{prefix}_{col}_change_pct'] = np.where(
                result[f'{prefix}_{col}_prev'] > 0,
                (result[f'{prefix}_{col}_change'] / result[f'{prefix}_{col}_prev'] * 100).round(1),
                0
            )

        return result

    def get_yoy_comparison(
        self,
        seller_id: Optional[int],
        month: date,
        asin_selection: Optional[ASINSelection] = None,
        aggregation_level: Literal['child', 'parent', 'account'] = 'account'
    ) -> pd.DataFrame:
        """Get Year-over-Year comparison for a specific month.

        Args:
            seller_id: Seller to get metrics for
            month: The month to compare (e.g., date(2025, 1, 1) for January 2025)
            asin_selection: Optional ASIN filter
            aggregation_level: How to aggregate results

        Returns:
            DataFrame with current month, prior year month, and change columns
        """
        # Normalize to first of month
        current_month = month.replace(day=1)
        prior_year_month = date(current_month.year - 1, current_month.month, 1)

        # Get current month metrics
        current_range = TimeRange(specific_months=[current_month])
        current_metrics = self.get_metrics(
            seller_id=seller_id,
            asin_selection=asin_selection,
            time_range=current_range,
            aggregation_level=aggregation_level,
            granularity='monthly',
            include_comparison=False
        )

        # Get prior year month metrics
        prior_range = TimeRange(specific_months=[prior_year_month])
        prior_metrics = self.get_metrics(
            seller_id=seller_id,
            asin_selection=asin_selection,
            time_range=prior_range,
            aggregation_level=aggregation_level,
            granularity='monthly',
            include_comparison=False
        )

        if current_metrics.empty:
            return pd.DataFrame()

        # Prepare result
        result = current_metrics.copy()

        # Add suffix to current period columns
        metric_cols = [
            'total_sales', 'sessions', 'units', 'page_views',
            'ad_spend', 'ad_sales', 'impressions', 'clicks',
            'organic_sales', 'cvr_pct', 'ctr_pct', 'roas', 'acos_pct', 'tacos_pct'
        ]
        metric_cols = [c for c in metric_cols if c in result.columns]

        # Rename current columns
        rename_current = {col: f'{col}_current' for col in metric_cols}
        result = result.rename(columns=rename_current)

        # Determine merge keys based on aggregation level
        if aggregation_level == 'child':
            merge_keys = ['seller_id', 'child_asin']
        elif aggregation_level == 'parent':
            merge_keys = ['seller_id', 'adjusted_normalized_name']
        else:  # account
            merge_keys = ['seller_id']

        merge_keys = [k for k in merge_keys if k in result.columns]

        # Merge with prior year
        if not prior_metrics.empty and merge_keys:
            # Prepare prior metrics
            prior_rename = {col: f'{col}_prior' for col in metric_cols if col in prior_metrics.columns}
            prior_metrics = prior_metrics.rename(columns=prior_rename)

            # Keep only merge keys and renamed metric columns
            prior_cols = merge_keys + [c for c in prior_metrics.columns if c.endswith('_prior')]
            prior_metrics = prior_metrics[prior_cols].drop_duplicates(subset=merge_keys)

            result = result.merge(prior_metrics, on=merge_keys, how='left')

        # Calculate YoY changes
        for col in metric_cols:
            current_col = f'{col}_current'
            prior_col = f'{col}_prior'

            if current_col in result.columns:
                if prior_col not in result.columns:
                    result[prior_col] = 0

                result[prior_col] = result[prior_col].fillna(0)

                # Absolute change
                result[f'{col}_yoy_change'] = result[current_col] - result[prior_col]

                # Percent change
                result[f'{col}_yoy_pct'] = np.where(
                    result[prior_col] > 0,
                    ((result[current_col] - result[prior_col]) / result[prior_col] * 100).round(1),
                    np.where(result[current_col] > 0, 100.0, 0.0)  # 100% if new, 0% if both zero
                )

        # Add period info
        result['current_month'] = current_month
        result['prior_year_month'] = prior_year_month

        # Remove period_start_date column if present (redundant now)
        if 'period_start_date' in result.columns:
            result = result.drop(columns=['period_start_date'])

        return result

    def get_cumulative_metrics(
        self,
        seller_id: Optional[int] = None,
        asin_selection: Optional[ASINSelection] = None,
        time_range: Optional[TimeRange] = None,
        aggregation_level: Literal['child', 'parent', 'account', 'custom'] = 'account',
        granularity: Literal['weekly', 'monthly'] = 'weekly'
    ) -> pd.DataFrame:
        """Get cumulative (running total) metrics.

        Args:
            seller_id: Seller to get metrics for
            asin_selection: Optional ASIN filter
            time_range: Time range (cumulative over these periods)
            aggregation_level: How to aggregate
            granularity: weekly or monthly

        Returns:
            Single row with cumulative totals for the selected periods
        """
        # Get period-by-period metrics
        metrics = self.get_metrics(
            seller_id=seller_id,
            asin_selection=asin_selection,
            time_range=time_range,
            aggregation_level=aggregation_level,
            granularity=granularity,
            include_comparison=False
        )

        if metrics.empty:
            return pd.DataFrame()

        # Sum up all periods
        sum_cols = [
            'total_sales', 'sessions', 'units', 'page_views',
            'ad_spend', 'ad_sales', 'impressions', 'clicks', 'ad_orders',
            'organic_sales'
        ]
        sum_cols = [c for c in sum_cols if c in metrics.columns]

        result = {col: metrics[col].sum() for col in sum_cols}

        # Add period info
        if 'period_start_date' in metrics.columns:
            result['period_start'] = metrics['period_start_date'].min()
            result['period_end'] = metrics['period_start_date'].max()
            result['periods_count'] = metrics['period_start_date'].nunique()

        # Calculate derived metrics on cumulative
        result_df = pd.DataFrame([result])
        return self._calculate_derived_metrics(result_df)

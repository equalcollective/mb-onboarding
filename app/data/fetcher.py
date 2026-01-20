"""Data fetcher - retrieves data from Metabase cards."""

import pandas as pd
from datetime import date, timedelta
from typing import Literal, Optional

from ..metabase.client import MetabaseClient
from ..config import get_settings


class DataFetcher:
    """Fetches data from Metabase cards with caching."""

    def __init__(self, client: Optional[MetabaseClient] = None):
        """Initialize the data fetcher.

        Args:
            client: Optional MetabaseClient instance. If not provided, creates one.
        """
        if client is None:
            settings = get_settings()
            self.client = MetabaseClient(settings.metabase_url, settings.metabase_api_key)
            self._owns_client = True
        else:
            self.client = client
            self._owns_client = False

        self.settings = get_settings()

    def get_sellers(self) -> pd.DataFrame:
        """Fetch list of all sellers.

        Returns:
            DataFrame with seller_id, seller_name, amazon_seller_id, marketplace, asin_count
        """
        card_id = self.settings.card_sellers_list
        if not card_id:
            raise ValueError("CARD_SELLERS_LIST not configured in environment")

        return self.client.fetch_card(card_id)

    def get_asin_mapping(self, seller_name: Optional[str] = None) -> pd.DataFrame:
        """Fetch ASIN mapping (child -> parent -> normalized name).

        Args:
            seller_name: Optional filter by seller name

        Returns:
            DataFrame with ASIN hierarchy and metadata
        """
        card_id = self.settings.card_asin_mapping
        if not card_id:
            raise ValueError("CARD_ASIN_MAPPING not configured in environment")

        params = {}
        if seller_name:
            params["seller_name"] = seller_name

        return self.client.fetch_card(card_id, params if params else None)

    def get_business_report(
        self,
        seller_name: Optional[str] = None,
        granularity: Literal["weekly", "monthly"] = "weekly",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """Fetch business report data (sales/traffic).

        Args:
            seller_name: Optional filter by seller name
            granularity: 'weekly' or 'monthly'
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            DataFrame with sales/traffic metrics by child ASIN
        """
        card_id = self.settings.card_business_report
        if not card_id:
            raise ValueError("CARD_BUSINESS_REPORT not configured in environment")

        params = {"granularity": granularity}
        if seller_name:
            params["seller_name"] = seller_name
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        df = self.client.fetch_card(card_id, params)

        # Convert date columns
        if "period_start_date" in df.columns:
            df["period_start_date"] = pd.to_datetime(df["period_start_date"]).dt.date

        return df

    def get_ads_report(
        self,
        seller_name: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """Fetch advertising report data.

        Args:
            seller_name: Optional filter by seller name
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            DataFrame with advertising metrics by child ASIN (daily)
        """
        card_id = self.settings.card_ads_report
        if not card_id:
            raise ValueError("CARD_ADS_REPORT not configured in environment")

        params = {}
        if seller_name:
            params["seller_name"] = seller_name
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        df = self.client.fetch_card(card_id, params if params else None)

        # Convert date columns
        if "record_date" in df.columns:
            df["record_date"] = pd.to_datetime(df["record_date"]).dt.date

        return df

    def get_data_gaps(
        self,
        granularity: Literal["weekly", "monthly"] = "weekly",
        seller_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch data gap analysis.

        Args:
            granularity: 'weekly' or 'monthly'
            seller_name: Optional filter by seller name

        Returns:
            DataFrame with missing periods
        """
        if granularity == "weekly":
            card_id = self.settings.card_gaps_weekly
        else:
            card_id = self.settings.card_gaps_monthly

        if not card_id:
            raise ValueError(f"CARD_GAPS_{granularity.upper()} not configured in environment")

        params = {}
        if seller_name:
            params["seller_name"] = seller_name

        return self.client.fetch_card(card_id, params if params else None)

    def close(self):
        """Close the client if we own it."""
        if self._owns_client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

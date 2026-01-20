"""Metabase API client for fetching card data."""

import httpx
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import date


class MetabaseClient:
    """Client for interacting with Metabase API."""

    def __init__(self, base_url: str, api_key: str):
        """Initialize the Metabase client.

        Args:
            base_url: Metabase instance URL (e.g., https://metabase.example.com)
            api_key: Metabase API key for authentication
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={"x-api-key": self.api_key},
            timeout=60.0,
        )

    def _build_parameters(self, params: Dict[str, Any]) -> List[Dict]:
        """Convert simple params dict to Metabase parameter format.

        Args:
            params: Dictionary of parameter names to values

        Returns:
            List of Metabase-formatted parameter objects
        """
        result = []
        for key, value in params.items():
            if value is None:
                continue

            param = {"type": "category", "value": value, "target": ["variable", ["template-tag", key]]}

            # Handle date parameters
            if isinstance(value, date):
                param["type"] = "date/single"
                param["value"] = value.isoformat()

            # Handle field filters (seller_name, etc.) - requires list value
            if key in ["seller_name", "normalized_name"]:
                param["type"] = "string/="
                param["target"] = ["dimension", ["template-tag", key]]
                # Value must be a list for string/= operator
                param["value"] = [value] if not isinstance(value, list) else value

            result.append(param)

        return result

    def fetch_card(self, card_id: int, parameters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Fetch data from a Metabase card (saved question).

        Args:
            card_id: The ID of the Metabase card
            parameters: Optional parameters to pass to the card

        Returns:
            DataFrame with the query results

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        url = f"/api/card/{card_id}/query/json"

        if parameters:
            metabase_params = self._build_parameters(parameters)
            response = self._client.post(url, json={"parameters": metabase_params})
        else:
            response = self._client.post(url)

        response.raise_for_status()
        data = response.json()

        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict) and "error" in data:
            raise ValueError(f"Metabase error: {data['error']}")
        else:
            return pd.DataFrame()

    def fetch_card_metadata(self, card_id: int) -> dict:
        """Fetch metadata about a card (columns, parameters, etc.).

        Args:
            card_id: The ID of the Metabase card

        Returns:
            Dictionary with card metadata
        """
        url = f"/api/card/{card_id}"
        response = self._client.get(url)
        response.raise_for_status()
        return response.json()

    def list_cards(self) -> List[Dict]:
        """List all available cards.

        Returns:
            List of card metadata dictionaries
        """
        url = "/api/card"
        response = self._client.get(url)
        response.raise_for_status()
        return response.json()

    def test_connection(self) -> bool:
        """Test the connection to Metabase.

        Returns:
            True if connection is successful
        """
        try:
            response = self._client.get("/api/user/current")
            response.raise_for_status()
            return True
        except Exception:
            return False

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

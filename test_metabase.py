#!/usr/bin/env python3
"""
Quick test script to verify Metabase connection and data fetching.

Run this BEFORE creating the Metabase cards to test connection.
Run AFTER creating the cards to test full data fetching.
"""

import sys
from datetime import date, timedelta

# Add app to path
sys.path.insert(0, ".")

from app.config import get_settings
from app.metabase.client import MetabaseClient


def test_connection():
    """Test basic Metabase connection."""
    print("=" * 60)
    print("Testing Metabase Connection")
    print("=" * 60)

    settings = get_settings()
    print(f"\nMetabase URL: {settings.metabase_url}")
    print(f"API Key: {settings.metabase_api_key[:20]}...")

    client = MetabaseClient(settings.metabase_url, settings.metabase_api_key)

    print("\nTesting connection...")
    if client.test_connection():
        print("SUCCESS: Connected to Metabase!")
    else:
        print("FAILED: Could not connect to Metabase")
        return False

    client.close()
    return True


def test_existing_cards():
    """Test fetching data from existing cards (before creating new ones)."""
    print("\n" + "=" * 60)
    print("Testing Existing Cards (to verify data access)")
    print("=" * 60)

    settings = get_settings()
    client = MetabaseClient(settings.metabase_url, settings.metabase_api_key)

    # Try Card 569 - List of Sellers (no parameters needed)
    print("\nFetching Card 569 (List of Sellers)...")
    try:
        df = client.fetch_card(569)
        print(f"SUCCESS: Got {len(df)} sellers")
        print(f"Columns: {list(df.columns)}")
        if not df.empty:
            print(f"\nFirst 5 sellers:")
            print(df.head()[["Name", "Seller ID"]].to_string())
    except Exception as e:
        print(f"FAILED: {e}")

    # Try Card 648 with a seller parameter
    print("\n\nFetching Card 648 (MoM Data Parent ASIN level) with seller filter...")
    try:
        # First get a seller name
        sellers_df = client.fetch_card(569)
        if not sellers_df.empty:
            seller_name = sellers_df.iloc[0]["Name"]
            print(f"Testing with seller: {seller_name}")

            df = client.fetch_card(648, {"seller_name": seller_name})
            print(f"SUCCESS: Got {len(df)} rows")
            if not df.empty:
                print(f"Columns: {list(df.columns)}")
    except Exception as e:
        print(f"FAILED: {e}")

    client.close()


def test_new_cards():
    """Test the new pipeline cards after they're created."""
    print("\n" + "=" * 60)
    print("Testing New Pipeline Cards")
    print("=" * 60)

    settings = get_settings()

    # Check if cards are configured
    if not settings.card_sellers_list:
        print("\nCard IDs not configured yet!")
        print("Please create the cards in Metabase and update .env with the IDs:")
        print(f"  CARD_BUSINESS_REPORT={settings.card_business_report or 'NOT SET'}")
        print(f"  CARD_ADS_REPORT={settings.card_ads_report or 'NOT SET'}")
        print(f"  CARD_ASIN_MAPPING={settings.card_asin_mapping or 'NOT SET'}")
        print(f"  CARD_SELLERS_LIST={settings.card_sellers_list or 'NOT SET'}")
        print(f"  CARD_GAPS_WEEKLY={settings.card_gaps_weekly or 'NOT SET'}")
        print(f"  CARD_GAPS_MONTHLY={settings.card_gaps_monthly or 'NOT SET'}")
        return

    from app.data.fetcher import DataFetcher

    fetcher = DataFetcher()

    # Test sellers list
    print("\nTesting get_sellers()...")
    try:
        df = fetcher.get_sellers()
        print(f"SUCCESS: Got {len(df)} sellers")
        if not df.empty:
            print(df.head().to_string())
    except Exception as e:
        print(f"FAILED: {e}")

    # Test ASIN mapping
    print("\nTesting get_asin_mapping()...")
    try:
        df = fetcher.get_asin_mapping()
        print(f"SUCCESS: Got {len(df)} ASIN mappings")
    except Exception as e:
        print(f"FAILED: {e}")

    # Test business report
    print("\nTesting get_business_report()...")
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        df = fetcher.get_business_report(
            granularity="weekly",
            start_date=start_date,
            end_date=end_date,
        )
        print(f"SUCCESS: Got {len(df)} rows")
    except Exception as e:
        print(f"FAILED: {e}")

    fetcher.close()


def main():
    """Run all tests."""
    print("\nMB Onboarding - Metabase Connection Test")
    print("=" * 60)

    # Test connection first
    if not test_connection():
        print("\nConnection failed. Please check your API key and URL.")
        return

    # Test existing cards to verify access
    test_existing_cards()

    # Test new cards if configured
    test_new_cards()

    print("\n" + "=" * 60)
    print("Testing Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

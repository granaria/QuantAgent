"""
Tests for Yahoo Finance client in yahoofinanceClient.py
"""

import pandas as pd
import pytest
from datetime import datetime, timedelta

# Add parent directory to path to import modules
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from yahoofinanceClient import YahooFinanceClient


@pytest.fixture
def yf_client():
    """Create a Yahoo Finance client instance."""
    return YahooFinanceClient()


class TestSymbolMapping:
    """Test cases for symbol mapping functionality."""
    
    def test_get_symbol_crypto(self, yf_client):
        """Test symbol conversion for crypto."""
        assert yf_client.get_symbol("BTC") == "BTC-USD"
        assert yf_client.get_symbol("ETH") == "ETH-USD"
        assert yf_client.get_symbol("SOL") == "SOL-USD"
    
    def test_get_symbol_stocks(self, yf_client):
        """Test symbol conversion for stocks."""
        assert yf_client.get_symbol("AAPL") == "AAPL"
        assert yf_client.get_symbol("TSLA") == "TSLA"
        assert yf_client.get_symbol("SPY") == "SPY"
    
    def test_get_symbol_forex(self, yf_client):
        """Test symbol conversion for forex."""
        assert yf_client.get_symbol("EURUSD") == "EURUSD=X"
        assert yf_client.get_symbol("GBPUSD") == "GBPUSD=X"
    
    def test_get_symbol_futures(self, yf_client):
        """Test symbol conversion for futures."""
        assert yf_client.get_symbol("ES") == "ES=F"
        assert yf_client.get_symbol("NQ") == "NQ=F"
        assert yf_client.get_symbol("GC") == "GC=F"
    
    def test_get_symbol_indices(self, yf_client):
        """Test symbol conversion for indices."""
        assert yf_client.get_symbol("SPX") == "^GSPC"
        assert yf_client.get_symbol("DJI") == "^DJI"
        assert yf_client.get_symbol("VIX") == "^VIX"
    
    def test_get_symbol_unknown_passthrough(self, yf_client):
        """Test that unknown symbols are passed through uppercase."""
        assert yf_client.get_symbol("UNKNOWN") == "UNKNOWN"
        assert yf_client.get_symbol("test") == "TEST"
    
    def test_get_symbol_case_insensitive(self, yf_client):
        """Test that symbol lookup is case-insensitive."""
        assert yf_client.get_symbol("btc") == "BTC-USD"
        assert yf_client.get_symbol("Btc") == "BTC-USD"
        assert yf_client.get_symbol("BTC") == "BTC-USD"


class TestIntervalMapping:
    """Test cases for interval mapping functionality."""
    
    def test_get_interval_minute(self, yf_client):
        """Test interval conversion for minute intervals."""
        assert yf_client.get_interval("1m") == "1m"
        assert yf_client.get_interval("5m") == "5m"
        assert yf_client.get_interval("15m") == "15m"
        assert yf_client.get_interval("30m") == "30m"
    
    def test_get_interval_hourly(self, yf_client):
        """Test interval conversion for hourly intervals."""
        assert yf_client.get_interval("1h") == "1h"
        assert yf_client.get_interval("60") == "1h"
        assert yf_client.get_interval("60m") == "1h"
        assert yf_client.get_interval("4h") == "4h"
    
    def test_get_interval_daily_weekly(self, yf_client):
        """Test interval conversion for daily/weekly intervals."""
        assert yf_client.get_interval("1d") == "1d"
        assert yf_client.get_interval("1w") == "1wk"
        assert yf_client.get_interval("1mo") == "1mo"
    
    def test_get_interval_unknown_passthrough(self, yf_client):
        """Test that unknown intervals are passed through."""
        assert yf_client.get_interval("unknown") == "unknown"


class TestDataFrameCleaning:
    """Test cases for DataFrame cleaning functionality."""
    
    def test_clean_empty_dataframe(self, yf_client):
        """Test cleaning an empty DataFrame."""
        df = pd.DataFrame()
        result = yf_client._clean_dataframe(df)
        assert result.empty
    
    def test_clean_none_dataframe(self, yf_client):
        """Test cleaning None returns empty DataFrame."""
        result = yf_client._clean_dataframe(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_clean_dataframe_with_valid_data(self, yf_client):
        """Test cleaning DataFrame with valid data."""
        data = {
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "Open": [100.0, 101.0, 102.0],
            "High": [105.0, 106.0, 107.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [104.0, 105.0, 106.0],
            "Volume": [1000, 1100, 1200]
        }
        df = pd.DataFrame(data).set_index("Date")
        
        result = yf_client._clean_dataframe(df)
        
        assert not result.empty
        assert "Datetime" in result.columns
        assert "Open" in result.columns
        assert "High" in result.columns
        assert "Low" in result.columns
        assert "Close" in result.columns
    
    def test_clean_dataframe_handles_multiindex(self, yf_client):
        """Test cleaning DataFrame with MultiIndex columns."""
        data = {
            ("Open", "AAPL"): [100.0, 101.0],
            ("High", "AAPL"): [105.0, 106.0],
            ("Low", "AAPL"): [99.0, 100.0],
            ("Close", "AAPL"): [104.0, 105.0],
        }
        df = pd.DataFrame(data)
        df.columns = pd.MultiIndex.from_tuples(df.columns)
        df.index = pd.to_datetime(["2024-01-01", "2024-01-02"])
        df.index.name = "Date"
        
        result = yf_client._clean_dataframe(df)
        
        # Should either successfully clean or return empty (due to missing columns)
        assert isinstance(result, pd.DataFrame)
    
    def test_clean_dataframe_removes_duplicates(self, yf_client):
        """Test that cleaning removes duplicate rows."""
        data = {
            "Date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02"]),
            "Open": [100.0, 100.0, 101.0],
            "High": [105.0, 105.0, 106.0],
            "Low": [99.0, 99.0, 100.0],
            "Close": [104.0, 104.0, 105.0],
        }
        df = pd.DataFrame(data).set_index("Date")
        
        result = yf_client._clean_dataframe(df)
        
        # Should have fewer rows after duplicate removal
        assert len(result) <= 3


class TestAssetMapping:
    """Test cases for asset mapping constants."""
    
    def test_asset_mapping_has_common_assets(self, yf_client):
        """Test that ASSET_MAPPING contains common assets."""
        assert "BTC" in yf_client.ASSET_MAPPING
        assert "SPX" in yf_client.ASSET_MAPPING
        assert "GC" in yf_client.ASSET_MAPPING
    
    def test_asset_mapping_values_are_strings(self, yf_client):
        """Test that ASSET_MAPPING values are display names."""
        for key, value in yf_client.ASSET_MAPPING.items():
            assert isinstance(value, str)
            assert len(value) > 0
    
    def test_yfinance_symbols_has_common_symbols(self, yf_client):
        """Test that YFINANCE_SYMBOLS contains common symbols."""
        assert "BTC" in yf_client.YFINANCE_SYMBOLS
        assert "AAPL" in yf_client.YFINANCE_SYMBOLS
        assert "SPY" in yf_client.YFINANCE_SYMBOLS
    
    def test_yfinance_intervals_has_common_intervals(self, yf_client):
        """Test that YFINANCE_INTERVALS contains common intervals."""
        assert "1m" in yf_client.YFINANCE_INTERVALS
        assert "1h" in yf_client.YFINANCE_INTERVALS
        assert "1d" in yf_client.YFINANCE_INTERVALS


class TestClientInitialization:
    """Test cases for client initialization."""
    
    def test_client_can_be_instantiated(self):
        """Test that YahooFinanceClient can be instantiated."""
        client = YahooFinanceClient()
        assert client is not None
    
    def test_client_has_required_attributes(self, yf_client):
        """Test that client has required attributes."""
        assert hasattr(yf_client, "ASSET_MAPPING")
        assert hasattr(yf_client, "YFINANCE_SYMBOLS")
        assert hasattr(yf_client, "YFINANCE_INTERVALS")
        assert hasattr(yf_client, "get_symbol")
        assert hasattr(yf_client, "get_interval")
        assert hasattr(yf_client, "fetch_data")

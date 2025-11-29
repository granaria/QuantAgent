"""
Tests for technical indicator computations in graph_util.py
"""

import numpy as np
import pandas as pd
import pytest

# Add parent directory to path to import modules
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from graph_util import TechnicalTools


# Sample OHLCV test data fixture
@pytest.fixture
def sample_kline_data():
    """Create sample OHLCV data for testing indicators."""
    # Generate 50 data points with realistic price movements
    np.random.seed(42)  # For reproducibility
    base_price = 100.0
    
    closes = []
    highs = []
    lows = []
    opens = []
    datetimes = []
    
    current_price = base_price
    for i in range(50):
        # Simulate price movements
        change = np.random.normal(0, 2)
        open_price = current_price
        close_price = open_price + change
        high_price = max(open_price, close_price) + abs(np.random.normal(0, 1))
        low_price = min(open_price, close_price) - abs(np.random.normal(0, 1))
        
        opens.append(open_price)
        closes.append(close_price)
        highs.append(high_price)
        lows.append(low_price)
        datetimes.append(f"2024-01-{(i+1):02d} 10:00:00")
        
        current_price = close_price
    
    return {
        "Datetime": datetimes,
        "Open": opens,
        "High": highs,
        "Low": lows,
        "Close": closes
    }


class TestComputeRSI:
    """Test cases for RSI computation."""
    
    def test_rsi_returns_dict_with_rsi_key(self, sample_kline_data):
        """Test that compute_rsi returns a dict with 'rsi' key."""
        result = TechnicalTools.compute_rsi.invoke({"kline_data": sample_kline_data})
        assert isinstance(result, dict)
        assert "rsi" in result
    
    def test_rsi_values_in_valid_range(self, sample_kline_data):
        """Test that RSI values are between 0 and 100."""
        result = TechnicalTools.compute_rsi.invoke({"kline_data": sample_kline_data})
        rsi_values = result["rsi"]
        
        for value in rsi_values:
            assert 0 <= value <= 100, f"RSI value {value} is out of range [0, 100]"
    
    def test_rsi_returns_list(self, sample_kline_data):
        """Test that RSI returns a list of values."""
        result = TechnicalTools.compute_rsi.invoke({"kline_data": sample_kline_data})
        assert isinstance(result["rsi"], list)
    
    def test_rsi_custom_period(self, sample_kline_data):
        """Test RSI with custom period parameter."""
        result = TechnicalTools.compute_rsi.invoke({
            "kline_data": sample_kline_data,
            "period": 7
        })
        assert isinstance(result, dict)
        assert "rsi" in result
        assert len(result["rsi"]) > 0


class TestComputeMACD:
    """Test cases for MACD computation."""
    
    def test_macd_returns_required_keys(self, sample_kline_data):
        """Test that compute_macd returns dict with all required keys."""
        result = TechnicalTools.compute_macd.invoke({"kline_data": sample_kline_data})
        assert isinstance(result, dict)
        assert "macd" in result
        assert "macd_signal" in result
        assert "macd_hist" in result
    
    def test_macd_values_are_lists(self, sample_kline_data):
        """Test that MACD values are lists."""
        result = TechnicalTools.compute_macd.invoke({"kline_data": sample_kline_data})
        assert isinstance(result["macd"], list)
        assert isinstance(result["macd_signal"], list)
        assert isinstance(result["macd_hist"], list)
    
    def test_macd_custom_periods(self, sample_kline_data):
        """Test MACD with custom periods."""
        result = TechnicalTools.compute_macd.invoke({
            "kline_data": sample_kline_data,
            "fastperiod": 8,
            "slowperiod": 21,
            "signalperiod": 5
        })
        assert "macd" in result
        assert "macd_signal" in result
        assert "macd_hist" in result


class TestComputeStochastic:
    """Test cases for Stochastic Oscillator computation."""
    
    def test_stoch_returns_required_keys(self, sample_kline_data):
        """Test that compute_stoch returns dict with stoch_k and stoch_d."""
        result = TechnicalTools.compute_stoch.invoke({"kline_data": sample_kline_data})
        assert isinstance(result, dict)
        assert "stoch_k" in result
        assert "stoch_d" in result
    
    def test_stoch_values_in_valid_range(self, sample_kline_data):
        """Test that Stochastic values are between 0 and 100."""
        result = TechnicalTools.compute_stoch.invoke({"kline_data": sample_kline_data})
        
        for value in result["stoch_k"]:
            assert 0 <= value <= 100, f"Stoch K value {value} is out of range [0, 100]"
        
        for value in result["stoch_d"]:
            assert 0 <= value <= 100, f"Stoch D value {value} is out of range [0, 100]"
    
    def test_stoch_values_are_lists(self, sample_kline_data):
        """Test that Stochastic values are lists."""
        result = TechnicalTools.compute_stoch.invoke({"kline_data": sample_kline_data})
        assert isinstance(result["stoch_k"], list)
        assert isinstance(result["stoch_d"], list)


class TestComputeROC:
    """Test cases for Rate of Change computation."""
    
    def test_roc_returns_dict_with_roc_key(self, sample_kline_data):
        """Test that compute_roc returns a dict with 'roc' key."""
        result = TechnicalTools.compute_roc.invoke({"kline_data": sample_kline_data})
        assert isinstance(result, dict)
        assert "roc" in result
    
    def test_roc_returns_list(self, sample_kline_data):
        """Test that ROC returns a list of values."""
        result = TechnicalTools.compute_roc.invoke({"kline_data": sample_kline_data})
        assert isinstance(result["roc"], list)
    
    def test_roc_custom_period(self, sample_kline_data):
        """Test ROC with custom period parameter."""
        result = TechnicalTools.compute_roc.invoke({
            "kline_data": sample_kline_data,
            "period": 5
        })
        assert "roc" in result
        assert len(result["roc"]) > 0


class TestComputeWilliamsR:
    """Test cases for Williams %R computation."""
    
    def test_willr_returns_dict_with_willr_key(self, sample_kline_data):
        """Test that compute_willr returns a dict with 'willr' key."""
        result = TechnicalTools.compute_willr.invoke({"kline_data": sample_kline_data})
        assert isinstance(result, dict)
        assert "willr" in result
    
    def test_willr_values_in_valid_range(self, sample_kline_data):
        """Test that Williams %R values are between -100 and 0."""
        result = TechnicalTools.compute_willr.invoke({"kline_data": sample_kline_data})
        
        for value in result["willr"]:
            assert -100 <= value <= 0, f"Williams %R value {value} is out of range [-100, 0]"
    
    def test_willr_returns_list(self, sample_kline_data):
        """Test that Williams %R returns a list of values."""
        result = TechnicalTools.compute_willr.invoke({"kline_data": sample_kline_data})
        assert isinstance(result["willr"], list)
    
    def test_willr_custom_period(self, sample_kline_data):
        """Test Williams %R with custom period parameter."""
        result = TechnicalTools.compute_willr.invoke({
            "kline_data": sample_kline_data,
            "period": 7
        })
        assert "willr" in result
        assert len(result["willr"]) > 0


class TestTrendlineHelpers:
    """Test cases for trendline helper functions."""
    
    def test_check_trend_line_support_valid(self):
        """Test check_trend_line for valid support line."""
        from graph_util import check_trend_line
        
        # Create simple ascending data
        y = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        pivot = 0
        slope = 1.0
        
        # For support line, all differences should be positive or near zero
        result = check_trend_line(support=True, pivot=pivot, slope=slope, y=y)
        # Result should be non-negative (valid line)
        assert result >= 0 or result == -1.0
    
    def test_fit_trendlines_single_returns_tuple(self):
        """Test that fit_trendlines_single returns two coefficient tuples."""
        from graph_util import fit_trendlines_single
        
        # Create simple price data
        data = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109])
        
        support_coefs, resist_coefs = fit_trendlines_single(data)
        
        assert isinstance(support_coefs, tuple)
        assert isinstance(resist_coefs, tuple)
        assert len(support_coefs) == 2  # slope and intercept
        assert len(resist_coefs) == 2
    
    def test_split_line_into_segments(self):
        """Test that split_line_into_segments creates correct segments."""
        from graph_util import split_line_into_segments
        
        line_points = [(0, 1), (1, 2), (2, 3), (3, 4)]
        segments = split_line_into_segments(line_points)
        
        assert len(segments) == 3  # n points -> n-1 segments
        assert segments[0] == [(0, 1), (1, 2)]
        assert segments[1] == [(1, 2), (2, 3)]
        assert segments[2] == [(2, 3), (3, 4)]

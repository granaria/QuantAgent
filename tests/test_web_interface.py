"""
Tests for web interface functionality in web_interface.py
"""

import pytest
from datetime import datetime

# Add parent directory to path to import modules
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDateRangeValidation:
    """Test cases for date range validation logic."""
    
    def test_validate_date_range_valid(self):
        """Test validation with valid date range."""
        from web_interface import analyzer
        
        result = analyzer.validate_date_range(
            start_date="2024-01-01",
            end_date="2024-01-05",
            timeframe="1h"
        )
        
        assert result["valid"] == True
        assert "days" in result
    
    def test_validate_date_range_start_after_end(self):
        """Test validation fails when start is after end."""
        from web_interface import analyzer
        
        result = analyzer.validate_date_range(
            start_date="2024-01-10",
            end_date="2024-01-05",
            timeframe="1h"
        )
        
        assert result["valid"] == False
        assert "error" in result
    
    def test_validate_date_range_with_time(self):
        """Test validation with time parameters."""
        from web_interface import analyzer
        
        result = analyzer.validate_date_range(
            start_date="2024-01-01",
            end_date="2024-01-01",
            timeframe="1h",
            start_time="09:00",
            end_time="17:00"
        )
        
        assert result["valid"] == True
    
    def test_validate_date_range_1m_max_7_days(self):
        """Test 1-minute timeframe has 7 day limit."""
        from web_interface import analyzer
        
        # Test date range exceeding 7 days for 1m timeframe
        result = analyzer.validate_date_range(
            start_date="2024-01-01",
            end_date="2024-01-15",  # 14 days
            timeframe="1m"
        )
        
        assert result["valid"] == False
        assert "max_days" in result
        assert result["max_days"] == 7
    
    def test_validate_date_range_invalid_format(self):
        """Test validation fails with invalid date format."""
        from web_interface import analyzer
        
        result = analyzer.validate_date_range(
            start_date="invalid-date",
            end_date="2024-01-05",
            timeframe="1h"
        )
        
        assert result["valid"] == False
        assert "error" in result


class TestTimeframeLimits:
    """Test cases for timeframe date limits."""
    
    def test_get_timeframe_date_limits_1m(self):
        """Test 1-minute timeframe has correct limits."""
        from web_interface import analyzer
        
        limits = analyzer.get_timeframe_date_limits("1m")
        
        assert limits["max_days"] == 7
        assert "description" in limits
    
    def test_get_timeframe_date_limits_1h(self):
        """Test 1-hour timeframe has correct limits."""
        from web_interface import analyzer
        
        limits = analyzer.get_timeframe_date_limits("1h")
        
        assert limits["max_days"] == 730
    
    def test_get_timeframe_date_limits_1d(self):
        """Test daily timeframe has correct limits."""
        from web_interface import analyzer
        
        limits = analyzer.get_timeframe_date_limits("1d")
        
        assert limits["max_days"] == 730
    
    def test_get_timeframe_date_limits_unknown(self):
        """Test unknown timeframe returns default limits."""
        from web_interface import analyzer
        
        limits = analyzer.get_timeframe_date_limits("unknown")
        
        assert limits["max_days"] == 730  # default


class TestAnalyzerAssets:
    """Test cases for analyzer asset management."""
    
    def test_get_available_assets_returns_list(self):
        """Test get_available_assets returns a sorted list."""
        from web_interface import analyzer
        
        assets = analyzer.get_available_assets()
        
        assert isinstance(assets, list)
        assert len(assets) > 0
        # Check it's sorted
        assert assets == sorted(assets)
    
    def test_asset_mapping_contains_expected_assets(self):
        """Test asset mapping contains expected assets."""
        from web_interface import analyzer
        
        assert "BTC" in analyzer.asset_mapping
        assert "SPX" in analyzer.asset_mapping
    
    def test_yfinance_symbols_available(self):
        """Test yfinance symbols mapping is available."""
        from web_interface import analyzer
        
        assert isinstance(analyzer.yfinance_symbols, dict)
        assert len(analyzer.yfinance_symbols) > 0
    
    def test_yfinance_intervals_available(self):
        """Test yfinance intervals mapping is available."""
        from web_interface import analyzer
        
        assert isinstance(analyzer.yfinance_intervals, dict)
        assert "1h" in analyzer.yfinance_intervals


class TestAnalyzerConfig:
    """Test cases for analyzer configuration."""
    
    def test_analyzer_has_config(self):
        """Test analyzer has config attribute."""
        from web_interface import analyzer
        
        assert hasattr(analyzer, "config")
        assert isinstance(analyzer.config, dict)
    
    def test_config_has_required_keys(self):
        """Test config has required configuration keys."""
        from web_interface import analyzer
        
        assert "agent_llm_model" in analyzer.config
        assert "graph_llm_model" in analyzer.config
        assert "agent_llm_temperature" in analyzer.config
        assert "graph_llm_temperature" in analyzer.config


class TestExtractAnalysisResults:
    """Test cases for analysis results extraction."""
    
    def test_extract_error_result(self):
        """Test extracting error results."""
        from web_interface import analyzer
        
        error_results = {
            "success": False,
            "error": "Test error message"
        }
        
        extracted = analyzer.extract_analysis_results(error_results)
        
        assert "error" in extracted
        assert extracted["error"] == "Test error message"
    
    def test_extract_success_result_structure(self):
        """Test extracting successful results has correct structure."""
        from web_interface import analyzer
        
        success_results = {
            "success": True,
            "asset_name": "BTC",
            "timeframe": "1hour",
            "data_length": 45,
            "final_state": {
                "indicator_report": "Test indicator report",
                "pattern_report": "Test pattern report",
                "trend_report": "Test trend report",
                "final_trade_decision": '{"decision": "LONG", "risk_reward_ratio": "1.5", "forecast_horizon": "4 hours", "justification": "Test"}',
                "pattern_image": "test_base64",
                "trend_image": "test_base64"
            }
        }
        
        extracted = analyzer.extract_analysis_results(success_results)
        
        assert "success" in extracted
        assert extracted["success"] == True
        assert "asset_name" in extracted
        assert "timeframe" in extracted
        assert "technical_indicators" in extracted
        assert "pattern_analysis" in extracted
        assert "trend_analysis" in extracted
        assert "final_decision" in extracted


class TestCustomAssets:
    """Test cases for custom asset management."""
    
    def test_load_custom_assets_returns_list(self):
        """Test load_custom_assets returns a list."""
        from web_interface import analyzer
        
        result = analyzer.load_custom_assets()
        
        assert isinstance(result, list)

# yfinance_client.py
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import logging
import requests  # For Polygon fallback
from functools import lru_cache  # Simple caching
import os


class YahooFinanceClient:
    """
    Robust Yahoo Finance data client with crypto, stocks, forex & futures support.
    Designed to work flawlessly with granariaTWO and similar trading dashboards.
    """
    # Available assets and their display names
    ASSET_MAPPING = {
        "SPX": "S&P 500",
        "BTC": "Bitcoin",
        "GC": "Gold Futures",
        "NQ": "Nasdaq Futures",
        "CL": "Crude Oil",
        "ES": "E-mini S&P 500",
        "DJI": "Dow Jones",
        "QQQ": "Invesco QQQ Trust",
        "VIX": "Volatility Index",
        "DXY": "US Dollar Index",
        "AAPL": "Apple Inc.",  # New asset
        "TSLA": "Tesla Inc.",  # New asset
    }
    # Common symbol overrides (Yahoo uses different tickers sometimes)

    YFINANCE_SYMBOLS = {
        # Crypto
        "BTC": "BTC-USD",
        "ETH": "ETH-USD",
        "BNB": "BNB-USD",
        "SOL": "SOL-USD",
        "XRP": "XRP-USD",
        "ADA": "ADA-USD",
        "DOGE": "DOGE-USD",
        "AVAX": "AVAX-USD",
        "LINK": "LINK-USD",
        "DOT": "DOT-USD",

        # Popular stocks
        "SPY": "SPY",
        "QQQ": "QQQ",
        "AAPL": "AAPL",
        "TSLA": "TSLA",
        "NVDA": "NVDA",

        # Forex (Yahoo format)
        "EURUSD": "EURUSD=X",
        "GBPUSD": "GBPUSD=X",
        "USDJPY": "USDJPY=X",

        # Futures
        "ES": "ES=F",  # S&P 500 futures
        "NQ": "NQ=F",  # Nasdaq futures
        "GC": "GC=F",  # Gold
        "CL": "CL=F",  # Crude Oil
        "BTCF": "BTC=F",  # Bitcoin CME Futures

        "SPX": "^GSPC",  # S&P 500
        "DJI": "^DJI",  # Dow Jones
        "VIX": "^VIX",  # Volatility Index
        "DXY": "DX-Y.NYB",  # US Dollar Index
    }

    # Supported intervals and their Yahoo equivalents
    YFINANCE_INTERVALS = {
        "1": "1m",
        "5": "5m",
        "15": "15m",
        "30": "30m",
        "60": "1h",
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "60m": "1h",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
        "1w": "1wk",
        "1mo": "1mo",
    }
    '''
    self.yfinance_intervals = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",  # yfinance supports 4h natively!
        "1d": "1d",
        "1w": "1wk",
        "1mo": "1mo",
    }
    '''

    def __init__(self, polygon_api_key: str = None):
        self.polygon_key = polygon_api_key or os.environ.get('POLYGON_API_KEY')
        self.cache = {}  # Simple dict cache (use Redis for prod)
        # logging.getLogger("yfinance").setLevel(logging.ERROR)

    def get_symbol(self, symbol: str) -> str:
        """Return correct Yahoo Finance ticker."""
        return self.YFINANCE_SYMBOLS.get(symbol.upper(), symbol.upper())

    '''
    def get_interval(self, interval: str) -> str:
        """Normalize interval to yfinance format."""
        return self.YFINANCE_INTERVALS.get(interval, interval)
    '''

    def fetch_yfinance_data(
            self, symbol: str, interval: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Fetch OHLCV data from Yahoo Finance."""
        try:
            yf_symbol = self.YFINANCE_SYMBOLS  # self.yfinance_symbols.get(symbol, symbol)
            yf_interval = self.YFINANCE_INTERVALS  # self.yfinance_intervals.get(interval, interval)

            df = yf.download(
                tickers=yf_symbol, start=start_date, end=end_date, interval=yf_interval
            )

            if df is None or df.empty:
                return pd.DataFrame()

            # Ensure df is a DataFrame, not a Series
            if isinstance(df, pd.Series):
                df = df.to_frame()

            # Reset index to ensure we have a clean DataFrame
            df = df.reset_index()

            # Ensure we have a DataFrame
            if not isinstance(df, pd.DataFrame):
                return pd.DataFrame()

            # Handle potential MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Rename columns if needed
            column_mapping = {
                "Date": "Datetime",
                "Open": "Open",
                "High": "High",
                "Low": "Low",
                "Close": "Close",
                "Volume": "Volume",
            }

            # Only rename columns that exist
            existing_columns = {
                old: new for old, new in column_mapping.items() if old in df.columns
            }
            df = df.rename(columns=existing_columns)

            # Ensure we have the required columns
            required_columns = ["Datetime", "Open", "High", "Low", "Close"]
            if not all(col in df.columns for col in required_columns):
                print(f"Warning: Missing columns. Available: {list(df.columns)}")
                return pd.DataFrame()

            # Select only the required columns
            df = df[required_columns]
            df["Datetime"] = pd.to_datetime(df["Datetime"])

            return df

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    @lru_cache(maxsize=128)
    def fetch_data(
            self,
            symbol: str,
            interval: str = "1h",
            period: str = None,
            start_date: str = None,
            end_date: str = None,
            lookback_days: int = None,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data from Yahoo Finance.

        Args:
            symbol: Asset ticker (e.g., "BTC", "AAPL", "EURUSD")
            interval: "1m", "5m", "15m", "1h", "1d", etc.
            period: "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
            start_date: "YYYY-MM-DD"
            end_date: "YYYY-MM-DD"
            lookback_days: Alternative to period/start-end

        Returns:
            Clean DataFrame with Datetime, Open, High, Low, Close, Volume
        """
        ticker = self.YFINANCE_SYMBOLS.get(symbol, symbol)  # self.get_symbol(symbol)
        yf_interval = self.YFINANCE_INTERVALS.get(interval, interval)  # self.get_interval(interval)

        # Validate interval (1m only allowed for last 7 days)
        if yf_interval == "1m":
            if period not in ["1d", "5d", "7d"] and not lookback_days:
                lookback_days = 7

        try:
            # Build date range
            if period:
                df = yf.download(ticker, period=period, interval=yf_interval, progress=False)
            elif start_date or end_date or lookback_days:
                if lookback_days:
                    start = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                    end = datetime.now().strftime("%Y-%m-%d")
                else:
                    start = start_date
                    end = end_date or datetime.now().strftime("%Y-%m-%d")

                df = yf.download(ticker, start=start, end=end, interval=yf_interval, progress=False)
            else:
                # Default: last 30 days
                df = yf.download(ticker, period="30d", interval=yf_interval, progress=False)

            return self._clean_dataframe(df)

        except Exception as e:
            logging.error(f"yfinance error for {ticker} {interval}: {e}")
            return pd.DataFrame()

    @lru_cache(maxsize=128)
    def fetch_data2(self, symbol: str, interval: str = "1h", lookback_days: int = 30,
                    period: str = None) -> pd.DataFrame:
        """Fetch OHLCV data from Yahoo with Polygon fallback."""
        try:
            # Try Yahoo first
            yf_symbol = self.YFINANCE_SYMBOLS.get(symbol, symbol)
            yf_interval = self.YFINANCE_INTERVALS.get(interval, interval)

            if period:
                df = yf.download(tickers=yf_symbol, period=period, interval=yf_interval)
            else:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=lookback_days)
                df = yf.download(tickers=yf_symbol, start=start_date, end=end_date, interval=yf_interval)

            if df.empty:
                raise ValueError("Yahoo data empty")

           ....
            return df
        except Exception as e:
            logging.warning(f"Yahoo fetch failed for {symbol}: {e}. Falling back to Polygon.")
            return self._fetch_polygon_fallback(symbol, interval, lookback_days)

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize and clean yfinance output."""
        if df is None or df.empty:
            return pd.DataFrame()

        # Handle MultiIndex columns (happens when downloading multiple tickers)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1) if df.columns.nlevels > 1 else df.columns

        # Reset index to get Date/Datetime as column
        df = df.reset_index()

        # Rename columns safely
        column_map = {
            "Date": "Datetime",
            "Datetime": "Datetime",  # For intraday
            "Open": "Open",
            "High": "High",
            "Low": "Low",
            "Close": "Close",
            "Adj Close": "Close",  # Prefer Adj Close for stocks
            "Volume": "Volume",
        }
        #----
        # Clean as before (rename columns, handle Adj Close, etc.)
        # column_map = {"Open": "Open", "High": "High", "Low": "Low", "Close": "Close", "Volume": "Volume"}
        # df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})
        # if "Adj Close" in df.columns and "Close" not in df.columns:
        #    df["Close"] = df["Adj Close"]
        # required = ["Datetime", "Open", "High", "Low", "Close"]
        # df = df[required + ["Volume"] if "Volume" in df.columns else required]
        # df["Datetime"] = pd.to_datetime(df.index).tz_localize(None)  # Fix index to column
        df = df.reset_index(drop=True)
        self.cache[f"{symbol}_{interval}_{lookback_days}"] = df.to_dict('records')
        #----




        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Use Adj Close if available (better for stocks/dividends)
        if "Adj Close" in df.columns and "Close" not in df.columns:
            df["Close"] = df["Adj Close"]

        # Ensure required columns
        required = ["Datetime", "Open", "High", "Low", "Close"]
        if not all(col in df.columns for col in required):
            return pd.DataFrame()

        df = df[required + ["Volume"] if "Volume" in df.columns else required]

        # Clean timestamps
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        # df["Datetime"] = pd.to_datetime(df.index).tz_localize(None)  # Fix index to column

        # Remove timezone (Plotly/Matplotlib prefer naive datetime)
        if df["Datetime"].dt.tz is not None:
            df["Datetime"] = df["Datetime"].dt.tz_localize(None)

        # Remove duplicates and sort
        df = df.drop_duplicates(subset=["Datetime"]).sort_values("Datetime").reset_index(drop=True)

        # Forward fill small gaps (optional)
        #        df = df.set_index("Datetime").resample("1min").ffill().reset_index()

        return df

    def get_price(self, symbol: str) -> Optional[float]:
        """Get current/last price."""
        try:
            ticker = yf.Ticker(self.get_symbol(symbol))
            data = ticker.history(period="1d")
            return data["Close"].iloc[-1] if not data.empty else None
        except:
            return None

    def get_info(self, symbol: str) -> Dict:
        """Get metadata (name, market, etc.)"""
        try:
            ticker = yf.Ticker(self.get_symbol(symbol))
            return ticker.info
        except:
            return {}

    def _fetch_polygon_fallback(self, symbol: str, interval: str, lookback_days: int) -> pd.DataFrame:
        """Fallback to Polygon API (assumes key is configured)."""
        if not self.polygon_key:
            raise ValueError("Polygon API key required for fallback.")

        # Map intervals (Polygon uses different enums)
        poly_intervals = {"1m": "minute", "5m": "minute", "1h": "hour", "1d": "day"}
        poly_interval = poly_intervals.get(interval, "hour")

        end = datetime.now().isoformat()
        start = (datetime.now() - timedelta(days=lookback_days)).isoformat()

        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{poly_interval}/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={self.polygon_key}"
        resp = requests.get(url)
        data = resp.json().get('results', [])

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['Datetime'] = pd.to_datetime(df['t'], unit='ms').dt.tz_localize(None)
        df = df.rename(columns={'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'})
        df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
        return df


# Global instance (recommended)
yf_client = YahooFinanceClient()

# Example usage
if __name__ == "__main__":
    client = YahooFinanceClient()

    # Crypto 1h
    df = client.fetch_data("BTC", interval="1h", lookback_days=30)
    print(df.tail())

    # Stock daily
    df2 = client.fetch_data("AAPL", interval="1d", period="1y")
    print(df2.tail())

    # Current price
    print("BTC Price:", client.get_price("BTC"))

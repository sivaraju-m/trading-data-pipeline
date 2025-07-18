"""
KiteConnect Integration for Live Trading Data and Execution
==========================================================

This module provides comprehensive integration with Zerodha's KiteConnect API
for real-time data ingestion and live trade execution in the AI Trading Machine.

Features:
- Real-time market data streaming
- Historical data fetching with proper formatting
- Live order placement and management
- Portfolio and position tracking
- SEBI-compliant trade execution

Dependencies:
- kiteconnect: pip install kiteconnect
- Configure API credentials in secrets management

Author: AI Trading Machine
Licensed by SJ Trading
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    # Find the project root and load .env file
    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    env_path = os.path.join(project_root, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        logging.info("Loaded environment variables from {env_path}")
except ImportError:
    logging.warning(
        "python-dotenv not available. Environment variables from shell only."
    )

try:
    from kiteconnect import KiteConnect, KiteTicker

    KITE_AVAILABLE = True
except ImportError:
    KITE_AVAILABLE = False
    logging.warning("KiteConnect not installed. Install with: pip install kiteconnect")

from ..utils.logger import setup_logger

# Import GCP Secret Manager (with fallback to environment variables)
try:
    from ..utils.gcp_secrets import (
        get_kite_access_token,
        get_kite_api_key,
        get_kite_api_secret,
    )

    SECRETS_MANAGER_AVAILABLE = True
except ImportError:
    SECRETS_MANAGER_AVAILABLE = False
    logging.info("GCP Secret Manager not available, using environment variables")

logger = setup_logger(__name__)


class KiteDataLoader:
    """
    Zerodha KiteConnect integration for live trading data and execution.

    This class handles:
    1. Authentication and session management
    2. Real-time and historical data fetching
    3. Order placement and portfolio management
    4. SEBI-compliant trade execution
    """

    def __init__(self, api_key: str = None, access_token: str = None):
        """
        Initialize KiteConnect integration.

        Args:
            api_key: Zerodha API key (optional, can be set via environment)
            access_token: Valid access token (optional, can be set via environment)
        """
        if not KITE_AVAILABLE:
            raise ImportError(
                "KiteConnect not available. Install with: pip install kiteconnect"
            )

        # Load credentials from Secret Manager first, then environment, then parameters
        if SECRETS_MANAGER_AVAILABLE:
            self.api_key = api_key or get_kite_api_key() or os.getenv("KITE_API_KEY")
            self.access_token = (
                access_token
                or get_kite_access_token()
                or os.getenv("KITE_ACCESS_TOKEN")
            )
        else:
            # Fallback to environment variables only
            self.api_key = api_key or os.getenv("KITE_API_KEY")
            self.access_token = access_token or os.getenv("KITE_ACCESS_TOKEN")

        if not self.api_key:
            raise ValueError(
                "KITE_API_KEY must be provided via Secret Manager, environment, or parameter"
            )

        # Initialize KiteConnect
        self.kite = KiteConnect(api_key=self.api_key)

        if self.access_token:
            self.kite.set_access_token(self.access_token)
            logger.info("KiteConnect initialized with access token")
        else:
            logger.warning("No access token provided. Call authenticate() first.")

        # Initialize WebSocket for real-time data
        self.kws = None
        self.is_authenticated = bool(self.access_token)

        # Cache for instrument tokens and symbols
        self._instruments_cache = {}
        self._symbol_to_token = {}
        self._token_to_symbol = {}

        # Real-time data storage
        self.live_data = {}
        self.subscribed_tokens = set()

    def authenticate(self, request_token: str = None) -> str:
        """
        Complete the authentication process.

        Args:
            request_token: Request token from Kite login flow

        Returns:
            access_token: Generated access token for API calls

        Note:
            For live trading, you need to implement the full OAuth flow.
            See: https://kite.trade/docs/connect/v3/user/#login-flow
        """
        if not request_token:
            login_url = self.kite.login_url()
            logger.info("Please visit: {login_url}")
            logger.info(
                "After login, copy the request_token from URL and call authenticate(request_token)"
            )
            return None

        try:
            # Generate access token
            # Get API secret from Secret Manager or environment
            if SECRETS_MANAGER_AVAILABLE:
                api_secret = get_kite_api_secret() or os.getenv("KITE_API_SECRET")
            else:
                api_secret = os.getenv("KITE_API_SECRET")

            data = self.kite.generate_session(request_token, api_secret=api_secret)
            self.access_token = data["access_token"]
            self.kite.set_access_token(self.access_token)
            self.is_authenticated = True

            logger.info("Authentication successful!")
            logger.info("Access Token: {self.access_token}")
            logger.info("Store this token securely for future use")

            return self.access_token

        except Exception as e:
            logger.error("Authentication failed: {e}")
            raise

    def load_instruments(self, exchange: str = "NSE") -> pd.DataFrame:
        """
        Load and cache instrument data for symbol-token mapping.

        Args:
            exchange: Exchange name (NSE, BSE, etc.)

        Returns:
            DataFrame with instrument details
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        try:
            instruments = self.kite.instruments(exchange)
            df = pd.DataFrame(instruments)

            # Build mapping caches
            for _, row in df.iterrows():
                symbol = row["tradingsymbol"]
                token = row["instrument_token"]

                self._symbol_to_token[symbol] = token
                self._token_to_symbol[token] = symbol

            self._instruments_cache[exchange] = df
            logger.info("Loaded {len(df)} instruments for {exchange}")

            return df

        except Exception as e:
            logger.error("Failed to load instruments: {e}")
            raise

    def get_instrument_token(self, symbol: str, exchange: str = "NSE") -> int:
        """Get instrument token for a symbol."""
        if exchange not in self._instruments_cache:
            self.load_instruments(exchange)

        return self._symbol_to_token.get(symbol)

    def fetch_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "day",
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Fetch historical data for backtesting and analysis.

        Args:
            symbol: Trading symbol (e.g., "RELIANCE", "TCS")
            start_date: Start date for data
            end_date: End date for data
            interval: Data interval (minute, 3minute, 5minute, 15minute, day)
            exchange: Exchange name

        Returns:
            DataFrame with OHLCV data in standard format
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        try:
            # Get instrument token
            token = self.get_instrument_token(symbol, exchange)
            if not token:
                raise ValueError("Instrument token not found for {symbol}")

            # Fetch historical data
            data = self.kite.historical_data(
                instrument_token=token,
                from_date=start_date,
                to_date=end_date,
                interval=interval,
            )

            # Convert to DataFrame and standardize format
            df = pd.DataFrame(data)

            if df.empty:
                logger.warning(
                    "No data found for {symbol} from {start_date} to {end_date}"
                )
                return df

            # Standardize column names (lowercase for consistency)
            df = df.rename(
                columns={
                    "date": "date",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "volume",
                }
            )

            # Ensure date is datetime and set as index
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

            # Add symbol column for tracking
            df["symbol"] = symbol

            logger.info("Fetched {len(df)} records for {symbol}")
            return df

        except Exception as e:
            logger.error("Failed to fetch historical data for {symbol}: {e}")
            raise

    def fetch_multiple_symbols(
        self,
        symbols: list[str],
        start_date: datetime,
        end_date: datetime,
        interval: str = "day",
        exchange: str = "NSE",
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch historical data for multiple symbols efficiently.

        Args:
            symbols: List of trading symbols
            start_date: Start date for data
            end_date: End date for data
            interval: Data interval
            exchange: Exchange name

        Returns:
            Dictionary mapping symbols to their DataFrames
        """
        results = {}

        for symbol in symbols:
            try:
                df = self.fetch_historical_data(
                    symbol, start_date, end_date, interval, exchange
                )
                if not df.empty:
                    results[symbol] = df

                # Rate limiting to avoid API limits
                time.sleep(0.1)

            except Exception as e:
                logger.error("Failed to fetch data for {symbol}: {e}")
                continue

        logger.info(
            "Successfully fetched data for {len(results)}/{len(symbols)} symbols"
        )
        return results

    def setup_websocket(self) -> None:
        """Setup WebSocket connection for real-time data."""
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        self.kws = KiteTicker(self.api_key, self.access_token)

        # Define WebSocket event handlers
        def on_ticks(ws, ticks):
            """Handle incoming tick data."""
            for tick in ticks:
                token = tick["instrument_token"]
                symbol = self._token_to_symbol.get(token, str(token))

                # Store latest tick data
                self.live_data[symbol] = {
                    "timestamp": datetime.now(),
                    "last_price": tick.get("last_price", 0),
                    "volume": tick.get("volume", 0),
                    "buy_quantity": tick.get("buy_quantity", 0),
                    "sell_quantity": tick.get("sell_quantity", 0),
                    "ohlc": tick.get("ohlc", {}),
                }

                logger.debug("Tick received for {symbol}: {tick['last_price']}")

        def on_connect(ws, response):
            """Handle WebSocket connection."""
            logger.info("WebSocket connected successfully")
            if self.subscribed_tokens:
                ws.subscribe(list(self.subscribed_tokens))
                ws.set_mode(ws.MODE_FULL, list(self.subscribed_tokens))

        def on_close(ws, code, reason):
            """Handle WebSocket disconnection."""
            logger.warning("WebSocket closed: {code} - {reason}")

        def on_error(ws, code, reason):
            """Handle WebSocket errors."""
            logger.error("WebSocket error: {code} - {reason}")

        # Assign event handlers
        self.kws.on_ticks = on_ticks
        self.kws.on_connect = on_connect
        self.kws.on_close = on_close
        self.kws.on_error = on_error

    def subscribe_symbols(self, symbols: list[str], exchange: str = "NSE") -> None:
        """
        Subscribe to real-time data for given symbols.

        Args:
            symbols: List of trading symbols to subscribe
            exchange: Exchange name
        """
        if not self.kws:
            self.setup_websocket()

        # Get instrument tokens
        tokens = []
        for symbol in symbols:
            token = self.get_instrument_token(symbol, exchange)
            if token:
                tokens.append(token)
                self.subscribed_tokens.add(token)
            else:
                logger.warning("Token not found for {symbol}")

        if tokens:
            logger.info("Subscribing to {len(tokens)} symbols")
            self.kws.subscribe(tokens)
            self.kws.set_mode(self.kws.MODE_FULL, tokens)

    def start_streaming(self) -> None:
        """Start real-time data streaming."""
        if not self.kws:
            raise RuntimeError("WebSocket not setup. Call subscribe_symbols() first.")

        logger.info("Starting real-time data streaming...")
        self.kws.connect(threaded=True)

    def get_live_price(self, symbol: str) -> Optional[float]:
        """Get latest price for a symbol."""
        data = self.live_data.get(symbol)
        return data["last_price"] if data else None

    def get_portfolio(self) -> pd.DataFrame:
        """
        Get current portfolio positions.

        Returns:
            DataFrame with portfolio details
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated")

        try:
            positions = self.kite.positions()
            df = pd.DataFrame(positions["net"])

            if not df.empty:
                # Standardize column names
                df = df.rename(
                    columns={
                        "tradingsymbol": "symbol",
                        "quantity": "quantity",
                        "average_price": "avg_price",
                        "last_price": "current_price",
                        "pnl": "unrealized_pnl",
                        "realised": "realized_pnl",
                    }
                )

            return df

        except Exception as e:
            logger.error("Failed to fetch portfolio: {e}")
            raise

    def place_order(
        self,
        symbol: str,
        quantity: int,
        order_type: str = "MARKET",
        transaction_type: str = "BUY",
        product: str = "MIS",
        price: float = None,
        exchange: str = "NSE",
    ) -> str:
        """
        Place a trading order.

        Args:
            symbol: Trading symbol
            quantity: Number of shares
            order_type: ORDER type (MARKET, LIMIT, SL, SL-M)
            transaction_type: BUY or SELL
            product: Product type (MIS, CNC, NRML)
            price: Limit price (required for LIMIT orders)
            exchange: Exchange name

        Returns:
            order_id: Unique order identifier
        """
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated")

        try:
            order_params = {
                "tradingsymbol": symbol,
                "exchange": exchange,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "product": product,
                "order_type": order_type,
            }

            if order_type == "LIMIT" and price:
                order_params["price"] = price

            order_id = self.kite.place_order(**order_params)

            logger.info(
                "Order placed: {order_id} - {transaction_type} {quantity} {symbol}"
            )
            return order_id

        except Exception as e:
            logger.error("Failed to place order: {e}")
            raise

    def get_orders(self) -> pd.DataFrame:
        """Get all orders for the day."""
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated")

        try:
            orders = self.kite.orders()
            return pd.DataFrame(orders)

        except Exception as e:
            logger.error("Failed to fetch orders: {e}")
            raise

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order."""
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated")

        try:
            self.kite.cancel_order(order_id)
            logger.info("Order cancelled: {order_id}")
            return True

        except Exception as e:
            logger.error("Failed to cancel order {order_id}: {e}")
            return False

    def get_margins(self) -> dict[str, Any]:
        """Get account margins and available funds."""
        if not self.is_authenticated:
            raise RuntimeError("Not authenticated")

        try:
            margins = self.kite.margins()
            return margins

        except Exception as e:
            logger.error("Failed to fetch margins: {e}")
            raise


class LiveTradingEngine:
    """
    Complete live trading engine using KiteConnect integration.

    This class combines the KiteDataLoader with trading logic
    for automated strategy execution in live markets.
    """

    def __init__(self, kite_loader: KiteDataLoader):
        """
        Initialize live trading engine.

        Args:
            kite_loader: Authenticated KiteDataLoader instance
        """
        self.kite = kite_loader
        self.active_strategies = {}
        self.position_tracker = {}
        self.risk_limits = {
            "max_position_size": 0.05,  # 5% of portfolio per position
            "max_daily_loss": 0.02,  # 2% daily loss limit
            "max_drawdown": 0.10,  # 10% max drawdown
        }

        logger.info("Live Trading Engine initialized")

    def register_strategy(self, strategy_name: str, strategy_func: callable) -> None:
        """Register a trading strategy for live execution."""
        self.active_strategies[strategy_name] = strategy_func
        logger.info("Strategy registered: {strategy_name}")

    def execute_strategy_signals(self, symbol: str, strategy_name: str) -> None:
        """
        Execute trading signals from a registered strategy.

        Args:
            symbol: Trading symbol
            strategy_name: Name of registered strategy
        """
        if strategy_name not in self.active_strategies:
            raise ValueError("Strategy not registered: {strategy_name}")

        try:
            # Get recent data for strategy
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)  # 30 days of data

            df = self.kite.fetch_historical_data(symbol, start_date, end_date)

            if df.empty:
                logger.warning("No data available for {symbol}")
                return

            # Run strategy
            strategy_func = self.active_strategies[strategy_name]
            signals = strategy_func(df)

            # Get latest signal
            if not signals.empty:
                latest_signal = signals.iloc[-1]
                signal_value = latest_signal.get("signal", 0)
                confidence = latest_signal.get("confidence", 0.5)

                # Execute trade based on signal
                if signal_value > 0.5 and confidence > 0.6:  # Strong buy signal
                    self._execute_buy_order(symbol, confidence)
                elif signal_value < -0.5 and confidence > 0.6:  # Strong sell signal
                    self._execute_sell_order(symbol, confidence)

        except Exception as e:
            logger.error("Strategy execution failed for {symbol}: {e}")

    def _execute_buy_order(self, symbol: str, confidence: float) -> None:
        """Execute a buy order with position sizing."""
        try:
            # Get current portfolio and margins
            margins = self.kite.get_margins()
            available_cash = margins["equity"]["available"]["cash"]

            # Get current price
            current_price = self.kite.get_live_price(symbol)
            if not current_price:
                logger.warning("No live price available for {symbol}")
                return

            # Calculate position size based on confidence and risk limits
            max_investment = available_cash * self.risk_limits["max_position_size"]
            confidence_adjusted = max_investment * confidence
            quantity = int(confidence_adjusted / current_price)

            if quantity > 0:
                order_id = self.kite.place_order(
                    symbol=symbol,
                    quantity=quantity,
                    transaction_type="BUY",
                    order_type="MARKET",
                )

                logger.info("Buy order placed: {symbol} x {quantity} @ {current_price}")

                # Track position
                self.position_tracker[symbol] = {
                    "quantity": quantity,
                    "entry_price": current_price,
                    "entry_time": datetime.now(),
                    "order_id": order_id,
                }

        except Exception as e:
            logger.error("Buy order execution failed for {symbol}: {e}")

    def _execute_sell_order(self, symbol: str, confidence: float) -> None:
        """Execute a sell order."""
        try:
            # Check if we have a position to sell
            if symbol not in self.position_tracker:
                logger.info("No position to sell for {symbol}")
                return

            position = self.position_tracker[symbol]
            quantity = position["quantity"]

            if quantity > 0:
                order_id = self.kite.place_order(
                    symbol=symbol,
                    quantity=quantity,
                    transaction_type="SELL",
                    order_type="MARKET",
                )

                logger.info("Sell order placed: {symbol} x {quantity}")

                # Remove from position tracker
                del self.position_tracker[symbol]

        except Exception as e:
            logger.error("Sell order execution failed for {symbol}: {e}")

    def monitor_positions(self) -> None:
        """Monitor open positions for risk management."""
        for symbol, position in self.position_tracker.items():
            current_price = self.kite.get_live_price(symbol)
            if current_price:
                entry_price = position["entry_price"]
                pnl_pct = (current_price - entry_price) / entry_price

                # Stop loss at 5% loss
                if pnl_pct < -0.05:
                    logger.warning("Stop loss triggered for {symbol}: {pnl_pct:.2%}")
                    self._execute_sell_order(symbol, 1.0)

                # Take profit at 15% gain
                elif pnl_pct > 0.15:
                    logger.info("Take profit triggered for {symbol}: {pnl_pct:.2%}")
                    self._execute_sell_order(symbol, 1.0)


# Configuration and setup helpers
def create_kite_config() -> dict[str, str]:
    """
    Create template configuration for KiteConnect setup.

    Returns:
        Dictionary with required configuration keys
    """
    return {
        "KITE_API_KEY": "your_api_key_here",
        "KITE_API_SECRET": "your_api_secret_here",
        "KITE_ACCESS_TOKEN": "generate_via_authentication_flow",
        "ENVIRONMENT": "production",  # or "sandbox" for testing
        "DEFAULT_EXCHANGE": "NSE",
    }


def validate_kite_setup() -> bool:
    """
    Validate that KiteConnect is properly configured.

    Returns:
        True if setup is valid, False otherwise
    """
    required_vars = ["KITE_API_KEY", "KITE_API_SECRET"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        logger.error("Missing environment variables: {missing}")
        return False

    if not KITE_AVAILABLE:
        logger.error("KiteConnect package not installed")
        return False

    return True


# Example usage and testing
if __name__ == "__main__":
    # Example setup and testing
    print("🚀 KiteConnect Integration for AI Trading Machine")
    print("=" * 50)

    if not validate_kite_setup():
        print("❌ KiteConnect setup validation failed")
        print("\n📋 Setup Requirements:")
        print("1. Install KiteConnect: pip install kiteconnect")
        print("2. Set environment variables:")
        for key, value in create_kite_config().items():
            print("   export {key}='{value}'")
        print("\n3. Complete authentication flow to get access token")
    else:
        print("✅ KiteConnect setup validation passed")

        # Initialize (example - requires actual credentials)
        try:
            loader = KiteDataLoader()
            print("✅ KiteDataLoader initialized")

            if loader.is_authenticated:
                print("✅ Authentication successful")

                # Load instruments
                instruments = loader.load_instruments("NSE")
                print("✅ Loaded {len(instruments)} NSE instruments")

                # Example: Fetch data for a symbol
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)

                sample_symbol = "RELIANCE"
                df = loader.fetch_historical_data(sample_symbol, start_date, end_date)
                print("✅ Fetched {len(df)} records for {sample_symbol}")

            else:
                print("⚠️  Authentication required")
                login_url = loader.kite.login_url()
                print("Visit: {login_url}")

        except Exception as e:
            print("❌ Error during testing: {e}")

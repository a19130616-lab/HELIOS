"""
Data Ingestor for Helios Trading System
Handles real-time data ingestion from Binance WebSocket streams.
"""

import asyncio
import websockets
import json
import logging
import threading
import time
import random
import os
from typing import List, Dict, Any, Optional, Callable
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

from app.models import OrderBookSnapshot, OrderBookLevel, TradeData, OrderSide
from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config

class DataIngestor:
    """
    Handles real-time data ingestion from Binance Futures WebSocket streams.
    Processes order book, trade, and kline data and stores in Redis for other services.
    """
    
    def __init__(self, symbols: List[str], redis_manager: RedisManager, api_client=None, synthetic_mode: bool = False):
        """
        Initialize the Data Ingestor.
        
        Args:
            symbols: List of trading symbols to monitor
            redis_manager: Redis manager instance
            api_client: Binance API client (None when unauthenticated - public/synthetic modes)
        """
        self.symbols = symbols
        self.redis_manager = redis_manager
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        self.api_client = api_client
        
        # Synthetic (simulated) mode:
        # Enabled ONLY when:
        #   - explicit synthetic_mode param True OR
        #   - ALLOW_SYNTHETIC=1 and no authenticated api_client
        self.synthetic_mode = bool(
            synthetic_mode or (
                os.getenv('ALLOW_SYNTHETIC', 'false').lower() in ('1', 'true', 'yes', 'on')
                and api_client is None
            )
        )
        
        # WebSocket client
        self.ws_client = None
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Data storage
        self.order_books = {}
        self.latest_trades = {}
        self.kline_data = {}
        
        # Stream IDs for cleanup
        self.stream_ids = []
        
        # Synthetic mode data generation
        self.synthetic_thread = None
        self.synthetic_prices = {}  # Track simulated prices for each symbol
        
        # Price logging
        self.price_logging_thread = None
        self.last_price_log_time = 0
        
        if self.synthetic_mode:
            self.logger.warning("🔸 Data Ingestor running in SYNTHETIC MODE (simulated market data) - gated by ALLOW_SYNTHETIC")
            self._initialize_synthetic_prices()
        
    def start(self) -> None:
        """Start the data ingestion process."""
        self.logger.info("Starting Data Ingestor...")
        self.is_running = True

        # Backfill historical klines needed by downstream components.
        # The SignalEngine hybrid strategy requires ~50 closed 1h candles to compute EMA(50).
        # Without backfill, a cold start can produce *zero* signals/decisions for many hours.
        # PublicPriceIngestor already performs historical backfill in public mode; this is for live WS mode.
        try:
            self._backfill_required_klines()
        except Exception as e:
            # Non-fatal: do not block ingestion if backfill fails.
            self.logger.warning(f"Kline backfill skipped/failed: {e}")

        # Historical backfill to prevent long warm-up periods for strategies that
        # require higher-timeframe indicators (e.g., 1h EMA).
        # Without this, the system may run for many hours with 0 decisions after a cold start.
        try:
            if not self.synthetic_mode:
                self._backfill_historical_klines()
        except Exception as e:
            # Non-fatal: keep the system running even if backfill fails (rate limits, network, etc.)
            self.logger.warning(f"Historical kline backfill skipped/failed: {e}")
        
        # Start price logging thread for both demo and live modes
        self.price_logging_thread = threading.Thread(target=self._run_price_logging, daemon=True)
        self.price_logging_thread.start()
        
        if self.synthetic_mode:
            # Start synthetic data generation
            self.synthetic_thread = threading.Thread(target=self._run_synthetic_mode, daemon=True)
            self.synthetic_thread.start()
            self.logger.info(f"🔸 Data Ingestor started in SYNTHETIC MODE for symbols: {self.symbols}")
        else:
            # Start WebSocket client in a separate thread
            self.ws_thread = threading.Thread(target=self._run_websocket, daemon=True)
            self.ws_thread.start()
            self.logger.info(f"Data Ingestor started for symbols: {self.symbols}")

    def _backfill_required_klines(self) -> None:
        """Backfill historical klines into Redis to avoid long warm-up periods.

        Currently backfills 1h klines so EMA(50) can be computed immediately.
        Uses authenticated REST client when available.
        """
        if self.synthetic_mode:
            return
        if self.api_client is None:
            # Public mode uses PublicPriceIngestor for backfill.
            return

        # Fetch enough candles for EMA(50) plus buffer.
        interval = "1h"
        limit = 120

        now_ms = get_timestamp()
        total_loaded = 0

        for symbol in self.symbols:
            try:
                list_key = f"klines:{symbol}:{interval}"
                try:
                    existing = int(self.redis_manager.redis_client.llen(list_key) or 0)
                except Exception:
                    existing = 0

                # If we already have enough history, skip.
                if existing >= 60:
                    continue

                self.logger.info(f"[HIST] Backfilling {interval} klines for {symbol} (need EMA warmup)...")
                klines = self.api_client.klines(symbol=symbol, interval=interval, limit=limit)
                if not klines or not isinstance(klines, list):
                    continue

                loaded_for_symbol = 0
                for k in klines:
                    try:
                        # Expected format: [open_time, open, high, low, close, volume, close_time, ...]
                        if not isinstance(k, (list, tuple)) or len(k) < 7:
                            continue

                        close_time = int(k[6])
                        # Skip candles that are not closed yet (avoid partial 1h candle).
                        if close_time > now_ms:
                            continue

                        kline = {
                            "symbol": symbol,
                            "interval": interval,
                            "timestamp": close_time,
                            "open": float(k[1]),
                            "high": float(k[2]),
                            "low": float(k[3]),
                            "close": float(k[4]),
                            "volume": float(k[5]),
                            "trades": int(k[8]) if len(k) > 8 and str(k[8]).isdigit() else 0,
                        }

                        # Store in Redis list+key schema used elsewhere.
                        self._store_kline(kline)
                        loaded_for_symbol += 1
                    except Exception:
                        continue

                if loaded_for_symbol:
                    total_loaded += loaded_for_symbol
                    self.logger.info(f"[HIST] Backfilled {loaded_for_symbol} {interval} klines for {symbol}")
            except Exception as e:
                self.logger.warning(f"[HIST] Backfill failed for {symbol} {interval}: {e}")

        if total_loaded:
            self.logger.info(f"[HIST] Kline backfill complete interval={interval} total_loaded={total_loaded}")

    def _backfill_historical_klines(self, interval: str = "1h", limit: int = 120) -> None:
        """Backfill historical klines into Redis.

        This is intentionally small and focused: it only fills Redis with enough
        higher-timeframe candles so SignalEngine can compute trend indicators
        immediately after startup.

        Args:
            interval: Kline interval string (e.g., '1h')
            limit: Number of candles to fetch per symbol
        """
        if not self.api_client:
            self.logger.info("Skipping historical kline backfill (no authenticated api_client)")
            return

        started = time.time()
        total = 0
        self.logger.info(
            f"[HIST] Backfilling {interval} klines (limit={limit}) for {len(self.symbols)} symbols to warm indicators..."
        )

        for symbol in self.symbols:
            try:
                klines = self.api_client.klines(symbol=symbol, interval=interval, limit=limit)
                if not klines or not isinstance(klines, list):
                    self.logger.warning(f"[HIST] No klines returned for {symbol} {interval}")
                    continue

                # Binance returns oldest->newest. We store via LPUSH so the newest ends up at the head.
                loaded = 0
                for idx, k in enumerate(klines):
                    try:
                        # kline format: [openTime, open, high, low, close, volume, closeTime, quoteVol, trades, ...]
                        close_time = int(k[6]) if len(k) > 6 else int(k[0])
                        kline = {
                            "symbol": symbol,
                            "interval": interval,
                            "timestamp": close_time,
                            "open": float(k[1]),
                            "high": float(k[2]),
                            "low": float(k[3]),
                            "close": float(k[4]),
                            "volume": float(k[5]),
                            "trades": int(k[8]) if len(k) > 8 else 0,
                        }
                        self._store_kline(kline)
                        loaded += 1
                    except Exception as inner_e:
                        self.logger.debug(f"[HIST] Parse/store error {symbol} {interval} idx={idx}: {inner_e}")

                total += loaded
                self.logger.info(f"[HIST] Loaded {loaded}/{len(klines)} {interval} klines for {symbol}")

                # Gentle pacing to reduce risk of rate limiting
                time.sleep(0.2)
            except Exception as e:
                self.logger.warning(f"[HIST] Failed to backfill klines for {symbol} {interval}: {e}")

        self.logger.info(
            f"[HIST] Kline backfill complete interval={interval} total_klines={total} duration={time.time() - started:.2f}s"
        )

    def _normalize_ws_message(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        """Normalize websocket callback payloads across connector versions.

        Different websocket client versions may call callbacks as:
          - callback(message_dict)
          - callback(ws, message_dict)
          - callback(ws, message_json_str)
        Some may also wrap stream messages as {"stream": "...", "data": {...}}.

        Returns the inner message dict (event payload) or None.
        """
        raw: Any = None
        if args:
            raw = args[-1]
        elif kwargs:
            raw = (
                kwargs.get("data")
                or kwargs.get("message")
                or kwargs.get("msg")
                or kwargs.get("payload")
            )

        if raw is None:
            return None

        # Decode JSON strings
        if isinstance(raw, (str, bytes)):
            try:
                raw = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
                raw = json.loads(raw)
            except Exception:
                return None

        if not isinstance(raw, dict):
            return None

        # Unwrap multiplexed messages
        if "data" in raw and isinstance(raw.get("data"), dict):
            return raw["data"]

        return raw

    def _make_ws_callback(self, symbol: str, handler: Callable[[str, Dict[str, Any]], None]) -> Callable[..., None]:
        """Create a safe websocket callback that tolerates signature differences."""
        def _cb(*args: Any, **kwargs: Any) -> None:
            try:
                payload = self._normalize_ws_message(*args, **kwargs)
                if not payload:
                    return
                handler(symbol, payload)
            except Exception as e:
                # Avoid crashing websocket receive thread on malformed payloads
                self.logger.error(f"WebSocket callback error for {symbol}: {e}")
        return _cb

    def _on_ws_message(self, ws, message: Any) -> None:
        """Route all incoming websocket messages to the appropriate per-symbol handler.

        NOTE: binance-futures-connector websocket clients dispatch messages ONLY via the
        `on_message` callback. The stream subscription helper methods accept **kwargs
        but do not invoke per-stream callbacks.
        """
        payload = self._normalize_ws_message(ws, message)
        if not payload:
            return

        # Subscription ACKs look like: {"result": null, "id": ...}
        if "result" in payload and "id" in payload and payload.get("result") is None:
            return

        event_type = payload.get("e")
        # Symbol can be at the root or nested (kline)
        symbol = payload.get("s") or payload.get("ps")
        if not symbol and isinstance(payload.get("k"), dict):
            symbol = payload["k"].get("s")

        if not symbol:
            return

        # Normalize to the same symbol format used everywhere else
        symbol = str(symbol).upper()

        try:
            if event_type == "depthUpdate":
                self._handle_depth_update(symbol, payload)
            elif event_type == "aggTrade":
                self._handle_trade_update(symbol, payload)
            elif event_type == "kline":
                self._handle_kline_update(symbol, payload)
        except Exception as e:
            self.logger.error(f"WebSocket message routing error for {symbol}: {e}")
    
    def stop(self) -> None:
        """Stop the data ingestion process."""
        self.logger.info("Stopping Data Ingestor...")
        self.is_running = False
        
        if self.ws_client:
            self.ws_client.stop()
        
        self.logger.info("Data Ingestor stopped")
    
    def _run_websocket(self) -> None:
        """Run the WebSocket client with reconnection logic."""
        while self.is_running:
            try:
                self._connect_websocket()
                self.reconnect_attempts = 0
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}")
                self._handle_reconnection()
    
    def _connect_websocket(self) -> None:
        """Connect to Binance WebSocket streams."""
        self.logger.info("Connecting to Binance WebSocket...")
        
        # Use testnet if configured
        testnet = self.config.get('binance', 'testnet', bool)
        
        # Define callbacks for connection debugging
        def on_open(ws):
            self.logger.info("✅ Binance WebSocket connection OPENED")
            
        def on_close(ws, *args):
            # Handle variable arguments for on_close (some versions send code/msg, others don't)
            try:
                if len(args) >= 2:
                    code, msg = args[0], args[1]
                    self.logger.warning(f"⚠️ Binance WebSocket connection CLOSED: code={code}, msg={msg}")
                else:
                    self.logger.warning(f"⚠️ Binance WebSocket connection CLOSED: args={args}")
            except Exception as e:
                self.logger.warning(f"⚠️ Binance WebSocket connection CLOSED (error parsing args): {e}")
            
        def on_error(ws, error):
            self.logger.error(f"❌ Binance WebSocket connection ERROR: {error}")

        # IMPORTANT: Do not pass stream_url manually unless you are sure.
        # The library handles the base URL based on on_message/on_error callbacks usually,
        # but for UMFuturesWebsocketClient, it defaults to production.
        # If testnet is True, we must explicitly set the testnet URL.
        # If testnet is False, we can let it default or set the production URL.
        
        try:
            if testnet:
                self.ws_client = UMFuturesWebsocketClient(
                    stream_url="wss://stream.binancefuture.com",
                    on_message=self._on_ws_message,
                    on_open=on_open,
                    on_close=on_close,
                    on_error=on_error
                )
            else:
                # Production default
                self.ws_client = UMFuturesWebsocketClient(
                    on_message=self._on_ws_message,
                    on_open=on_open,
                    on_close=on_close,
                    on_error=on_error
                )
            # Some versions of binance-futures-connector expose start(); older
            # ones spin up immediately when subscriptions are created. Call start
            # only when available to avoid attribute errors seen in production.
            if hasattr(self.ws_client, "start"):
                try:
                    self.ws_client.start()
                    self.logger.info("WebSocket client background thread started")
                except Exception as start_err:
                    self.logger.warning(f"WebSocket client start() failed, continuing without explicit start: {start_err}")
            
            self.logger.info(f"WebSocket client initialized (testnet={testnet}). Subscribing to {len(self.symbols)} symbols...")
            
            # Subscribe to streams for each symbol
            for symbol in self.symbols:
                # Skip ALPACAUSDT for now as it might be causing issues
                if symbol == 'ALPACAUSDT':
                    self.logger.warning(f"Skipping subscription for {symbol} to isolate connection issues")
                    continue
                    
                try:
                    self._subscribe_symbol_streams(symbol)
                    # Add delay to avoid hitting WebSocket rate limits (5 messages per second)
                    time.sleep(0.5)
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to {symbol}: {e}")
            
            self.logger.info("All subscription requests sent. Entering keep-alive loop.")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize WebSocket client: {e}")
            raise

        # Keep connection alive
        while self.is_running:
            time.sleep(1)
    
    def _subscribe_symbol_streams(self, symbol: str) -> None:
        """
        Subscribe to all required streams for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        symbol_lower = symbol.lower()
        
        # Order book depth stream (20 levels, 100ms updates)
        # Note: Using partial_book_depth instead of depth for binance-futures-connector compatibility
        depth_stream_id = self.ws_client.partial_book_depth(
            symbol=symbol_lower,
            level=20,
            speed=100,
            callback=self._make_ws_callback(symbol, self._handle_depth_update)
        )
        self.stream_ids.append(depth_stream_id)
        time.sleep(0.2)  # Rate limit protection
        
        # Aggregate trade stream
        trade_stream_id = self.ws_client.agg_trade(
            symbol=symbol_lower,
            callback=self._make_ws_callback(symbol, self._handle_trade_update)
        )
        self.stream_ids.append(trade_stream_id)
        time.sleep(0.2)  # Rate limit protection
        
        # 1-minute kline stream
        kline_stream_id = self.ws_client.kline(
            symbol=symbol_lower,
            interval="1m",
            callback=self._make_ws_callback(symbol, self._handle_kline_update)
        )
        self.stream_ids.append(kline_stream_id)

        # 1-hour kline stream (for trend filter)
        kline_1h_stream_id = self.ws_client.kline(
            symbol=symbol_lower,
            interval="1h",
            callback=self._make_ws_callback(symbol, self._handle_kline_update)
        )
        self.stream_ids.append(kline_1h_stream_id)

        # 1-hour kline stream (for trend filter)
        kline_1h_stream_id = self.ws_client.kline(
            symbol=symbol_lower,
            interval="1h",
            callback=self._make_ws_callback(symbol, self._handle_kline_update)
        )
        self.stream_ids.append(kline_1h_stream_id)
        
        self.logger.info(f"Subscribed to streams for {symbol}")
    
    def _handle_depth_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        Handle order book depth updates.
        
        Args:
            symbol: Trading symbol
            data: WebSocket data
        """
        try:
            # Check for error response in data
            if (data.get('e') == 'error') or ('code' in data and 'msg' in data) or ('msg' in data and 'e' in data):
                self.logger.error(f"Binance WebSocket Error for {symbol}: {data.get('msg')}")
                return

            # Parse order book data
            bids = [
                OrderBookLevel(price=float(bid[0]), quantity=float(bid[1]))
                for bid in data.get('b', [])
            ]
            asks = [
                OrderBookLevel(price=float(ask[0]), quantity=float(ask[1]))
                for ask in data.get('a', [])
            ]
            
            # Create order book snapshot
            order_book = OrderBookSnapshot(
                symbol=symbol,
                timestamp=data.get('E', get_timestamp()),
                bids=sorted(bids, key=lambda x: x.price, reverse=True),
                asks=sorted(asks, key=lambda x: x.price)
            )
            
            # Store in memory and Redis
            self.order_books[symbol] = order_book
            self._store_order_book(order_book)
            
        except Exception as e:
            self.logger.error(f"Error processing depth update for {symbol}: {e}")
    
    def _handle_trade_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        Handle trade updates.
        
        Args:
            symbol: Trading symbol
            data: WebSocket data
        """
        try:
            if (data.get('e') == 'error') or ('code' in data and 'msg' in data):
                self.logger.error(f"Binance WebSocket Error for {symbol}: {data.get('msg')}")
                return

            # Parse trade data
            trade = TradeData(
                symbol=symbol,
                timestamp=data.get('T', get_timestamp()),
                price=float(data.get('p', 0)),
                quantity=float(data.get('q', 0)),
                # Binance aggTrade field `m` = isBuyerMaker. If buyer is maker, taker is SELL.
                side=OrderSide.SELL if data.get('m', False) else OrderSide.BUY,
                trade_id=str(data.get('a', 0))
            )
            
            # Store latest trade
            self.latest_trades[symbol] = trade
            self._store_trade(trade)
            
        except Exception as e:
            self.logger.error(f"Error processing trade update for {symbol}: {e}")
    
    def _handle_kline_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        Handle kline (candlestick) updates.
        
        Args:
            symbol: Trading symbol
            data: WebSocket data
        """
        try:
            if (data.get('e') == 'error') or ('code' in data and 'msg' in data):
                self.logger.error(f"Binance WebSocket Error for {symbol}: {data.get('msg')}")
                return

            kline_data = data.get('k', {})
            
            # Only process closed klines
            if not kline_data.get('x', False):
                return
            
            # Parse kline data
            interval = kline_data.get('i', '1m')
            kline = {
                'symbol': symbol,
                'interval': interval,
                'timestamp': kline_data.get('T', get_timestamp()),
                'open': float(kline_data.get('o', 0)),
                'high': float(kline_data.get('h', 0)),
                'low': float(kline_data.get('l', 0)),
                'close': float(kline_data.get('c', 0)),
                'volume': float(kline_data.get('v', 0)),
                'trades': int(kline_data.get('n', 0))
            }
            
            # Store kline data
            if symbol not in self.kline_data:
                self.kline_data[symbol] = {}
            
            # Handle migration from list to dict if necessary
            if isinstance(self.kline_data[symbol], list):
                 self.kline_data[symbol] = {'1m': self.kline_data[symbol]}

            if interval not in self.kline_data[symbol]:
                self.kline_data[symbol][interval] = []
            
            self.kline_data[symbol][interval].append(kline)
            
            # Keep only last 200 klines per symbol/interval
            if len(self.kline_data[symbol][interval]) > 200:
                self.kline_data[symbol][interval] = self.kline_data[symbol][interval][-200:]
            
            self._store_kline(kline)
            
        except Exception as e:
            self.logger.error(f"Error processing kline update for {symbol}: {e}")
    
    def _store_order_book(self, order_book: OrderBookSnapshot) -> None:
        """
        Store order book snapshot in Redis.
        
        Args:
            order_book: Order book snapshot
        """
        key = f"orderbook:{order_book.symbol}"
        data = {
            'symbol': order_book.symbol,
            'timestamp': order_book.timestamp,
            'bids': [[level.price, level.quantity] for level in order_book.bids],
            'asks': [[level.price, level.quantity] for level in order_book.asks],
            'mid_price': order_book.get_mid_price(),
            'spread': order_book.get_spread()
        }
        
        # Store with 60 second expiry
        self.redis_manager.set_data(key, data, expiry=60)
        
        # Publish to subscribers
        self.redis_manager.publish(f"orderbook_updates", {
            'symbol': order_book.symbol,
            'timestamp': order_book.timestamp,
            'mid_price': order_book.get_mid_price()
        })
    
    def _store_trade(self, trade: TradeData) -> None:
        """
        Store trade data in Redis.
        
        Args:
            trade: Trade data
        """
        key = f"trade:{trade.symbol}"
        data = {
            'symbol': trade.symbol,
            'timestamp': trade.timestamp,
            'price': trade.price,
            'quantity': trade.quantity,
            'side': trade.side.value,
            'trade_id': trade.trade_id
        }
        
        # Store with 30 second expiry
        self.redis_manager.set_data(key, data, expiry=30)
        
        # Publish to subscribers
        self.redis_manager.publish("trade_updates", data)
    
    def _store_kline(self, kline: Dict[str, Any]) -> None:
        """
        Store kline data in Redis.
        
        Args:
            kline: Kline data
        """
        interval = kline.get('interval', '1m')
        key = f"kline:{kline['symbol']}:{interval}"
        
        # Store with 300 second expiry
        self.redis_manager.set_data(key, kline, expiry=300)
        
        # Also store in list for historical data
        list_key = f"klines:{kline['symbol']}:{interval}"
        self.redis_manager.redis_client.lpush(list_key, json.dumps(kline))
        self.redis_manager.redis_client.ltrim(list_key, 0, 199)  # Keep last 200
        self.redis_manager.redis_client.expire(list_key, 3600 * 24)  # 24 hour expiry
    
    def _handle_reconnection(self) -> None:
        """Handle WebSocket reconnection with exponential backoff."""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error("Max reconnection attempts reached. Stopping Data Ingestor.")
            self.is_running = False
            return
        
        self.reconnect_attempts += 1
        wait_time = min(60, 2 ** self.reconnect_attempts)
        
        self.logger.warning(f"Reconnection attempt {self.reconnect_attempts} in {wait_time} seconds...")
        time.sleep(wait_time)
    
    def get_latest_order_book(self, symbol: str) -> Optional[OrderBookSnapshot]:
        """
        Get the latest order book for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Latest order book snapshot or None
        """
        return self.order_books.get(symbol)
    
    def get_latest_trade(self, symbol: str) -> Optional[TradeData]:
        """
        Get the latest trade for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Latest trade data or None
        """
        return self.latest_trades.get(symbol)
    
    def get_kline_history(self, symbol: str, count: int = 100, interval: str = '1m') -> List[Dict[str, Any]]:
        """
        Get kline history for a symbol.
        
        Args:
            symbol: Trading symbol
            count: Number of klines to retrieve
            interval: Kline interval (default '1m')
            
        Returns:
            List of kline data
        """
        symbol_data = self.kline_data.get(symbol, {})
        if isinstance(symbol_data, list):
             # Fallback for old structure
             klines = symbol_data if interval == '1m' else []
        else:
             klines = symbol_data.get(interval, [])
             
        return klines[-count:] if len(klines) > count else klines
    
    def add_symbol(self, symbol: str) -> None:
        """
        Add a new symbol to monitor.
        
        Args:
            symbol: Trading symbol to add
        """
        if symbol not in self.symbols:
            self.symbols.append(symbol)
            if self.ws_client and self.is_running:
                self._subscribe_symbol_streams(symbol)
            self.logger.info(f"Added symbol {symbol} to monitoring")
    
    def remove_symbol(self, symbol: str) -> None:
        """
        Remove a symbol from monitoring.
        
        Args:
            symbol: Trading symbol to remove
        """
        if symbol in self.symbols:
            self.symbols.remove(symbol)
            # Clean up stored data
            self.order_books.pop(symbol, None)
            self.latest_trades.pop(symbol, None)
            self.kline_data.pop(symbol, None)
            self.logger.info(f"Removed symbol {symbol} from monitoring")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the Data Ingestor.
        
        Returns:
            Health status information
        """
        return {
            'is_running': self.is_running,
            'symbols_monitored': len(self.symbols),
            'symbols': self.symbols,
            'reconnect_attempts': self.reconnect_attempts,
            'order_books_active': len(self.order_books),
            'latest_trades_active': len(self.latest_trades),
            'websocket_connected': self.ws_client is not None,
            'synthetic_mode': self.synthetic_mode
        }

    def _initialize_synthetic_prices(self) -> None:
        """Initialize synthetic prices for simulation."""
        # Realistic starting prices for major crypto pairs
        base_prices = {
            'BTCUSDT': 45000.0,
            'ETHUSDT': 2800.0,
            'BNBUSDT': 320.0,
            'ADAUSDT': 1.2,
            'SOLUSDT': 95.0,
            'DOTUSDT': 25.0,
            'AVAXUSDT': 35.0,
            'MATICUSDT': 0.85
        }
        
        for symbol in self.symbols:
            if symbol in base_prices:
                self.synthetic_prices[symbol] = base_prices[symbol]
            else:
                # Generate random price for unknown symbols
                self.synthetic_prices[symbol] = random.uniform(1.0, 100.0)

    def _run_synthetic_mode(self) -> None:
        """Run synthetic mode with simulated market data (used only when ALLOW_SYNTHETIC enabled)."""
        self.logger.info("🔸 Starting synthetic data generation...")
        
        while self.is_running:
            try:
                for symbol in self.symbols:
                    self._generate_synthetic_orderbook(symbol)
                    self._generate_synthetic_trade(symbol)
                    
                    # Generate kline every minute
                    if int(time.time()) % 60 == 0:
                        self._generate_synthetic_kline(symbol)
                
                # Update every 100ms to simulate high-frequency data
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Error in synthetic mode: {e}")
                time.sleep(1)

    def _generate_synthetic_orderbook(self, symbol: str) -> None:
        """Generate simulated order book data."""
        current_price = self.synthetic_prices[symbol]
        
        # Add some random price movement (-0.1% to +0.1%)
        price_change = random.uniform(-0.001, 0.001)
        new_price = current_price * (1 + price_change)
        self.synthetic_prices[symbol] = new_price
        
        # Generate order book levels
        bids = []
        asks = []
        
        # Generate 20 bid levels below current price
        for i in range(20):
            price = new_price * (1 - (i + 1) * 0.0001)
            quantity = random.uniform(0.1, 10.0)
            bids.append(OrderBookLevel(price=price, quantity=quantity))
        
        # Generate 20 ask levels above current price
        for i in range(20):
            price = new_price * (1 + (i + 1) * 0.0001)
            quantity = random.uniform(0.1, 10.0)
            asks.append(OrderBookLevel(price=price, quantity=quantity))
        
        # Create order book snapshot
        order_book = OrderBookSnapshot(
            symbol=symbol,
            timestamp=get_timestamp(),
            bids=sorted(bids, key=lambda x: x.price, reverse=True),
            asks=sorted(asks, key=lambda x: x.price)
        )
        
        # Store in memory and Redis
        self.order_books[symbol] = order_book
        self._store_order_book(order_book)

    def _generate_synthetic_trade(self, symbol: str) -> None:
        """Generate simulated trade data."""
        current_price = self.synthetic_prices[symbol]
        
        # Generate trade around current price
        trade_price = current_price * random.uniform(0.9999, 1.0001)
        trade_quantity = random.uniform(0.01, 5.0)
        trade_side = random.choice([OrderSide.BUY, OrderSide.SELL])
        
        trade = TradeData(
            symbol=symbol,
            timestamp=get_timestamp(),
            price=trade_price,
            quantity=trade_quantity,
            side=trade_side,
            trade_id=str(int(time.time() * 1000))
        )
        
        # Store latest trade
        self.latest_trades[symbol] = trade
        self._store_trade(trade)

    def _generate_synthetic_kline(self, symbol: str) -> None:
        """Generate simulated kline data."""
        current_price = self.synthetic_prices[symbol]
        
        # Generate OHLC data with some volatility
        open_price = current_price * random.uniform(0.999, 1.001)
        close_price = current_price * random.uniform(0.999, 1.001)
        high_price = max(open_price, close_price) * random.uniform(1.0, 1.002)
        low_price = min(open_price, close_price) * random.uniform(0.998, 1.0)
        volume = random.uniform(100, 1000)
        trades = random.randint(50, 200)
        
        kline = {
            'symbol': symbol,
            'timestamp': get_timestamp(),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume,
            'trades': trades
        }
        
        # Store kline data
        if symbol not in self.kline_data:
            self.kline_data[symbol] = []
        
        self.kline_data[symbol].append(kline)
        
        # Keep only last 200 klines per symbol
        if len(self.kline_data[symbol]) > 200:
            self.kline_data[symbol] = self.kline_data[symbol][-200:]
        
        self._store_kline(kline)

    def _run_price_logging(self) -> None:
        """Run periodic price logging every 10 seconds."""
        self.logger.info("🔸 Starting periodic price logging every 10 seconds...")
        
        # Wait 5 seconds for system to initialize
        time.sleep(5.0)
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # Log prices every 10 seconds
                if current_time - self.last_price_log_time >= 10.0:
                    self._log_all_prices()
                    self.last_price_log_time = current_time
                
                # Sleep for 1 second to check time regularly
                time.sleep(1.0)
                
            except Exception as e:
                self.logger.error(f"Error in price logging: {e}")
                time.sleep(1.0)

    def _log_all_prices(self) -> None:
        """Log current prices for all monitored symbols."""
        price_info = []
        
        # Always try to log something to confirm the logging is working
        mode_indicator = "🧪 SYNTHETIC" if self.synthetic_mode else "📈 LIVE"
        
        for symbol in self.symbols:
            try:
                if self.synthetic_mode:
                    # Use synthetic prices
                    current_price = self.synthetic_prices.get(symbol)
                    if current_price is not None and current_price > 0:
                        price_info.append(f"{symbol}: ${current_price:.4f}")
                    else:
                        price_info.append(f"{symbol}: Initializing...")
                else:
                    # Use latest trade price or order book mid price
                    latest_trade = self.latest_trades.get(symbol)
                    if latest_trade:
                        price_info.append(f"{symbol}: ${latest_trade.price:.4f}")
                    else:
                        order_book = self.order_books.get(symbol)
                        if order_book:
                            mid_price = order_book.get_mid_price()
                            if mid_price:
                                price_info.append(f"{symbol}: ${mid_price:.4f}")
                            else:
                                price_info.append(f"{symbol}: No mid price")
                        else:
                            price_info.append(f"{symbol}: No data")
            except Exception as e:
                price_info.append(f"{symbol}: Error({str(e)[:20]})")
        
        # Always log, even if no data
        if price_info:
            self.logger.info(f"{mode_indicator} MARKET PRICES: {' | '.join(price_info)}")
        else:
            self.logger.info(f"{mode_indicator} MARKET PRICES: No symbols configured")
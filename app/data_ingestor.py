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
    
    def __init__(self, symbols: List[str], redis_manager: RedisManager, api_client=None):
        """
        Initialize the Data Ingestor.
        
        Args:
            symbols: List of trading symbols to monitor
            redis_manager: Redis manager instance
            api_client: Binance API client (None for demo mode)
        """
        self.symbols = symbols
        self.redis_manager = redis_manager
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        self.api_client = api_client
        
        # Check for demo mode
        self.demo_mode = (api_client is None or
                         os.getenv('DEMO_MODE', 'false').lower() == 'true')
        
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
        
        # Demo mode data generation
        self.demo_thread = None
        self.demo_prices = {}  # Track simulated prices for each symbol
        
        if self.demo_mode:
            self.logger.warning("🔸 Data Ingestor running in DEMO MODE - Using simulated market data")
            self._initialize_demo_prices()
    
    def start(self) -> None:
        """Start the data ingestion process."""
        self.logger.info("Starting Data Ingestor...")
        self.is_running = True
        
        if self.demo_mode:
            # Start demo data generation
            self.demo_thread = threading.Thread(target=self._run_demo_mode, daemon=True)
            self.demo_thread.start()
            self.logger.info(f"🔸 Data Ingestor started in DEMO MODE for symbols: {self.symbols}")
        else:
            # Start WebSocket client in a separate thread
            self.ws_thread = threading.Thread(target=self._run_websocket, daemon=True)
            self.ws_thread.start()
            self.logger.info(f"Data Ingestor started for symbols: {self.symbols}")
    
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
        
        self.ws_client = UMFuturesWebsocketClient(
            stream_url="wss://fstream.binance.com" if not testnet else "wss://stream.binancefuture.com"
        )
        
        # Subscribe to streams for each symbol
        for symbol in self.symbols:
            self._subscribe_symbol_streams(symbol)
        
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
        depth_stream_id = self.ws_client.depth(
            symbol=symbol_lower,
            level=20,
            speed=100,
            callback=lambda data: self._handle_depth_update(symbol, data)
        )
        self.stream_ids.append(depth_stream_id)
        
        # Aggregate trade stream
        trade_stream_id = self.ws_client.agg_trade(
            symbol=symbol_lower,
            callback=lambda data: self._handle_trade_update(symbol, data)
        )
        self.stream_ids.append(trade_stream_id)
        
        # 1-minute kline stream
        kline_stream_id = self.ws_client.kline(
            symbol=symbol_lower,
            interval="1m",
            callback=lambda data: self._handle_kline_update(symbol, data)
        )
        self.stream_ids.append(kline_stream_id)
        
        self.logger.info(f"Subscribed to streams for {symbol}")
    
    def _handle_depth_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        Handle order book depth updates.
        
        Args:
            symbol: Trading symbol
            data: WebSocket data
        """
        try:
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
            # Parse trade data
            trade = TradeData(
                symbol=symbol,
                timestamp=data.get('T', get_timestamp()),
                price=float(data.get('p', 0)),
                quantity=float(data.get('q', 0)),
                side=OrderSide.BUY if data.get('m', False) else OrderSide.SELL,
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
            kline_data = data.get('k', {})
            
            # Only process closed klines
            if not kline_data.get('x', False):
                return
            
            # Parse kline data
            kline = {
                'symbol': symbol,
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
                self.kline_data[symbol] = []
            
            self.kline_data[symbol].append(kline)
            
            # Keep only last 200 klines per symbol
            if len(self.kline_data[symbol]) > 200:
                self.kline_data[symbol] = self.kline_data[symbol][-200:]
            
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
        key = f"kline:{kline['symbol']}"
        
        # Store with 300 second expiry
        self.redis_manager.set_data(key, kline, expiry=300)
        
        # Also store in list for historical data
        list_key = f"klines:{kline['symbol']}"
        self.redis_manager.redis_client.lpush(list_key, json.dumps(kline))
        self.redis_manager.redis_client.ltrim(list_key, 0, 199)  # Keep last 200
        self.redis_manager.redis_client.expire(list_key, 3600)  # 1 hour expiry
    
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
    
    def get_kline_history(self, symbol: str, count: int = 100) -> List[Dict[str, Any]]:
        """
        Get kline history for a symbol.
        
        Args:
            symbol: Trading symbol
            count: Number of klines to retrieve
            
        Returns:
            List of kline data
        """
        klines = self.kline_data.get(symbol, [])
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
            'demo_mode': self.demo_mode
        }

    def _initialize_demo_prices(self) -> None:
        """Initialize demo prices for simulation."""
        # Realistic starting prices for major crypto pairs
        demo_prices = {
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
            if symbol in demo_prices:
                self.demo_prices[symbol] = demo_prices[symbol]
            else:
                # Generate random price for unknown symbols
                self.demo_prices[symbol] = random.uniform(1.0, 100.0)

    def _run_demo_mode(self) -> None:
        """Run demo mode with simulated market data."""
        self.logger.info("🔸 Starting demo data generation...")
        
        while self.is_running:
            try:
                for symbol in self.symbols:
                    self._generate_demo_orderbook(symbol)
                    self._generate_demo_trade(symbol)
                    
                    # Generate kline every minute
                    if int(time.time()) % 60 == 0:
                        self._generate_demo_kline(symbol)
                
                # Update every 100ms to simulate high-frequency data
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Error in demo mode: {e}")
                time.sleep(1)

    def _generate_demo_orderbook(self, symbol: str) -> None:
        """Generate simulated order book data."""
        current_price = self.demo_prices[symbol]
        
        # Add some random price movement (-0.1% to +0.1%)
        price_change = random.uniform(-0.001, 0.001)
        new_price = current_price * (1 + price_change)
        self.demo_prices[symbol] = new_price
        
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

    def _generate_demo_trade(self, symbol: str) -> None:
        """Generate simulated trade data."""
        current_price = self.demo_prices[symbol]
        
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

    def _generate_demo_kline(self, symbol: str) -> None:
        """Generate simulated kline data."""
        current_price = self.demo_prices[symbol]
        
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
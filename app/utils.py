"""
Utility functions for the Helios Trading System.
"""

import logging
import time
import json
import hashlib
import hmac
from typing import Dict, Any, List, Optional
import redis
import numpy as np
from datetime import datetime

def setup_logging(log_level: str = "INFO") -> None:
    """
    Set up logging configuration for the system.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/helios.log'),
            logging.StreamHandler()
        ]
    )

def get_timestamp() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)

def calculate_nobi(bids: List[tuple], asks: List[tuple], depth: int = 10) -> float:
    """
    Calculate Normalized Order Book Imbalance (NOBI).
    
    Args:
        bids: List of (price, quantity) tuples for bid side
        asks: List of (price, quantity) tuples for ask side
        depth: Number of levels to consider
        
    Returns:
        NOBI value between -1 and 1
    """
    if not bids or not asks:
        return 0.0
    
    # Calculate depth on each side
    depth_bid = sum(price * quantity for price, quantity in bids[:depth])
    depth_ask = sum(price * quantity for price, quantity in asks[:depth])
    
    # Calculate NOBI
    if depth_bid + depth_ask == 0:
        return 0.0
    
    nobi = (depth_ask - depth_bid) / (depth_ask + depth_bid)
    return nobi

def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """
    Calculate Average True Range (ATR).
    
    Args:
        highs: List of high prices
        lows: List of low prices
        closes: List of close prices
        period: Period for ATR calculation
        
    Returns:
        ATR value
    """
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return 0.0
    
    true_ranges = []
    for i in range(1, len(highs)):
        tr1 = highs[i] - lows[i]
        tr2 = abs(highs[i] - closes[i-1])
        tr3 = abs(lows[i] - closes[i-1])
        true_ranges.append(max(tr1, tr2, tr3))
    
    if len(true_ranges) < period:
        return 0.0
    
    return sum(true_ranges[-period:]) / period

def calculate_ema(prices: List[float], period: int) -> float:
    """
    Calculate Exponential Moving Average (EMA).
    
    Args:
        prices: List of prices
        period: Period for EMA calculation
        
    Returns:
        EMA value
    """
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0.0
    
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period  # Start with SMA
    
    for price in prices[period:]:
        ema = (price * multiplier) + (ema * (1 - multiplier))
    
    return ema

def calculate_rsi(prices: List[float], period: int = 14) -> float:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        prices: List of prices
        period: Period for RSI calculation
        
    Returns:
        RSI value between 0 and 100
    """
    if len(prices) < period + 1:
        return 50.0  # Neutral value
    
    gains = []
    losses = []
    
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    if len(gains) < period:
        return 50.0
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def kelly_criterion(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """
    Calculate Kelly Criterion percentage.
    
    Args:
        win_rate: Historical win rate (0-1)
        avg_win: Average winning trade amount
        avg_loss: Average losing trade amount
        
    Returns:
        Kelly percentage (0-1)
    """
    if avg_loss == 0 or win_rate == 0:
        return 0.0
    
    win_loss_ratio = avg_win / avg_loss
    kelly_pct = win_rate - ((1 - win_rate) / win_loss_ratio)
    
    # Ensure Kelly percentage is within reasonable bounds
    return max(0.0, min(1.0, kelly_pct))

class RedisManager:
    """Redis connection and data management."""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.Redis(
                host=host, 
                port=port, 
                db=db, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.redis_client.ping()
            logging.info(f"Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logging.error(f"Failed to connect to Redis: {e}")
            raise
    
    def set_data(self, key: str, data: Dict[str, Any], expiry: Optional[int] = None) -> bool:
        """
        Store data in Redis with optional expiry.
        
        Args:
            key: Redis key
            data: Data to store
            expiry: Expiry time in seconds
            
        Returns:
            True if successful
        """
        try:
            json_data = json.dumps(data)
            if expiry:
                return self.redis_client.setex(key, expiry, json_data)
            else:
                return self.redis_client.set(key, json_data)
        except Exception as e:
            logging.error(f"Failed to set Redis data: {e}")
            return False
    
    def get_data(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve data from Redis.
        
        Args:
            key: Redis key
            
        Returns:
            Data dictionary or None if not found
        """
        try:
            data = self.redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logging.error(f"Failed to get Redis data: {e}")
            return None
    
    def publish(self, channel: str, message: Dict[str, Any]) -> bool:
        """
        Publish message to Redis channel.
        
        Args:
            channel: Channel name
            message: Message to publish
            
        Returns:
            True if successful
        """
        try:
            json_message = json.dumps(message)
            self.redis_client.publish(channel, json_message)
            return True
        except Exception as e:
            logging.error(f"Failed to publish to Redis: {e}")
            return False
    
    def subscribe(self, channels: List[str]):
        """
        Subscribe to Redis channels.
        
        Args:
            channels: List of channel names
            
        Returns:
            Redis pubsub object
        """
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channels)
        return pubsub

def create_binance_signature(query_string: str, secret: str) -> str:
    """
    Create signature for Binance API requests.
    
    Args:
        query_string: Query parameters string
        secret: API secret
        
    Returns:
        HMAC SHA256 signature
    """
    return hmac.new(
        secret.encode('utf-8'), 
        query_string.encode('utf-8'), 
        hashlib.sha256
    ).hexdigest()

def validate_symbol(symbol: str) -> bool:
    """
    Validate cryptocurrency symbol format.
    
    Args:
        symbol: Trading symbol (e.g., BTCUSDT)
        
    Returns:
        True if valid format
    """
    if not symbol or len(symbol) < 6:
        return False
    
    # Basic validation - should end with USDT for futures
    return symbol.endswith('USDT') and len(symbol) <= 12

def format_price(price: float, precision: int = 8) -> str:
    """
    Format price with appropriate precision.
    
    Args:
        price: Price value
        precision: Decimal precision
        
    Returns:
        Formatted price string
    """
    return f"{price:.{precision}f}".rstrip('0').rstrip('.')

def calculate_position_size(balance: float, risk_percentage: float, entry_price: float, stop_loss: float) -> float:
    """
    Calculate position size based on risk management.
    
    Args:
        balance: Account balance
        risk_percentage: Risk percentage (0-1)
        entry_price: Entry price
        stop_loss: Stop loss price
        
    Returns:
        Position size
    """
    if entry_price == 0 or stop_loss == 0 or entry_price == stop_loss:
        return 0.0
    
    risk_amount = balance * risk_percentage
    price_diff = abs(entry_price - stop_loss)
    position_size = risk_amount / price_diff
    
    return position_size

class CircularBuffer:
    """Circular buffer for storing time series data efficiently."""
    
    def __init__(self, size: int):
        """Initialize circular buffer with fixed size."""
        self.size = size
        self.buffer = [0.0] * size
        self.index = 0
        self.count = 0
    
    def append(self, value: float) -> None:
        """Add value to buffer."""
        self.buffer[self.index] = value
        self.index = (self.index + 1) % self.size
        if self.count < self.size:
            self.count += 1
    
    def get_values(self) -> List[float]:
        """Get all values in chronological order."""
        if self.count < self.size:
            return self.buffer[:self.count]
        
        return self.buffer[self.index:] + self.buffer[:self.index]
    
    def get_latest(self, n: int) -> List[float]:
        """Get the latest n values."""
        values = self.get_values()
        return values[-n:] if len(values) >= n else values
    
    def is_full(self) -> bool:
        """Check if buffer is full."""
        return self.count == self.size
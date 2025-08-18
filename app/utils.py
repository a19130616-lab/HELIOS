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

class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles numpy data types."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

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
            json_data = json.dumps(data, cls=NumpyEncoder)
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
            json_message = json.dumps(message, cls=NumpyEncoder)
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
    
    def get_keys(self, pattern: str) -> List[str]:
        """
        Get Redis keys matching a pattern.
        
        Args:
            pattern: Key pattern with wildcards
            
        Returns:
            List of matching keys
        """
        try:
            return self.redis_client.keys(pattern)
        except Exception as e:
            logging.error(f"Failed to get Redis keys: {e}")
            return []

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

# Enhanced Trend Analysis Functions

def calculate_slope(prices: List[float], lookback: int = 14) -> float:
    """
    Calculate the slope of price movement over a lookback period.
    
    Args:
        prices: List of price values
        lookback: Number of periods to look back
        
    Returns:
        Slope value (positive = uptrend, negative = downtrend)
    """
    if len(prices) < lookback:
        return 0.0
    
    recent_prices = prices[-lookback:]
    x = list(range(len(recent_prices)))
    y = recent_prices
    
    # Simple linear regression
    n = len(x)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x_squared = sum(xi * xi for xi in x)
    
    if n * sum_x_squared - sum_x * sum_x == 0:
        return 0.0
    
    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x)
    return slope


def classify_trend(short_ema: float, long_ema: float, slope: float, 
                  slope_threshold: float = 0.001) -> int:
    """
    Classify trend direction based on EMAs and slope.
    
    Args:
        short_ema: Short-period EMA value
        long_ema: Long-period EMA value
        slope: Price slope
        slope_threshold: Minimum slope for trend confirmation
        
    Returns:
        1 for uptrend, -1 for downtrend, 0 for sideways
    """
    # EMA trend
    if short_ema > long_ema and slope > slope_threshold:
        return 1  # Uptrend
    elif short_ema < long_ema and slope < -slope_threshold:
        return -1  # Downtrend
    else:
        return 0  # Sideways


def calculate_momentum(prices: List[float], period: int = 14) -> float:
    """
    Calculate momentum as rate of change.
    
    Args:
        prices: List of price values
        period: Momentum period
        
    Returns:
        Momentum value as percentage change
    """
    if len(prices) < period + 1:
        return 0.0
    
    current_price = prices[-1]
    past_price = prices[-(period + 1)]
    
    if past_price == 0:
        return 0.0
    
    return (current_price - past_price) / past_price * 100


def calculate_volatility_percentile(atr_values: List[float], current_atr: float) -> float:
    """
    Calculate the percentile of current ATR relative to historical ATR values.
    
    Args:
        atr_values: Historical ATR values
        current_atr: Current ATR value
        
    Returns:
        Percentile (0-100)
    """
    if not atr_values or len(atr_values) < 10:
        return 50.0  # Default to median
    
    sorted_atr = sorted(atr_values)
    
    # Find position of current ATR
    position = 0
    for atr in sorted_atr:
        if current_atr > atr:
            position += 1
        else:
            break
    
    percentile = (position / len(sorted_atr)) * 100
    return min(max(percentile, 0.0), 100.0)


def classify_volatility_regime(atr_percentile: float, low_threshold: float = 25.0, 
                              high_threshold: float = 75.0) -> str:
    """
    Classify volatility regime based on ATR percentile.
    
    Args:
        atr_percentile: ATR percentile (0-100)
        low_threshold: Low volatility threshold
        high_threshold: High volatility threshold
        
    Returns:
        'low', 'normal', or 'high' volatility regime
    """
    if atr_percentile < low_threshold:
        return 'low'
    elif atr_percentile > high_threshold:
        return 'high'
    else:
        return 'normal'


def calculate_multi_timeframe_trend(klines_data: Dict[str, List], 
                                   short_period: int = 21, 
                                   long_period: int = 55) -> Dict[str, int]:
    """
    Calculate trend direction across multiple timeframes.
    
    Args:
        klines_data: Dictionary with timeframe keys and kline lists
        short_period: Short EMA period
        long_period: Long EMA period
        
    Returns:
        Dictionary with trend direction for each timeframe
    """
    trends = {}
    
    for timeframe, klines in klines_data.items():
        if not klines or len(klines) < long_period:
            trends[timeframe] = 0
            continue
        
        # Extract closing prices
        closes = [float(kline[4]) for kline in klines]  # Assuming [open, high, low, close, volume, ...]
        
        # Calculate EMAs
        short_ema = calculate_ema(closes, short_period)
        long_ema = calculate_ema(closes, long_period)
        
        # Calculate slope
        slope = calculate_slope(closes, 14)
        
        # Classify trend
        trend = classify_trend(short_ema, long_ema, slope)
        trends[timeframe] = trend
    
    return trends


def calculate_trend_alignment(trends: Dict[str, int], 
                            required_timeframes: List[str] = None) -> Dict[str, any]:
    """
    Calculate trend alignment across timeframes.
    
    Args:
        trends: Dictionary of trend directions by timeframe
        required_timeframes: List of timeframes that must agree
        
    Returns:
        Dictionary with alignment metrics
    """
    if not trends:
        return {
            'aligned': False,
            'alignment_score': 0.0,
            'majority_trend': 0,
            'agreement_ratio': 0.0
        }
    
    # Filter to required timeframes if specified
    if required_timeframes:
        filtered_trends = {tf: trends[tf] for tf in required_timeframes if tf in trends}
    else:
        filtered_trends = trends
    
    if not filtered_trends:
        return {
            'aligned': False,
            'alignment_score': 0.0,
            'majority_trend': 0,
            'agreement_ratio': 0.0
        }
    
    trend_values = list(filtered_trends.values())
    
    # Count trend directions
    uptrend_count = trend_values.count(1)
    downtrend_count = trend_values.count(-1)
    sideways_count = trend_values.count(0)
    
    total_timeframes = len(trend_values)
    
    # Determine majority trend
    if uptrend_count > downtrend_count and uptrend_count > sideways_count:
        majority_trend = 1
        agreement_count = uptrend_count
    elif downtrend_count > uptrend_count and downtrend_count > sideways_count:
        majority_trend = -1
        agreement_count = downtrend_count
    else:
        majority_trend = 0
        agreement_count = sideways_count
    
    # Calculate alignment metrics
    agreement_ratio = agreement_count / total_timeframes
    alignment_score = agreement_ratio * abs(majority_trend) if majority_trend != 0 else agreement_ratio * 0.5
    
    # Consider aligned if majority agrees and it's a strong trend
    aligned = agreement_ratio >= 0.6 and majority_trend != 0
    
    return {
        'aligned': aligned,
        'alignment_score': alignment_score,
        'majority_trend': majority_trend,
        'agreement_ratio': agreement_ratio,
        'uptrend_count': uptrend_count,
        'downtrend_count': downtrend_count,
        'sideways_count': sideways_count,
        'timeframes_analyzed': list(filtered_trends.keys())
    }


def calculate_trend_strength(price_data: List[float], volume_data: List[float] = None, 
                           period: int = 14) -> float:
    """
    Calculate trend strength based on price movement and volume.
    
    Args:
        price_data: List of price values
        volume_data: Optional list of volume values
        period: Calculation period
        
    Returns:
        Trend strength (0-1, higher = stronger trend)
    """
    if len(price_data) < period:
        return 0.0
    
    recent_prices = price_data[-period:]
    
    # Calculate price momentum
    momentum = calculate_momentum(recent_prices, period // 2)
    momentum_strength = min(abs(momentum) / 10.0, 1.0)  # Normalize to 0-1
    
    # Calculate consistency (lower volatility relative to trend = higher strength)
    slope = calculate_slope(recent_prices, period)
    
    if len(recent_prices) > 1:
        price_changes = [abs(recent_prices[i] - recent_prices[i-1]) for i in range(1, len(recent_prices))]
        avg_change = sum(price_changes) / len(price_changes) if price_changes else 0
        
        # Trend strength is momentum relative to average volatility
        if avg_change > 0:
            consistency = abs(slope) / avg_change
            consistency = min(consistency, 1.0)
        else:
            consistency = 1.0 if slope != 0 else 0.0
    else:
        consistency = 0.0
    
    # Volume confirmation (if available)
    volume_confirmation = 1.0
    if volume_data and len(volume_data) >= period:
        recent_volumes = volume_data[-period:]
        volume_trend = calculate_slope(recent_volumes, period // 2)
        
        # Strong trends should have increasing volume
        if (slope > 0 and volume_trend > 0) or (slope < 0 and volume_trend > 0):
            volume_confirmation = 1.2
        elif (slope > 0 and volume_trend < 0) or (slope < 0 and volume_trend < 0):
            volume_confirmation = 0.8
    
    # Combine metrics
    strength = (momentum_strength * 0.4 + consistency * 0.6) * volume_confirmation
    return min(max(strength, 0.0), 1.0)
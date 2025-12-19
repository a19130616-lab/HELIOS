"""
Signal Engine for Helios Trading System
Implements the NOBI calculation and ML-based signal generation.
"""

import logging
import time
import threading
import json
import pickle
import numpy as np
import uuid
from typing import Dict, List, Optional, Any
from queue import Queue
import xgboost as xgb

from app.models import TradingSignal, SignalDirection, OrderBookSnapshot, MarketRegime
from app.utils import (RedisManager, calculate_nobi, calculate_ema, calculate_rsi, get_timestamp, CircularBuffer,
                       calculate_slope, classify_trend, calculate_momentum, calculate_multi_timeframe_trend,
                       calculate_trend_alignment, calculate_vwap)
from app.config_manager import get_config
from app.decision_logger import CSVDecisionLogger

class SignalEngine:
    """
    Core signal generation engine implementing NOBI calculation and ML filtering.
    Uses a trigger-filter mechanism for efficient signal generation.
    """
    
    def __init__(self, redis_manager: RedisManager, symbols: List[str]):
        """
        Initialize the Signal Engine.
        
        Args:
            redis_manager: Redis manager instance
            symbols: List of symbols to monitor
        """
        self.redis_manager = redis_manager
        self.symbols = symbols
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Configuration parameters
        self.nobi_depth = self.config.get('signals', 'nobi_depth', int)
        self.nobi_threshold = self.config.get('signals', 'nobi_trigger_threshold', float)
        self.ml_confidence_threshold = self.config.get('signals', 'ml_confidence_threshold', float)
        self.ml_model_path = self.config.get('signals', 'ml_model_path')
        
        # Machine Learning model
        self.ml_model = None
        self.feature_scaler = None
        self._load_ml_model()
        
        # 10-minute strategy / aggregation mode (feature-flagged)
        try:
            # Use config flag to enable 10-minute mode (fallback False)
            # Fix: Pass bool type to get() to ensure "false" string is parsed as False boolean
            self.use_10m_mode = self.config.get('signals', 'use_10m_mode', bool, fallback=False)
        except Exception:
            self.use_10m_mode = False
        try:
            self.confirm_n = int(self.config.get('signals', 'confirm_n', fallback=2))
            self.confirm_m = int(self.config.get('signals', 'confirm_m', fallback=3))
        except Exception:
            self.confirm_n = 2
            self.confirm_m = 3
        # Per-symbol recent confirmation history (stores last confirm_m directions as 'LONG'/'SHORT')
        self.ten_min_history = {symbol: [] for symbol in symbols}
        self._last_10m_run = 0
        # Expose lightweight metrics for dashboard
        self.ten_min_metrics = {symbol: {'confirmed_signals': 0, 'last_confirm_ts': 0} for symbol in symbols}
        
        # Signal generation state
        self.is_running = False
        self.signal_queue = Queue()
        
        # Data buffers for feature engineering
        self.price_buffers = {symbol: CircularBuffer(100) for symbol in symbols}
        self.nobi_buffers = {symbol: CircularBuffer(50) for symbol in symbols}
        self.volume_buffers = {symbol: CircularBuffer(100) for symbol in symbols}
        self.last_trade_ids = {} # Track last processed trade to avoid duplicates
        
        # Market regime tracking
        self.market_regimes = {symbol: MarketRegime.UNKNOWN for symbol in symbols}
        # Performance tracking
        self.signals_generated = 0
        self.last_signal_time = {}
        
        # Decision Logger
        self.decision_logger = CSVDecisionLogger()
        
    def start(self) -> None:
        """Start the signal generation engine."""
        self.logger.info("Starting Signal Engine...")
        self.is_running = True
        
        # Start main processing loop
        self.processing_thread = threading.Thread(target=self._run_signal_loop, daemon=True)
        self.processing_thread.start()
        
        # Start market regime monitoring
        self.regime_thread = threading.Thread(target=self._monitor_market_regimes, daemon=True)
        self.regime_thread.start()
        
        self.logger.info("Signal Engine started")
    
    def stop(self) -> None:
        """Stop the signal generation engine."""
        self.logger.info("Stopping Signal Engine...")
        self.is_running = False
        self.logger.info("Signal Engine stopped")
    
    def _load_ml_model(self) -> None:
        """Load the pre-trained ML model and feature scaler."""
        try:
            # Check if model path exists
            if not self.ml_model_path:
                self.logger.info("No ML model path configured - operating without ML filtering")
                return
                
            import os
            if not os.path.exists(self.ml_model_path):
                self.logger.info(f"ML model file not found at {self.ml_model_path} - operating without ML filtering")
                return
            
            # Try to load XGBoost model
            self.ml_model = xgb.Booster()
            self.ml_model.load_model(self.ml_model_path)
            self.logger.info(f"Loaded ML model from {self.ml_model_path}")
            
            # Try to load feature scaler
            scaler_path = self.ml_model_path.replace('.json', '_scaler.pkl')
            try:
                with open(scaler_path, 'rb') as f:
                    self.feature_scaler = pickle.load(f)
                self.logger.info(f"Loaded feature scaler from {scaler_path}")
            except FileNotFoundError:
                self.logger.warning(f"Feature scaler not found at {scaler_path}")
                
        except Exception as e:
            self.logger.error(f"Failed to load ML model: {e}")
            self.logger.warning("Signal Engine will operate without ML filtering")
            self.ml_model = None
            self.feature_scaler = None
    
    def _run_signal_loop(self) -> None:
        """Main signal generation loop."""
        while self.is_running:
            try:
                if self.use_10m_mode:
                    # Run coarse 10-minute processing. Check boundary every second and execute once per 10m.
                    now = get_timestamp()
                    # Ensure we run once per 10 minutes (600_000 ms)
                    if now - getattr(self, '_last_10m_run', 0) >= 10 * 60 * 1000:
                        for symbol in self.symbols:
                            try:
                                self._process_symbol_signals_10m(symbol)
                            except Exception as e:
                                self.logger.error(f"Error processing 10m symbol {symbol}: {e}")
                        self._last_10m_run = now
                    # Light sleep while waiting for 10m boundary
                    time.sleep(1)
                else:
                    # Default: fine-grained processing
                    for symbol in self.symbols:
                        self._process_symbol_signals(symbol)
                    # Reduced latency from 0.1s to 0.01s (10ms)
                    time.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"Error in signal loop: {e}")
                time.sleep(1)
    
    def _process_symbol_signals(self, symbol: str) -> None:
        """
        Process signals for a specific symbol.
        
        Args:
            symbol: Trading symbol
        """
        # Get latest order book data
        order_book_data = self._get_latest_order_book(symbol)
        if not order_book_data:
            return
        
        # Calculate NOBI
        nobi_value = self._calculate_nobi_from_data(order_book_data)
        if nobi_value is None:
            return
        
        # Update data buffers
        self._update_buffers(symbol, order_book_data, nobi_value)
        
        # Check for NOBI trigger
        if abs(nobi_value) >= self.nobi_threshold:
            self._handle_nobi_trigger(symbol, nobi_value, order_book_data)
    
    def _process_symbol_signals_10m(self, symbol: str) -> None:
        """
        10-minute aggregated processing for a specific symbol.
        This uses coarser price / kline features (from Redis klines) and a persistent-confirmation
        (N-of-M) rule before emitting signals. Intended to be conservative and produce higher
        win-rate, lower-frequency signals.
        """
        try:
            # Fetch recent 1m klines from Redis (stored as JSON strings lpush newest first)
            raw_klines = []
            try:
                raw_klines = self.redis_manager.redis_client.lrange(f"klines:{symbol}", 0, 199)
            except Exception:
                raw_klines = []
            if not raw_klines:
                return
            
            # Parse into chronological closes
            closes = []
            highs = []
            lows = []
            volumes = []
            # Redis list is newest first; reverse for chronological order
            for raw in reversed(raw_klines):
                try:
                    k = json.loads(raw)
                    close = float(k.get('close') or k.get('price') or 0)
                    high = float(k.get('high', close))
                    low = float(k.get('low', close))
                    volume = float(k.get('volume', 0.0))
                    closes.append(close)
                    highs.append(high)
                    lows.append(low)
                    volumes.append(volume)
                except Exception:
                    continue
            
            if len(closes) < 10:
                # Not enough history to form a 10m view
                return
            
            # Use last 10 1m candles as approximate 10m window
            window_closes = closes[-10:]
            window_highs = highs[-10:]
            window_lows = lows[-10:]
            window_vols = volumes[-10:]
            
            # Feature heuristics (10m)
            ema_short = calculate_ema(window_closes, min(3, len(window_closes)))
            ema_long = calculate_ema(closes[-30:] if len(closes) >= 30 else closes, min(10, max(3, len(closes))))
            rsi = calculate_rsi(closes, period=14)
            atr = calculate_atr(window_highs, window_lows, window_closes, period=min(10, len(window_closes)-1))
            momentum = calculate_momentum(window_closes, period=5)
            
            # Determine directional suggestion
            direction_suggestion = None
            if ema_short > ema_long and momentum > 0:
                direction_suggestion = SignalDirection.LONG
            elif ema_short < ema_long and momentum < 0:
                direction_suggestion = SignalDirection.SHORT
            else:
                # No clear direction at 10m
                return
            
            # Persistent confirmation: push direction into history and require N of M agreement
            hist = self.ten_min_history.get(symbol, [])
            hist.append(direction_suggestion.value)
            # Keep at most confirm_m entries
            hist = hist[-self.confirm_m:]
            self.ten_min_history[symbol] = hist
            
            same_dir_count = sum(1 for v in hist if v == direction_suggestion.value)
            if same_dir_count < self.confirm_n:
                # Not yet confirmed across required bars
                return
            
            # Build a simple feature vector for ML if available (fallback to heuristic confidence)
            features = []
            try:
                # Basic features: last NOBI if present (non-mandatory), momentum, rsi normalized, atr normalized
                # NOBI not always available for aggregated 10m, so we skip if missing
                # Use momentum (scaled), rsi/100, atr normalized by price
                last_price = window_closes[-1]
                features.append(momentum)
                features.append(rsi / 100.0)
                features.append(atr / last_price if last_price > 0 else 0.0)
                # Add trend alignment placeholder (1.0 strong)
                features.append(1.0 if ema_short > ema_long else -1.0)
                features = np.array(features, dtype=np.float32)
                if self.feature_scaler:
                    features = self.feature_scaler.transform(features.reshape(1, -1))[0]
            except Exception:
                features = None
            
            # Get ML confidence if model exists
            confidence = 0.0
            if self.ml_model is not None and features is not None:
                try:
                    confidence = self._get_ml_prediction(features)
                except Exception:
                    confidence = 0.0
            else:
                # Heuristic fallback: combine momentum magnitude and normalized rsi distance from 50
                conf = min(1.0, max(0.0, (abs(momentum) / 5.0) + (abs(rsi - 50) / 100.0)))
                confidence = conf
            
            # Only publish if confidence meets threshold
            if confidence >= self.ml_confidence_threshold:
                signal = self._create_trading_signal(symbol, direction_suggestion, nobi_value=0.0, confidence=confidence, order_book_data={'mid_price': window_closes[-1]})
                self._publish_signal(signal)
                self.last_signal_time[symbol] = get_timestamp()
                self.signals_generated += 1
                # Update 10m metrics
                self.ten_min_metrics[symbol]['confirmed_signals'] += 1
                self.ten_min_metrics[symbol]['last_confirm_ts'] = get_timestamp()
                
                self.logger.info(f"[10m] Generated {direction_suggestion.value} signal for {symbol} - Confidence: {confidence:.3f}")
            
        except Exception as e:
            self.logger.error(f"Error in 10m processing for {symbol}: {e}")
            return
    
    def _get_latest_order_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get latest order book data from Redis.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Order book data or None
        """
        key = f"orderbook:{symbol}"
        return self.redis_manager.get_data(key)
    
    def _calculate_nobi_from_data(self, order_book_data: Dict[str, Any]) -> Optional[float]:
        """
        Calculate NOBI from order book data.
        
        Args:
            order_book_data: Order book data from Redis
            
        Returns:
            NOBI value or None if calculation fails
        """
        try:
            bids = order_book_data.get('bids', [])
            asks = order_book_data.get('asks', [])
            
            if not bids or not asks:
                return None
            
            # Convert to tuples for calculate_nobi function
            bid_tuples = [(float(bid[0]), float(bid[1])) for bid in bids]
            ask_tuples = [(float(ask[0]), float(ask[1])) for ask in asks]
            
            return calculate_nobi(bid_tuples, ask_tuples, self.nobi_depth)
            
        except Exception as e:
            self.logger.error(f"Error calculating NOBI: {e}")
            return None
    
    def _update_buffers(self, symbol: str, order_book_data: Dict[str, Any], nobi_value: float) -> None:
        """
        Update data buffers for feature engineering.
        
        Args:
            symbol: Trading symbol
            order_book_data: Order book data
            nobi_value: Calculated NOBI value
        """
        # Update price buffer
        mid_price = order_book_data.get('mid_price', 0)
        if mid_price > 0:
            self.price_buffers[symbol].append(mid_price)
        
        # Update NOBI buffer
        self.nobi_buffers[symbol].append(nobi_value)
        
        # Get trade data for volume - ONLY append if it's a new trade
        trade_data = self.redis_manager.get_data(f"trade:{symbol}")
        if trade_data:
            trade_id = trade_data.get('trade_id')
            # Check if we already processed this trade
            if trade_id and trade_id != self.last_trade_ids.get(symbol):
                volume = trade_data.get('quantity', 0)
                self.volume_buffers[symbol].append(volume)
                self.last_trade_ids[symbol] = trade_id

    def _handle_nobi_trigger(self, symbol: str, nobi_value: float, order_book_data: Dict[str, Any]) -> None:
        """
        Handle NOBI trigger event - run ML filter and potentially generate signal.
        
        Args:
            symbol: Trading symbol
            nobi_value: NOBI value that triggered
            order_book_data: Order book data
        """
        # Prevent signal spamming
        if self._is_too_soon_for_signal(symbol):
            return

        try:
            # Determine signal direction
            direction = SignalDirection.SHORT if nobi_value > 0 else SignalDirection.LONG
            
            # --- Volume-Price Strategy Optimization ---
            # 1. Volume Spike Confirmation (Whale Tracking)
            # We only trade if the current trade size is significantly larger than the recent average.
            volumes = self.volume_buffers[symbol].get_latest(20)
            current_vol = 0
            avg_vol = 0
            vol_ratio = 0.0
            
            if len(volumes) >= 5:
                current_vol = volumes[-1]
                # Calculate average of previous trades (excluding current)
                avg_vol = sum(volumes[:-1]) / (len(volumes)-1) if len(volumes) > 1 else current_vol
                vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0
                
                # Filter: Require trade size to be at least 1.5x average (Significant interest)
                if avg_vol > 0 and current_vol < (avg_vol * 1.5):
                    # Log rejection
                    self.decision_logger.log_decision({
                        "timestamp": get_timestamp(),
                        "symbol": symbol,
                        "price": order_book_data.get('mid_price', 0),
                        "nobi": nobi_value,
                        "rsi": 0, # Not calculated here yet
                        "volume_ratio": f"{vol_ratio:.2f}",
                        "vwap_gap_pct": 0,
                        "ml_confidence": 0,
                        "decision": "REJECT",
                        "reason": "Low Volume"
                    })
                    return

            # 2. VWAP Confirmation (Fair Price)
            prices = self.price_buffers[symbol].get_latest(20)
            vwap_gap = 0.0
            
            # Ensure we have matching data length for VWAP calculation
            min_len = min(len(prices), len(volumes))
            if min_len >= 5:
                vwap = calculate_vwap(prices[-min_len:], volumes[-min_len:])
                current_price = prices[-1]
                if vwap > 0:
                    vwap_gap = ((current_price - vwap) / vwap) * 100
                
                # Filter: Avoid buying if price is significantly above VWAP (Overextended)
                # Filter: Avoid selling if price is significantly below VWAP (Oversold)
                if direction == SignalDirection.LONG and current_price > vwap * 1.005: # 0.5% buffer
                    self.decision_logger.log_decision({
                        "timestamp": get_timestamp(),
                        "symbol": symbol,
                        "price": current_price,
                        "nobi": nobi_value,
                        "rsi": 0,
                        "volume_ratio": f"{vol_ratio:.2f}",
                        "vwap_gap_pct": f"{vwap_gap:.4f}",
                        "ml_confidence": 0,
                        "decision": "REJECT",
                        "reason": "Price > VWAP (Overextended)"
                    })
                    return
                if direction == SignalDirection.SHORT and current_price < vwap * 0.995: # 0.5% buffer
                    self.decision_logger.log_decision({
                        "timestamp": get_timestamp(),
                        "symbol": symbol,
                        "price": current_price,
                        "nobi": nobi_value,
                        "rsi": 0,
                        "volume_ratio": f"{vol_ratio:.2f}",
                        "vwap_gap_pct": f"{vwap_gap:.4f}",
                        "ml_confidence": 0,
                        "decision": "REJECT",
                        "reason": "Price < VWAP (Oversold)"
                    })
                    return
            # ------------------------------------------
            
            # Create feature vector for ML model
            features = self._create_feature_vector(symbol, nobi_value, order_book_data)
            if features is None:
                return
            
            # Get ML prediction if model is available
            confidence = self._get_ml_prediction(features)
            
            # Check confidence threshold
            if confidence >= self.ml_confidence_threshold:
                signal = self._create_trading_signal(symbol, direction, nobi_value, confidence, order_book_data)
                self._publish_signal(signal)
                self.last_signal_time[symbol] = get_timestamp()
                self.signals_generated += 1
                
                self.decision_logger.log_decision({
                    "timestamp": get_timestamp(),
                    "symbol": symbol,
                    "price": order_book_data.get('mid_price', 0),
                    "nobi": nobi_value,
                    "rsi": 0,
                    "volume_ratio": f"{vol_ratio:.2f}",
                    "vwap_gap_pct": f"{vwap_gap:.4f}",
                    "ml_confidence": f"{confidence:.3f}",
                    "decision": f"SIGNAL_{direction.value}",
                    "reason": "ML Confidence Met"
                })
                
                self.logger.info(f"Generated {direction.value} signal for {symbol} - "
                               f"NOBI: {nobi_value:.4f}, Confidence: {confidence:.3f}")
            else:
                self.decision_logger.log_decision({
                    "timestamp": get_timestamp(),
                    "symbol": symbol,
                    "price": order_book_data.get('mid_price', 0),
                    "nobi": nobi_value,
                    "rsi": 0,
                    "volume_ratio": f"{vol_ratio:.2f}",
                    "vwap_gap_pct": f"{vwap_gap:.4f}",
                    "ml_confidence": f"{confidence:.3f}",
                    "decision": "REJECT",
                    "reason": f"Low ML Confidence ({confidence:.3f})"
                })
                self.signals_generated += 1
                
                self.logger.info(f"Generated {direction.value} signal for {symbol} - "
                               f"NOBI: {nobi_value:.4f}, Confidence: {confidence:.3f}")
        
        except Exception as e:
            self.logger.error(f"Error handling NOBI trigger for {symbol}: {e}")
    
    def _is_too_soon_for_signal(self, symbol: str, min_interval_ms: int = 30000) -> bool:
        """
        Check if it's too soon to generate another signal for this symbol.
        
        Args:
            symbol: Trading symbol
            min_interval_ms: Minimum interval between signals in milliseconds
            
        Returns:
            True if too soon
        """
        last_time = self.last_signal_time.get(symbol, 0)
        return (get_timestamp() - last_time) < min_interval_ms
    
    def _create_feature_vector(self, symbol: str, nobi_value: float, order_book_data: Dict[str, Any]) -> Optional[np.ndarray]:
        """
        Create feature vector for ML model.
        
        Args:
            symbol: Trading symbol
            nobi_value: Current NOBI value
            order_book_data: Order book data
            
        Returns:
            Feature vector as numpy array or None
        """
        try:
            features = []
            
            # NOBI features
            features.append(nobi_value)
            nobi_history = self.nobi_buffers[symbol].get_latest(10)
            if len(nobi_history) >= 5:
                features.append(np.mean(nobi_history))
                features.append(np.std(nobi_history))
            else:
                features.extend([0.0, 0.0])
            
            # Price features
            prices = self.price_buffers[symbol].get_latest(50)
            if len(prices) >= 20:
                # EMAs
                ema_20 = calculate_ema(prices, 20)
                ema_50 = calculate_ema(prices, min(50, len(prices)))
                features.append((prices[-1] - ema_20) / ema_20 if ema_20 > 0 else 0)
                features.append((ema_20 - ema_50) / ema_50 if ema_50 > 0 else 0)
                
                # RSI
                rsi = calculate_rsi(prices)
                features.append(rsi / 100.0)  # Normalize to 0-1
                
                # Volatility (price std)
                features.append(np.std(prices[-20:]) / np.mean(prices[-20:]) if len(prices) >= 20 else 0)
            else:
                features.extend([0.0, 0.0, 0.5, 0.0])  # Default values
            
            # Order book features
            spread = order_book_data.get('spread', 0)
            mid_price = order_book_data.get('mid_price', 1)
            features.append(spread / mid_price if mid_price > 0 else 0)  # Relative spread
            
            # Market regime
            regime = self.market_regimes.get(symbol, MarketRegime.UNKNOWN)
            features.append(1.0 if regime == MarketRegime.TRENDING else 0.0)
            
            # Volume features
            volumes = self.volume_buffers[symbol].get_latest(10)
            if len(volumes) >= 5:
                features.append(np.mean(volumes))
                features.append(volumes[-1] / np.mean(volumes) if np.mean(volumes) > 0 else 1.0)
            else:
                features.extend([0.0, 1.0])
            
            # Sentiment features
            sentiment_data = self.redis_manager.get_data(f"sentiment:{symbol}")
            if sentiment_data:
                features.append(sentiment_data.get('weighted_sentiment', 0.0))
                features.append(sentiment_data.get('sentiment_strength', 0.0))
                features.append(sentiment_data.get('post_count', 0) / 100.0)  # Normalize post count
            else:
                features.extend([0.0, 0.0, 0.0])  # Default neutral sentiment
            
            # Enhanced trend analysis features
            if len(prices) >= 20:
                # Multi-timeframe trend analysis using simple EMAs
                short_prices = prices[-10:]
                medium_prices = prices[-20:]
                
                # Calculate simple trend direction using EMAs
                if len(short_prices) >= 5:
                    short_ema = calculate_ema(short_prices, 5)
                    short_trend_value = 1 if short_prices[-1] > short_ema else -1
                else:
                    short_trend_value = 0
                
                if len(medium_prices) >= 10:
                    medium_ema = calculate_ema(medium_prices, 10)
                    medium_trend_value = 1 if medium_prices[-1] > medium_ema else -1
                else:
                    medium_trend_value = 0
                
                # Simple alignment calculation
                if short_trend_value == medium_trend_value and short_trend_value != 0:
                    alignment = 1.0  # Strong alignment
                elif short_trend_value == medium_trend_value and short_trend_value == 0:
                    alignment = 0.0  # No trend
                else:
                    alignment = -0.5  # Conflicting signals
                
                features.append(alignment)
                
                # Momentum features
                momentum_5 = calculate_momentum(prices, 5)
                momentum_10 = calculate_momentum(prices, 10)
                features.append(momentum_5)
                features.append(momentum_10)
                
                # Trend classification
                if len(prices) >= 20:
                    short_ema = calculate_ema(prices[-20:], 10)
                    long_ema = calculate_ema(prices[-20:], 20)
                    slope = calculate_slope(prices[-20:], 14)
                    trend_class = classify_trend(short_ema, long_ema, slope)
                    features.append(float(trend_class))
                else:
                    features.append(0.0)
                
                # Slope features
                short_slope = calculate_slope(prices[-5:])
                medium_slope = calculate_slope(prices[-10:])
                features.append(short_slope)
                features.append(medium_slope)
            else:
                features.extend([0.0, 0.0, 0.0, 0.0, 0.0, 0.0])  # Default trend features
            
            # Time-based features
            current_time = get_timestamp()
            hour_of_day = (current_time // (1000 * 3600)) % 24
            features.append(np.sin(2 * np.pi * hour_of_day / 24))  # Cyclical hour encoding
            features.append(np.cos(2 * np.pi * hour_of_day / 24))
            
            feature_vector = np.array(features, dtype=np.float32)
            
            # Apply feature scaling if available
            if self.feature_scaler:
                feature_vector = self.feature_scaler.transform(feature_vector.reshape(1, -1))[0]
            
            return feature_vector
            
        except Exception as e:
            self.logger.error(f"Error creating feature vector for {symbol}: {e}")
            return None
    
    def _get_ml_prediction(self, features: np.ndarray) -> float:
        """
        Get ML model prediction.
        
        Args:
            features: Feature vector
            
        Returns:
            Confidence score (0-1)
        """
        if self.ml_model is None:
            # If no ML model, return confidence based on NOBI strength
            nobi_strength = abs(features[0]) if len(features) > 0 else 0
            return min(1.0, nobi_strength * 2)  # Simple fallback
        
        try:
            # Create DMatrix for XGBoost
            dmatrix = xgb.DMatrix(features.reshape(1, -1))
            
            # Get prediction
            prediction = self.ml_model.predict(dmatrix)[0]
            
            # Convert to confidence score (0-1)
            confidence = float(prediction)
            return max(0.0, min(1.0, confidence))
            
        except Exception as e:
            self.logger.error(f"Error getting ML prediction: {e}")
            return 0.0
    
    def _create_trading_signal(self, symbol: str, direction: SignalDirection, nobi_value: float, 
                             confidence: float, order_book_data: Dict[str, Any]) -> TradingSignal:
        """
        Create a trading signal.
        
        Args:
            symbol: Trading symbol
            direction: Signal direction
            nobi_value: NOBI value
            confidence: ML confidence score
            order_book_data: Order book data
            
        Returns:
            Trading signal
        """
        return TradingSignal(
            symbol=symbol,
            direction=direction,
            timestamp=get_timestamp(),
            confidence=confidence,
            nobi_value=nobi_value,
            entry_price=order_book_data.get('mid_price', 0)
        )
    
    def _publish_signal(self, signal: TradingSignal) -> None:
        """
        Publish trading signal to Redis queue.
        
        Args:
            signal: Trading signal to publish
        """
        # Correlation id for decision <-> execution status.
        if not getattr(signal, 'signal_id', None):
            signal.signal_id = uuid.uuid4().hex

        signal_data = {
            'symbol': signal.symbol,
            'direction': signal.direction.value,
            'timestamp': signal.timestamp,
            'confidence': signal.confidence,
            'nobi_value': signal.nobi_value,
            'entry_price': signal.entry_price,
            'signal_id': signal.signal_id,
        }
        
        # Add to local queue
        self.signal_queue.put(signal)
        
        # Publish to Redis
        self.redis_manager.publish("trading_signals", signal_data)
        
        self.logger.debug(f"Published signal: {signal_data}")
    
    def _monitor_market_regimes(self) -> None:
        """Monitor and update market regimes for all symbols."""
        while self.is_running:
            try:
                for symbol in self.symbols:
                    self._update_market_regime(symbol)
                
                # Update every 60 seconds
                time.sleep(60)
                
            except Exception as e:
                self.logger.error(f"Error monitoring market regimes: {e}")
                time.sleep(10)
    
    def _update_market_regime(self, symbol: str) -> None:
        """
        Update market regime for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        try:
            # Get recent kline data
            klines_data = self.redis_manager.redis_client.lrange(f"klines:{symbol}", 0, 99)
            if len(klines_data) < 50:
                return
            
            # Parse kline data and extract close prices
            prices = []
            for kline_json in reversed(klines_data):  # Reverse to get chronological order
                kline = json.loads(kline_json)
                prices.append(kline['close'])
            
            if len(prices) < 50:
                return
            
            # Calculate EMAs
            ema_20 = calculate_ema(prices, 20)
            ema_50 = calculate_ema(prices, 50)
            
            # Determine regime
            current_price = prices[-1]
            if abs(ema_20 - ema_50) / ema_50 > 0.02:  # 2% difference threshold
                if ema_20 > ema_50:
                    regime = MarketRegime.TRENDING
                else:
                    regime = MarketRegime.TRENDING
            else:
                regime = MarketRegime.RANGING
            
            self.market_regimes[symbol] = regime
            
        except Exception as e:
            self.logger.error(f"Error updating market regime for {symbol}: {e}")
    
    def get_signal(self) -> Optional[TradingSignal]:
        """
        Get the next signal from the queue.
        
        Returns:
            Next trading signal or None if queue is empty
        """
        try:
            return self.signal_queue.get_nowait()
        except:
            return None
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the Signal Engine.
        
        Returns:
            Health status information
        """
        try:
            return {
                'is_running': self.is_running,
                'symbols_monitored': len(self.symbols),
                'signals_generated': self.signals_generated,
                'ml_model_loaded': self.ml_model is not None,
                'feature_scaler_loaded': self.feature_scaler is not None,
                'signals_in_queue': self.signal_queue.qsize(),
                'market_regimes': {symbol: regime.value for symbol, regime in self.market_regimes.items()},
                # 10-minute mode indicators
                'ten_min_mode': bool(self.use_10m_mode),
                'ten_min_metrics': self.ten_min_metrics
            }
        except Exception as e:
            self.logger.error(f"Error getting signal engine health status: {e}")
            return {
                'is_running': self.is_running,
                'symbols_monitored': len(self.symbols)
            }
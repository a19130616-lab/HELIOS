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
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"DEBUG: SignalEngine initialized with symbols: {self.symbols}")
        
        self.config = get_config()
        
        # Configuration parameters
        self.nobi_depth = self.config.get('signals', 'nobi_depth', int)
        self.nobi_threshold = self.config.get('signals', 'nobi_trigger_threshold', float)
        self.ml_confidence_threshold = self.config.get('signals', 'ml_confidence_threshold', float)
        self.ml_model_path = self.config.get('signals', 'ml_model_path')

        # Volume-spike filter (whale confirmation)
        # If <= 1.0, the filter is effectively disabled.
        try:
            self.volume_spike_multiplier = float(self.config.get('signals', 'volume_spike_multiplier', float, fallback=1.5))
        except Exception:
            self.volume_spike_multiplier = 1.5

        # Regime gate: optional filter to avoid chop (best-ROI behavior tends to avoid ranging regimes).
        try:
            self.avoid_ranging_regime = self.config.get('signals', 'avoid_ranging_regime', bool, fallback=False)
        except Exception:
            self.avoid_ranging_regime = False
        try:
            self.market_regime_ema_diff_threshold = float(
                self.config.get('signals', 'market_regime_ema_diff_threshold', float, fallback=0.02)
            )
        except Exception:
            self.market_regime_ema_diff_threshold = 0.02
        
        # Machine Learning model
        self.ml_model = None
        self.feature_scaler = None
        self._load_ml_model()

        # --- Deprecated: hard-coded 10-minute mode ---
        # The old 10m mode depended on klines being present in Redis and only executed once per 10 minutes,
        # which can lead to long periods with no dashboard decisions.
        # We keep parsing the config for backward-compatibility, but the engine always runs the
        # fine-grained (orderbook/NOBI) loop and uses a configurable confirmation gate instead.
        try:
            _use_10m_mode_requested = bool(self.config.get('signals', 'use_10m_mode', bool, fallback=False))
        except Exception:
            _use_10m_mode_requested = False
        if _use_10m_mode_requested:
            self.logger.warning("signals.use_10m_mode is deprecated and ignored; using confirmation gate instead")
        self.use_10m_mode = False

        # Confirmation/cooldown knobs (best-ROI practice for reducing chop/flip churn)
        try:
            self.min_signal_interval_ms = int(self.config.get('signals', 'min_signal_interval_ms', fallback=30000))
        except Exception:
            self.min_signal_interval_ms = 30000
        try:
            self.confirm_window_sec = int(self.config.get('signals', 'confirm_window_sec', fallback=120))
        except Exception:
            self.confirm_window_sec = 120
        try:
            self.confirm_required = int(self.config.get('signals', 'confirm_required', fallback=2))
        except Exception:
            self.confirm_required = 2
        try:
            self.confirm_max_events = int(self.config.get('signals', 'confirm_max_events', fallback=6))
        except Exception:
            self.confirm_max_events = 6

        # Per-symbol recent triggers: list[(timestamp_ms, direction_value)]
        self.recent_triggers = {symbol: [] for symbol in symbols}
        
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
                # Fine-grained processing (orderbook/NOBI). Confirmation/cooldown handled in _handle_nobi_trigger.
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
        if symbol not in self.price_buffers:
            self.logger.error(f"DEBUG: Symbol {symbol} not in price_buffers! Skipping.")
            return

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
        DEPRECATED: old 10-minute aggregated processing.
        Kept only to avoid breaking imports/callers, but intentionally disabled.
        """
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

            # 0. Confirmation Gate (replaces the deprecated hard-coded 10m mode)
            # Require N triggers in the same direction within a rolling time window.
            try:
                now_ts = get_timestamp()
                hist = self.recent_triggers.get(symbol) or []
                hist.append((now_ts, direction.value))
                window_ms = int(max(1, int(getattr(self, 'confirm_window_sec', 120)))) * 1000
                hist = [(ts, d) for (ts, d) in hist if (now_ts - ts) <= window_ms]
                max_events = int(max(1, int(getattr(self, 'confirm_max_events', 6))))
                if len(hist) > max_events:
                    hist = hist[-max_events:]
                self.recent_triggers[symbol] = hist

                required = int(max(1, int(getattr(self, 'confirm_required', 2))))
                same_dir_count = sum(1 for (_, d) in hist if d == direction.value)
                if required > 1 and same_dir_count < required:
                    log_payload = {
                        "timestamp": get_timestamp(),
                        "symbol": symbol,
                        "price": order_book_data.get('mid_price', 0),
                        "nobi": nobi_value,
                        "rsi": 0,
                        "volume_ratio": "0.00",
                        "vwap_gap_pct": 0,
                        "ml_confidence": 0,
                        "decision": "REJECT",
                        "reason": f"Awaiting Confirmation ({same_dir_count}/{required})"
                    }
                    self.decision_logger.log_decision(log_payload)
                    self._log_decision_to_redis(log_payload)
                    return
            except Exception:
                # If confirmation gate fails for any reason, do not block trading.
                pass

            # 0. Regime Gate: optionally avoid signals during RANGING conditions.
            regime = self.market_regimes.get(symbol, MarketRegime.UNKNOWN)
            if self.avoid_ranging_regime and regime == MarketRegime.RANGING:
                log_payload = {
                    "timestamp": get_timestamp(),
                    "symbol": symbol,
                    "price": order_book_data.get('mid_price', 0),
                    "nobi": nobi_value,
                    "rsi": 0,
                    "volume_ratio": "0.00",
                    "vwap_gap_pct": 0,
                    "ml_confidence": 0,
                    "decision": "REJECT",
                    "reason": "Ranging Regime"
                }
                self.decision_logger.log_decision(log_payload)
                self._log_decision_to_redis(log_payload)
                return
            
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
                
                # Filter: Require trade size to be at least N x average (Significant interest)
                # If multiplier <= 1.0, treat as disabled.
                if self.volume_spike_multiplier > 1.0 and avg_vol > 0 and current_vol < (avg_vol * self.volume_spike_multiplier):
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
                    log_payload = {
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
                    }
                    self.decision_logger.log_decision(log_payload)
                    self._log_decision_to_redis(log_payload)
                    return
                if direction == SignalDirection.SHORT and current_price < vwap * 0.995: # 0.5% buffer
                    log_payload = {
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
                    }
                    self.decision_logger.log_decision(log_payload)
                    self._log_decision_to_redis(log_payload)
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
                log_payload = {
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
                }
                self.decision_logger.log_decision(log_payload)
                self._log_decision_to_redis(log_payload)
                self.signals_generated += 1
                
                self.logger.info(f"Generated {direction.value} signal for {symbol} - "
                               f"NOBI: {nobi_value:.4f}, Confidence: {confidence:.3f}")
        
        except Exception as e:
            self.logger.error(f"Error handling NOBI trigger for {symbol}: {e}")
    
    def _log_decision_to_redis(self, decision_data: Dict[str, Any]) -> None:
        """Log decision to Redis for dashboard visibility."""
        try:
            # Ensure timestamp is present
            if 'timestamp' not in decision_data:
                decision_data['timestamp'] = get_timestamp()
            
            # Ensure direction is present (dashboard requirement)
            if 'direction' not in decision_data:
                # Map 'decision' (e.g. REJECT) to 'direction'
                decision_data['direction'] = decision_data.get('decision', 'UNKNOWN')
            
            # Push to Redis list
            self.redis_manager.redis_client.lpush('decision_logs', json.dumps(decision_data))
            # Trim list to keep size manageable
            self.redis_manager.redis_client.ltrim('decision_logs', 0, 499)
        except Exception as e:
            self.logger.error(f"Error logging decision to Redis: {e}")

    def _is_too_soon_for_signal(self, symbol: str, min_interval_ms: Optional[int] = None) -> bool:
        """
        Check if it's too soon to generate another signal for this symbol.
        
        Args:
            symbol: Trading symbol
            min_interval_ms: Minimum interval between signals in milliseconds
            
        Returns:
            True if too soon
        """
        last_time = self.last_signal_time.get(symbol, 0)
        interval = int(min_interval_ms if min_interval_ms is not None else getattr(self, 'min_signal_interval_ms', 30000))
        return (get_timestamp() - last_time) < interval
    
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
            # NOTE: we only classify TRENDING vs RANGING.
            # TRENDING means a sufficiently large separation between EMAs, regardless of direction.
            ema_sep = abs(ema_20 - ema_50) / ema_50 if ema_50 != 0 else 0.0
            if ema_sep > self.market_regime_ema_diff_threshold:
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

    def add_symbol(self, symbol: str) -> None:
        """
        Dynamically add a symbol to the signal engine.
        
        Args:
            symbol: Trading symbol to add
        """
        if symbol in self.symbols:
            return
            
        self.logger.info(f"Adding symbol {symbol} to Signal Engine")
        self.symbols.append(symbol)
        
        # Initialize buffers
        self.price_buffers[symbol] = CircularBuffer(100)
        self.nobi_buffers[symbol] = CircularBuffer(50)
        self.volume_buffers[symbol] = CircularBuffer(100)
        self.market_regimes[symbol] = MarketRegime.UNKNOWN
        self.recent_triggers[symbol] = []
        self.last_signal_time[symbol] = 0
        
    def remove_symbol(self, symbol: str) -> None:
        """
        Dynamically remove a symbol from the signal engine.
        
        Args:
            symbol: Trading symbol to remove
        """
        if symbol not in self.symbols:
            return
            
        self.logger.info(f"Removing symbol {symbol} from Signal Engine")
        self.symbols.remove(symbol)
        
        # Clean up buffers
        self.price_buffers.pop(symbol, None)
        self.nobi_buffers.pop(symbol, None)
        self.volume_buffers.pop(symbol, None)
        self.market_regimes.pop(symbol, None)
        self.recent_triggers.pop(symbol, None)
        self.last_signal_time.pop(symbol, None)
        self.last_trade_ids.pop(symbol, None)
    
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
                # Deprecated 10m mode indicators (kept for UI compatibility)
                'ten_min_mode': False,
                'confirm_required': int(getattr(self, 'confirm_required', 2)),
                'confirm_window_sec': int(getattr(self, 'confirm_window_sec', 120)),
                'min_signal_interval_ms': int(getattr(self, 'min_signal_interval_ms', 30000)),
            }
        except Exception as e:
            self.logger.error(f"Error getting signal engine health status: {e}")
            return {
                'is_running': self.is_running,
                'symbols_monitored': len(self.symbols)
            }
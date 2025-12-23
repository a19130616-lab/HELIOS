"""
Execution Manager for Helios Trading System
Handles order execution, position management, and trade lifecycle.
"""

import logging
import time
import threading
import json
from typing import Dict, List, Optional, Any
from queue import Queue
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from uuid import uuid4
from binance.um_futures import UMFutures
from binance.error import ClientError

from app.models import TradingSignal, Position, OrderSide, SignalDirection
from app.risk_manager import RiskManager
from app.utils import RedisManager, get_timestamp, format_price
from app.config_manager import get_config

class ExecutionManager:
    """
    Manages trade execution and order lifecycle.
    Coordinates with Risk Manager for position sizing and stop loss management.
    """
    
    def __init__(self, api_client: UMFutures, risk_manager: RiskManager, redis_manager: RedisManager):
        """
        Initialize the Execution Manager.
        
        Args:
            api_client: Binance futures API client
            risk_manager: Risk manager instance
            redis_manager: Redis manager instance
        """
        self.api_client = api_client
        self.risk_manager = risk_manager
        self.redis_manager = redis_manager
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        # leverage configured as integer
        try:
            self.leverage = self.config.get('risk', 'leverage', int)
        except Exception:
            # fallback default
            self.leverage = 1
        
        # Optional: use exchange-native OCO where available.
        # Config value supports 'true'/'false' (string) or boolean.
        try:
            raw_val = self.config.get('risk', 'use_exchange_oco', fallback='true')
            self.use_exchange_oco = str(raw_val).lower() in ('1', 'true', 'yes', 'y')
        except Exception:
            self.use_exchange_oco = True
        
        # TTL for persisted stop metadata (seconds)
        try:
            self.persistent_stop_ttl = int(self.config.get('risk', 'persistent_stop_ttl_seconds', fallback='86400'))
        except Exception:
            self.persistent_stop_ttl = 24 * 3600  # 1 day

        # Exchange-side risk orders (optional).
        # NOTE: Some Binance endpoints reject STOP_MARKET on UMFutures with -4120.
        # Default: do NOT place exchange-side SL; keep exchange-side TP enabled.
        try:
            raw_val = self.config.get('risk', 'place_exchange_sl', fallback='false')
            self.place_exchange_sl = str(raw_val).lower() in ('1', 'true', 'yes', 'y')
        except Exception:
            self.place_exchange_sl = False

        try:
            raw_val = self.config.get('risk', 'place_exchange_tp', fallback='true')
            self.place_exchange_tp = str(raw_val).lower() in ('1', 'true', 'yes', 'y')
        except Exception:
            self.place_exchange_tp = True

        # Internal stop-loss close order behavior (limit-only, safety-focused)
        try:
            self.stop_loss_limit_slippage_pct = float(
                self.config.get('risk', 'stop_loss_limit_slippage_pct', float, fallback=0.01)
            )
        except Exception:
            self.stop_loss_limit_slippage_pct = 0.015

        try:
            self.stop_loss_replace_after_sec = int(
                self.config.get('risk', 'stop_loss_replace_after_sec', int, fallback=8)
            )
        except Exception:
            self.stop_loss_replace_after_sec = 8
            
        # Trading cooldown
        try:
            self.trading_cooldown_ms = self.config.get('trading', 'trading_cooldown_minutes', int, fallback=0) * 60 * 1000
        except Exception:
            self.trading_cooldown_ms = 0

        # Optional: flip (close-then-open) when an opposite signal arrives while a position is open.
        try:
            raw_val = self.config.get('trading', 'flip_on_opposite_signal', fallback='false')
            self.flip_on_opposite_signal = str(raw_val).lower() in ('1', 'true', 'yes', 'y')
        except Exception:
            self.flip_on_opposite_signal = False

        # Flip execution tuning (reduce fees by trying maker first)
        try:
            raw_val = self.config.get('trading', 'flip_exit_post_only', fallback='true')
            self.flip_exit_post_only = str(raw_val).lower() in ('1', 'true', 'yes', 'y')
        except Exception:
            self.flip_exit_post_only = True

        try:
            self.flip_exit_max_wait_sec = int(self.config.get('trading', 'flip_exit_max_wait_sec', int, fallback=4))
        except Exception:
            self.flip_exit_max_wait_sec = 4

        try:
            self.flip_exit_maker_offset_pct = float(
                self.config.get('trading', 'flip_exit_maker_offset_pct', float, fallback=0.0001)
            )
        except Exception:
            self.flip_exit_maker_offset_pct = 0.0001

        try:
            self.flip_min_interval_sec = int(self.config.get('trading', 'flip_min_interval_sec', int, fallback=20))
        except Exception:
            self.flip_min_interval_sec = 20
            
        # Execution state
        self.is_running = False
        self.signal_queue = Queue()
        self._stop_event = threading.Event()
        self._pubsub = None
        self.subscription_thread = None
        
        # Order tracking
        self.active_orders = {}  # order_id -> order_info
        self.monitored_exits = {} # order_id -> {symbol, side, type, timestamp}
        self.pending_signals = {}  # symbol -> signal
        # symbol -> pending flip signal (execute after FLIP_EXIT fills)
        self.pending_flips: Dict[str, TradingSignal] = {}
        self.last_flip_time: Dict[str, int] = {}
        self.last_trade_time = {}  # symbol -> timestamp_ms

        # Exchange metadata cache (precision/minNotional/stepSize)
        # symbol -> {step_size, min_qty, tick_size, min_notional}
        self._symbol_meta: Dict[str, Dict[str, float]] = {}
        # Cache user's position mode (Hedge Mode vs One-way). None = unknown.
        self._dual_side_position: Optional[bool] = None
        
        # Persisted OCO / stop metadata (in-memory index to the redis record)
        # key: symbol -> metadata { id, stop_order_id, tp_order_id, redis_key }
        self.persistent_stops = {}
        
        # Performance tracking
        self.orders_executed = 0
        self.execution_errors = 0
        
        # Subscribe to signals
        self._setup_signal_subscription()
    
    def start(self) -> None:
        """Start the execution manager."""
        self.logger.info("Starting Execution Manager...")
        self.is_running = True
        self._stop_event.clear()
        
        # Start signal processing thread
        self.signal_thread = threading.Thread(target=self._process_signals, daemon=True)
        self.signal_thread.start()
        
        # Start order monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_orders, daemon=True)
        self.monitor_thread.start()
        
        # Start stop loss monitoring thread
        self.stop_loss_thread = threading.Thread(target=self._monitor_stop_losses, daemon=True)
        self.stop_loss_thread.start()
        
        # Start exit monitoring thread
        self.exit_monitor_thread = threading.Thread(target=self._monitor_exits, daemon=True)
        self.exit_monitor_thread.start()
        
        self.logger.info("Execution Manager started")
    
    def stop(self) -> None:
        """Stop the execution manager."""
        self.logger.info("Stopping Execution Manager...")
        self.is_running = False
        self._stop_event.set()

        # Stop signal subscription
        try:
            if self._pubsub:
                try:
                    self._pubsub.close()
                except Exception:
                    pass
        except Exception:
            pass
        
        # Cancel all active orders
        self._cancel_all_orders()
        
        self.logger.info("Execution Manager stopped")
    
    def _setup_signal_subscription(self) -> None:
        """Set up subscription to trading signals from Redis."""
        def signal_handler():
            # Subscribe immediately so we don't miss early signals during startup.
            # We keep the thread alive until stop() is called.
            self._pubsub = self.redis_manager.subscribe(["trading_signals"])
            for message in self._pubsub.listen():
                if self._stop_event.is_set():
                    break
                
                if message['type'] == 'message':
                    try:
                        signal_data = json.loads(message['data'])
                        signal = TradingSignal(
                            symbol=signal_data['symbol'],
                            direction=SignalDirection(signal_data['direction']),
                            timestamp=signal_data['timestamp'],
                            confidence=signal_data['confidence'],
                            nobi_value=signal_data['nobi_value'],
                            entry_price=signal_data['entry_price'],
                            signal_id=signal_data.get('signal_id')
                        )
                        self.signal_queue.put(signal)
                        self.logger.info(
                            f"Received signal: {signal.symbol} {signal.direction.value} "
                            f"conf={signal.confidence:.3f} entry={signal.entry_price}"
                        )
                    except Exception as e:
                        self.logger.error(f"Error processing signal: {e}")
        
        # Start signal subscription thread
        if not self.subscription_thread or not self.subscription_thread.is_alive():
            self.subscription_thread = threading.Thread(target=signal_handler, daemon=True)
            self.subscription_thread.start()
    
    def _process_signals(self) -> None:
        """Process incoming trading signals."""
        while self.is_running:
            try:
                # Get signal from queue (blocking with timeout)
                try:
                    signal = self.signal_queue.get(timeout=1)
                except:
                    continue
                
                # Check if we should process this signal
                if self._should_process_signal(signal):
                    self._execute_signal(signal)
                
            except Exception as e:
                self.logger.error(f"Error processing signals: {e}")
                time.sleep(1)
    
    def _should_process_signal(self, signal: TradingSignal) -> bool:
        """
        Check if signal should be processed.
        
        Args:
            signal: Trading signal
            
        Returns:
            True if signal should be processed
        """
        # Check if risk manager allows new positions
        if self.risk_manager.emergency_mode:
            self.logger.warning(f"Ignoring signal for {signal.symbol} - emergency mode active")
            self._record_signal_status(signal, execution_status="SKIPPED", execution_error="emergency_mode_active")
            return False
        
        # Check if we already have a position in this symbol
        if signal.symbol in self.risk_manager.positions:
            # If flips are enabled and the signal is opposite direction, submit a close-then-open.
            if self.flip_on_opposite_signal:
                try:
                    if signal.symbol in self.pending_flips:
                        self.logger.info(f"Ignoring signal for {signal.symbol} - flip already pending")
                        self._record_signal_status(signal, execution_status="SKIPPED", execution_error="flip_already_pending")
                        return False

                    position = self.risk_manager.positions[signal.symbol]
                    current_dir = SignalDirection.LONG if position.side == OrderSide.BUY else SignalDirection.SHORT
                    if signal.direction == current_dir:
                        self.logger.info(f"Ignoring signal for {signal.symbol} - same-direction position already open")
                        self._record_signal_status(signal, execution_status="SKIPPED", execution_error="position_already_open_same_direction")
                        return False

                    self.logger.info(f"Flip requested for {signal.symbol}: closing then opening {signal.direction.value}")
                    self._record_signal_status(signal, execution_status="FLIP_EXIT_SUBMITTING")

                    # Prevent extremely rapid flip-churn in chop.
                    now_ts = get_timestamp()
                    last_flip = int(self.last_flip_time.get(signal.symbol, 0) or 0)
                    if self.flip_min_interval_sec and (now_ts - last_flip) < int(self.flip_min_interval_sec) * 1000:
                        self.logger.info(f"Ignoring flip for {signal.symbol} - flip_min_interval active")
                        self._record_signal_status(signal, execution_status="SKIPPED", execution_error="flip_min_interval_active")
                        return False

                    flip_exit_order_id = self._submit_flip_close(signal.symbol)
                    if not flip_exit_order_id:
                        self._record_signal_status(signal, execution_status="FAILED", execution_error="flip_exit_submission_failed")
                        return False

                    self.pending_flips[signal.symbol] = signal
                    self.last_flip_time[signal.symbol] = now_ts
                    self._record_signal_status(
                        signal,
                        execution_status="FLIP_EXIT_PLACED",
                        flip_exit_order_id=str(flip_exit_order_id),
                    )
                    return False
                except Exception as e:
                    self.logger.error(f"Error initiating flip for {signal.symbol}: {e}")
                    self._record_signal_status(signal, execution_status="FAILED", execution_error=f"flip_init_error {e}")
                    return False

            self.logger.info(f"Ignoring signal for {signal.symbol} - position already open")
            self._record_signal_status(signal, execution_status="SKIPPED", execution_error="position_already_open")
            return False
            
        # Check trading cooldown
        if self.trading_cooldown_ms > 0:
            last_time = self.last_trade_time.get(signal.symbol, 0)
            if get_timestamp() - last_time < self.trading_cooldown_ms:
                self.logger.debug(f"Ignoring signal for {signal.symbol} - cooldown active")
                self._record_signal_status(signal, execution_status="SKIPPED", execution_error="cooldown_active")
                return False
        
        # Check if we have a pending order for this symbol
        if signal.symbol in self.pending_signals:
            self.logger.info(f"Ignoring signal for {signal.symbol} - order already pending")
            self._record_signal_status(signal, execution_status="SKIPPED", execution_error="order_already_pending")
            return False
        
        # Check signal age (ignore signals older than 30 seconds)
        signal_age = get_timestamp() - signal.timestamp
        if signal_age > 30000:  # 30 seconds
            self.logger.warning(f"Ignoring stale signal for {signal.symbol} - age: {signal_age}ms")
            self._record_signal_status(signal, execution_status="SKIPPED", execution_error=f"stale_signal age_ms={signal_age}")
            return False
        
        return True

    def _submit_flip_close(self, symbol: str) -> Optional[str]:
        """Submit an aggressive reduce-only IOC LIMIT order to close an existing position.

        Returns the new exit order id if placed.
        """
        try:
            if symbol not in self.risk_manager.positions:
                return None

            position = self.risk_manager.positions[symbol]

            # Determine order side (opposite of current position)
            side = "SELL" if position.side == OrderSide.BUY else "BUY"

            # Cancel any existing TP exits we are monitoring for this symbol.
            for tp_order_id, info in list(self.monitored_exits.items()):
                if info.get('symbol') == symbol and info.get('type') == 'TAKE_PROFIT':
                    try:
                        self.api_client.cancel_order(symbol=symbol, orderId=tp_order_id)
                    except Exception:
                        pass
                    try:
                        del self.monitored_exits[tp_order_id]
                    except Exception:
                        pass

            # Format quantity (never exceed position size)
            quantity = self._format_quantity(symbol, abs(position.size), rounding="down")
            if quantity > abs(position.size):
                quantity = self._format_quantity(symbol, abs(position.size) * 0.999, rounding="down")
            if quantity <= 0:
                return None

            # Prefer post-only maker close (GTX) to reduce fees; fallback to IOC is handled in exit monitor.
            ticker = self.api_client.ticker_price(symbol=symbol)
            current_price = float(ticker['price'])
            if self.flip_exit_post_only:
                maker_off = float(self.flip_exit_maker_offset_pct)
                if side == "SELL":
                    # place on the ask side to be maker
                    limit_price = current_price * (1 + maker_off)
                else:
                    # place on the bid side to be maker
                    limit_price = current_price * (1 - maker_off)
                tif = "GTX"
            else:
                slippage = float(self.stop_loss_limit_slippage_pct)
                if side == "SELL":
                    limit_price = current_price * (1 - slippage)
                else:
                    limit_price = current_price * (1 + slippage)
                tif = "IOC"

            params: Dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": tif,
                "quantity": quantity,
                "price": self._format_price(symbol, limit_price),
            }

            if self._is_hedge_mode():
                params["positionSide"] = "LONG" if position.side == OrderSide.BUY else "SHORT"
            else:
                params["reduceOnly"] = "true"

            order = self.api_client.new_order(**params)
            if not (order and order.get('orderId')):
                return None

            oid = str(order['orderId'])
            self.logger.info(
                f"Flip close placed for {symbol}: orderId={oid} side={side} qty={quantity} price={params.get('price')} tif={tif}"
            )
            self.monitored_exits[oid] = {
                'symbol': symbol,
                'side': OrderSide(side),
                'type': 'FLIP_EXIT',
                'timestamp': get_timestamp(),
                'tif': tif,
            }
            return oid
        except Exception as e:
            self.logger.error(f"Error submitting flip close for {symbol}: {e}")
            return None

    def _submit_flip_close_aggressive(self, symbol: str) -> Optional[str]:
        """Aggressive fallback close for flip: IOC LIMIT that is likely to fill (taker-like)."""
        try:
            if symbol not in self.risk_manager.positions:
                return None

            position = self.risk_manager.positions[symbol]
            side = "SELL" if position.side == OrderSide.BUY else "BUY"

            quantity = self._format_quantity(symbol, abs(position.size), rounding="down")
            if quantity > abs(position.size):
                quantity = self._format_quantity(symbol, abs(position.size) * 0.999, rounding="down")
            if quantity <= 0:
                return None

            ticker = self.api_client.ticker_price(symbol=symbol)
            current_price = float(ticker['price'])
            slippage = float(self.stop_loss_limit_slippage_pct)
            if side == "SELL":
                limit_price = current_price * (1 - slippage)
            else:
                limit_price = current_price * (1 + slippage)

            params: Dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": "IOC",
                "quantity": quantity,
                "price": self._format_price(symbol, limit_price),
            }
            if self._is_hedge_mode():
                params["positionSide"] = "LONG" if position.side == OrderSide.BUY else "SHORT"
            else:
                params["reduceOnly"] = "true"

            order = self.api_client.new_order(**params)
            if not (order and order.get('orderId')):
                return None

            oid = str(order['orderId'])
            self.logger.warning(
                f"Flip close escalated to IOC for {symbol}: orderId={oid} side={side} qty={quantity} price={params.get('price')}"
            )
            self.monitored_exits[oid] = {
                'symbol': symbol,
                'side': OrderSide(side),
                'type': 'FLIP_EXIT',
                'timestamp': get_timestamp(),
                'tif': 'IOC',
            }
            return oid
        except Exception as e:
            self.logger.error(f"Error submitting aggressive flip close for {symbol}: {e}")
            return None

    def _record_signal_status(self, signal: TradingSignal, **status_fields: Any) -> None:
        """Persist execution status for a signal so the dashboard can show live order outcomes."""
        try:
            sid = getattr(signal, 'signal_id', None) or status_fields.get('signal_id')
            if not sid:
                # Best-effort fallback to avoid dropping status entirely
                sid = uuid4().hex
            key = f"signal_status:{sid}"

            payload = {
                'signal_id': sid,
                'symbol': getattr(signal, 'symbol', None),
                'direction': getattr(getattr(signal, 'direction', None), 'value', None),
                'signal_ts': getattr(signal, 'timestamp', None),
                'updated_ts': get_timestamp(),
            }
            payload.update(status_fields)

            # Keep statuses long enough for dashboard inspection
            self.redis_manager.set_data(key, payload, expiry=86400)
        except Exception:
            # Never let status publishing break execution
            pass
    
    def _execute_signal(self, signal: TradingSignal) -> None:
        """
        Execute a trading signal.
        
        Args:
            signal: Trading signal to execute
        """
        try:
            symbol = signal.symbol
            self.logger.info(f"Executing signal: {symbol} {signal.direction.value} @ {signal.entry_price}")

            self._record_signal_status(signal, execution_status="EXECUTING")
            
            # Set leverage for the symbol
            self._set_leverage(symbol)
            
            # Get position size from risk manager
            # Calculate preliminary stop loss for position sizing
            entry_price = signal.entry_price
            preliminary_stop = self._calculate_preliminary_stop_loss(signal)
            
            position_size = self.risk_manager.get_position_size(symbol, entry_price, preliminary_stop)
            
            if position_size <= 0:
                self.logger.warning(f"Risk manager returned zero position size for {symbol}")
                self._record_signal_status(signal, execution_status="FAILED", execution_error="position_size_zero")
                return
            
            # Format position size according to symbol precision
            formatted_size = self._format_quantity(symbol, position_size)
            
            if formatted_size <= 0:
                self.logger.warning(f"Formatted position size is zero for {symbol}")
                self._record_signal_status(signal, execution_status="FAILED", execution_error="formatted_quantity_zero")
                return
            
            # Determine order side
            side = "BUY" if signal.direction == SignalDirection.LONG else "SELL"
            order_side = OrderSide.BUY if side == "BUY" else OrderSide.SELL
            
            # Place limit order with small offset to act as maker
            order_price = self._calculate_order_price(signal)
            formatted_price = self._format_price(symbol, order_price)

            # Publish what we are about to attempt
            try:
                notional = float(formatted_size) * float(formatted_price)
            except Exception:
                notional = None
            self._record_signal_status(
                signal,
                execution_status="ORDER_SUBMITTING",
                leverage_used=self.leverage,
                requested_qty=formatted_size,
                requested_price=formatted_price,
                position_value_usd=notional,
                capital_used_usd=(notional / float(self.leverage or 1)) if notional is not None else None,
            )

            # Ensure notional meets exchange minimums (Binance can reject with -4164).
            min_notional = self._get_min_notional(symbol)
            try:
                px = float(formatted_price)
            except Exception:
                px = float(order_price)

            if min_notional and px > 0:
                current_notional = float(formatted_size) * px
                if current_notional + 1e-9 < float(min_notional):
                    target_qty = float(min_notional) / px
                    bumped_qty = self._format_quantity(symbol, target_qty, rounding="up")
                    bumped_notional = bumped_qty * px
                    if bumped_notional + 1e-9 >= float(min_notional):
                        self.logger.warning(
                            f"Bumping {symbol} qty to meet minNotional: {current_notional:.4f} -> {bumped_notional:.4f} (min={min_notional})"
                        )
                        formatted_size = bumped_qty
                    else:
                        self.logger.warning(
                            f"Cannot meet minNotional for {symbol} after rounding (min={min_notional}). Skipping order."
                        )
                        return
            
            # Place the order
            position_side = self._resolve_position_side(signal)
            order = self._place_limit_order(symbol, side, formatted_size, formatted_price, position_side=position_side)
            
            if order:
                # Track the order
                self.active_orders[order['orderId']] = {
                    'order': order,
                    'signal': signal,
                    'timestamp': get_timestamp(),
                    'position_size': formatted_size,
                    'order_side': order_side
                }
                
                self.pending_signals[symbol] = signal
                self.orders_executed += 1
                
                self.logger.info(f"Placed order: {order['orderId']} for {symbol}")

                self._record_signal_status(
                    signal,
                    execution_status="ORDER_PLACED",
                    exchange_order_id=order.get('orderId'),
                    exchange_status=order.get('status'),
                )
            else:
                self._record_signal_status(signal, execution_status="FAILED", execution_error="order_submission_failed")
            
        except Exception as e:
            self.logger.error(f"Error executing signal for {signal.symbol}: {e}")
            self._record_signal_status(signal, execution_status="FAILED", execution_error=str(e))
            self.execution_errors += 1
    
    def _set_leverage(self, symbol: str) -> None:
        """
        Set leverage for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        try:
            self.api_client.change_leverage(symbol=symbol, leverage=self.leverage)
            self.logger.debug(f"Set leverage to {self.leverage}x for {symbol}")
        except ClientError as e:
            if "No need to change leverage" not in str(e):
                self.logger.warning(f"Could not set leverage for {symbol}: {e}")
        except Exception as e:
            self.logger.error(f"Error setting leverage for {symbol}: {e}")
    
    def _calculate_preliminary_stop_loss(self, signal: TradingSignal) -> float:
        """
        Calculate preliminary stop loss for position sizing.
        
        Args:
            signal: Trading signal
            
        Returns:
            Preliminary stop loss price
        """
        # Use 1.5% as preliminary stop loss for position sizing
        stop_distance_pct = 0.015
        
        if signal.direction == SignalDirection.LONG:
            return signal.entry_price * (1 - stop_distance_pct)
        else:
            return signal.entry_price * (1 + stop_distance_pct)
    
    def _calculate_order_price(self, signal: TradingSignal) -> float:
        """
        Calculate order price with small offset to act as maker.
        
        Args:
            signal: Trading signal
            
        Returns:
            Order price
        """
        # Use small offset (0.02%) to increase fill probability while remaining maker
        offset_pct = 0.0002
        
        if signal.direction == SignalDirection.LONG:
            # Buy slightly below current price
            return signal.entry_price * (1 - offset_pct)
        else:
            # Sell slightly above current price
            return signal.entry_price * (1 + offset_pct)
    
    def _place_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        position_side: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Place a limit order.
        
        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            price: Order price
            
        Returns:
            Order response or None if failed
        """
        try:
            params: Dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": "GTC",  # Good Till Cancelled
                "quantity": quantity,
                "price": price,
            }

            # If user account is in Hedge Mode, Binance requires positionSide.
            if self._is_hedge_mode() and position_side:
                params["positionSide"] = position_side

            order = self.api_client.new_order(**params)
            
            self.logger.info(f"Placed limit order: {symbol} {side} {quantity} @ {price}")
            return order
            
        except ClientError as e:
            self.logger.error(f"Binance API error placing order for {symbol}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error placing order for {symbol}: {e}")
            return None
    
    def _monitor_orders(self) -> None:
        """Monitor active orders and handle fills."""
        while self.is_running:
            try:
                # Check each active order
                for order_id in list(self.active_orders.keys()):
                    self._check_order_status(order_id)
                
                time.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                self.logger.error(f"Error monitoring orders: {e}")
                time.sleep(5)
    
    def _check_order_status(self, order_id: str) -> None:
        """
        Check status of an active order.
        
        Args:
            order_id: Order ID to check
        """
        try:
            if order_id not in self.active_orders:
                return
            
            order_info = self.active_orders[order_id]
            symbol = order_info['signal'].symbol
            
            # Query order status
            order = self.api_client.query_order(symbol=symbol, orderId=order_id)
            
            status = order['status']
            
            if status == 'FILLED':
                self._handle_order_fill(order_id, order)
            elif status == 'CANCELED' or status == 'REJECTED' or status == 'EXPIRED':
                self._handle_order_cancellation(order_id, status)
            elif status == 'PARTIALLY_FILLED':
                # Continue monitoring
                pass
            else:
                # Check if order is too old (cancel after 60 seconds)
                order_age = get_timestamp() - order_info['timestamp']
                if order_age > 60000:  # 60 seconds
                    self._cancel_order(order_id)
                    
        except Exception as e:
            self.logger.error(f"Error checking order status {order_id}: {e}")
    
    def _handle_order_fill(self, order_id: str, order: Dict[str, Any]) -> None:
        """
        Handle order fill - create position and set up risk management.
        
        Args:
            order_id: Order ID
            order: Order details
        """
        try:
            if order_id not in self.active_orders:
                return
            
            order_info = self.active_orders[order_id]
            signal = order_info['signal']
            symbol = signal.symbol
            
            # Extract fill details
            filled_qty = float(order['executedQty'])
            avg_price = float(order['avgPrice'])
            
            self.logger.info(f"Order filled: {symbol} {filled_qty} @ {avg_price}")

            # Fetch commission info
            commission = 0.0
            commission_asset = "USDT"
            try:
                # Fetch trades associated with this order to get commission
                trades = self.api_client.get_account_trades(symbol=symbol, orderId=order_id)
                # Some Binance responses can return avgPrice=0 even for filled orders.
                # Reconstruct VWAP from fills if possible.
                try:
                    if (avg_price is None) or (float(avg_price) <= 0.0):
                        total_qty = 0.0
                        total_quote = 0.0
                        for t in trades or []:
                            q = float(t.get('qty') or t.get('quantity') or 0.0)
                            p = float(t.get('price') or 0.0)
                            if q > 0 and p > 0:
                                total_qty += q
                                total_quote += q * p
                        if total_qty > 0:
                            avg_price = total_quote / total_qty
                except Exception:
                    pass

                for trade in trades:
                    commission += float(trade.get('commission', 0.0))
                    commission_asset = trade.get('commissionAsset', commission_asset)
                self.logger.info(f"Order {order_id} commission: {commission} {commission_asset}")
            except Exception as e:
                self.logger.error(f"Error fetching commission for order {order_id}: {e}")

            # Final fallback for avg_price if still invalid
            try:
                if (avg_price is None) or (float(avg_price) <= 0.0):
                    cq = order.get('cummulativeQuoteQty') or order.get('cumQuote')
                    if cq is not None and filled_qty > 0:
                        avg_price = float(cq) / float(filled_qty)
            except Exception:
                pass

            if avg_price is None or float(avg_price) <= 0.0:
                self.logger.error(f"Filled order has invalid avg_price for {symbol} (orderId={order_id}). Skipping position/TP setup.")
                # Clean up active order tracking to avoid deadlocks
                try:
                    del self.active_orders[order_id]
                except Exception:
                    pass
                if symbol in self.pending_signals:
                    try:
                        del self.pending_signals[symbol]
                    except Exception:
                        pass
                self.execution_errors += 1
                return
            
            # Add position to risk manager
            self.risk_manager.add_position(
                symbol=symbol,
                side=order_info['order_side'],
                size=filled_qty,
                entry_price=avg_price
            )

            # --- NEW: Place Exchange-Side Stop Loss & Take Profit ---
            # Get the position object we just created to access calculated SL
            position = self.risk_manager.positions.get(symbol)
            if position:
                # Determine exit side
                exit_side = "SELL" if order_info['order_side'] == OrderSide.BUY else "BUY"
                
                # 1. Exchange-side Stop Loss (STOP_MARKET) is optional and disabled by default
                # because some Binance endpoints reject it with -4120.
                if self.place_exchange_sl and position.stop_loss:
                    try:
                        sl_params = {
                            "symbol": symbol,
                            "side": exit_side,
                            "type": "STOP_MARKET",
                            "stopPrice": self._format_price(symbol, position.stop_loss),
                            "quantity": self._format_quantity(symbol, filled_qty),
                        }

                        if self._is_hedge_mode():
                            sl_params["positionSide"] = "LONG" if order_info['order_side'] == OrderSide.BUY else "SHORT"
                        else:
                            sl_params["reduceOnly"] = True

                        self.logger.info(
                            f"Placing Exchange SL (Stop Market) for {symbol} at {sl_params['stopPrice']}"
                        )
                        self.api_client.new_order(**sl_params)
                    except Exception as e:
                        self.logger.error(f"Failed to place exchange-side SL for {symbol}: {e}")

                # 2. Place Take Profit (LIMIT - Maker)
                if self.place_exchange_tp:
                    try:
                        tp_price = self.risk_manager.calculate_take_profit(symbol, avg_price, order_info['order_side'])

                        if tp_price:
                            # ReduceOnly exits must not exceed actual position size.
                            tp_qty = self._format_quantity(symbol, filled_qty, rounding="down")
                            if tp_qty > float(filled_qty):
                                tp_qty = self._format_quantity(symbol, float(filled_qty) * 0.999, rounding="down")
                            if tp_qty <= 0:
                                raise ValueError(f"TP quantity formatted to 0 for {symbol} (filled_qty={filled_qty})")

                            tp_params = {
                                "symbol": symbol,
                                "side": exit_side,
                                "type": "LIMIT",
                                "price": self._format_price(symbol, tp_price),
                                "quantity": tp_qty,
                                "timeInForce": "GTC",
                            }
                            if self._is_hedge_mode():
                                tp_params["positionSide"] = "LONG" if order_info['order_side'] == OrderSide.BUY else "SHORT"
                            else:
                                tp_params["reduceOnly"] = True

                            self.logger.info(f"Placing Exchange TP (Maker) for {symbol} at {tp_params['price']}")
                            try:
                                tp_order = self.api_client.new_order(**tp_params)
                            except ClientError as ce:
                                # Common failure: account is in Hedge Mode but our detection returned False,
                                # causing reduceOnly to be rejected (-2022). Retry with positionSide.
                                if ("-2022" in str(ce)) and ("reduceonly" in str(ce).lower()):
                                    retry = dict(tp_params)
                                    retry.pop("reduceOnly", None)
                                    retry["positionSide"] = "LONG" if order_info['order_side'] == OrderSide.BUY else "SHORT"
                                    self.logger.warning(
                                        f"TP reduceOnly rejected for {symbol}; retrying with positionSide={retry['positionSide']}"
                                    )
                                    tp_order = self.api_client.new_order(**retry)
                                else:
                                    raise

                            # Track TP order for execution logging
                            if tp_order and 'orderId' in tp_order:
                                self.monitored_exits[tp_order['orderId']] = {
                                    'symbol': symbol,
                                    'side': OrderSide(tp_params['side']),
                                    'type': 'TAKE_PROFIT',
                                    'timestamp': get_timestamp()
                                }
                    except Exception as e:
                        self.logger.error(f"Failed to place exchange-side TP for {symbol}: {e}")
            # -------------------------------------------------------
            
            # Clean up
            del self.active_orders[order_id]
            if symbol in self.pending_signals:
                del self.pending_signals[symbol]
            
            # Log execution
            self._log_execution(symbol, order_info['order_side'], filled_qty, avg_price, "FILLED", commission, commission_asset)
            
            # Update last trade time for cooldown
            self.last_trade_time[symbol] = get_timestamp()
            
        except Exception as e:
            self.logger.error(f"Error handling order fill {order_id}: {e}")
    
    def _handle_order_cancellation(self, order_id: str, status: str) -> None:
        """
        Handle order cancellation.
        
        Args:
            order_id: Order ID
            status: Cancellation status
        """
        try:
            if order_id not in self.active_orders:
                return
            
            order_info = self.active_orders[order_id]
            symbol = order_info['signal'].symbol
            
            self.logger.info(f"Order {status}: {order_id} for {symbol}")
            
            # Clean up
            del self.active_orders[order_id]
            if symbol in self.pending_signals:
                del self.pending_signals[symbol]
                
        except Exception as e:
            self.logger.error(f"Error handling order cancellation {order_id}: {e}")
    
    def _cancel_order(self, order_id: str) -> None:
        """
        Cancel an active order.
        
        Args:
            order_id: Order ID to cancel
        """
        try:
            if order_id not in self.active_orders:
                return
            
            order_info = self.active_orders[order_id]
            symbol = order_info['signal'].symbol
            
            self.api_client.cancel_order(symbol=symbol, orderId=order_id)
            self.logger.info(f"Cancelled order: {order_id}")
            
        except Exception as e:
            self.logger.warning(f"Error cancelling order {order_id}: {e}")
    
    def _cancel_all_orders(self) -> None:
        """Cancel all active orders tracked by the bot."""
        for order_id in list(self.active_orders.keys()):
            self._cancel_order(order_id)

    def cancel_exchange_orders(self, symbols: List[str]) -> None:
        """
        Cancel all open orders on the exchange for the given symbols.
        Useful for cleanup on startup.
        
        Args:
            symbols: List of symbols to clean up
        """
        self.logger.info("Cleaning up open orders on exchange...")
        for symbol in symbols:
            try:
                self.api_client.cancel_open_orders(symbol=symbol)
                self.logger.info(f"Cancelled all open orders for {symbol}")
            except Exception as e:
                # Ignore error if no orders to cancel
                self.logger.debug(f"Could not cancel orders for {symbol}: {e}")
    
    def _monitor_stop_losses(self) -> None:
        """Monitor stop losses and close positions when triggered."""
        while self.is_running:
            try:
                for symbol in list(self.risk_manager.positions.keys()):
                    self._check_stop_loss(symbol)
                
                time.sleep(1)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error monitoring stop losses: {e}")
                time.sleep(5)

    def _monitor_exits(self) -> None:
        """Monitor exit orders (TP) and log execution when filled."""
        while self.is_running:
            try:
                # Create copy of keys to avoid modification during iteration
                order_ids = list(self.monitored_exits.keys())
                
                for order_id in order_ids:
                    self._check_exit_status(order_id)
                
                time.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                self.logger.error(f"Error in exit monitor loop: {e}")
                time.sleep(5)

    def _check_exit_status(self, order_id: str) -> None:
        """
        Check status of an exit order.
        
        Args:
            order_id: Order ID
        """
        try:
            # NOTE: monitored_exits can be modified by other threads (e.g., stop-loss replacement).
            # Use get() to avoid a KeyError race where the exception message becomes the order_id.
            exit_info = self.monitored_exits.get(order_id)
            if not exit_info:
                return
            symbol = exit_info['symbol']
            
            # Query order status
            order = self.api_client.query_order(symbol=symbol, orderId=order_id)
            status = order['status']
            
            if status == 'FILLED':
                exit_type = exit_info.get('type') or 'FILLED'
                # Log execution
                filled_qty = float(order['executedQty'])
                avg_price = float(order['avgPrice'])
                
                # Fetch commission
                commission = 0.0
                commission_asset = "USDT"
                try:
                    trades = self.api_client.get_account_trades(symbol=symbol, orderId=order_id)
                    for trade in trades:
                        commission += float(trade.get('commission', 0.0))
                        commission_asset = trade.get('commissionAsset', commission_asset)
                except Exception:
                    pass
                
                self._log_execution(
                    symbol=symbol,
                    side=exit_info['side'],
                    quantity=filled_qty,
                    price=avg_price,
                    type=str(exit_type),
                    commission=commission,
                    commission_asset=commission_asset
                )
                
                self.logger.info(f"Exit order {order_id} filled for {symbol}")
                
                # Remove from monitored exits (safe if another thread already cleaned it)
                self.monitored_exits.pop(order_id, None)
                
                # Update RiskManager
                self.risk_manager.remove_position(symbol, avg_price, str(exit_type))

                # If this fill was a flip close, immediately execute the pending opposite signal.
                if str(exit_type) == 'FLIP_EXIT':
                    flip_signal = self.pending_flips.pop(symbol, None)
                    if flip_signal is not None:
                        self.logger.info(f"Flip exit filled for {symbol}; executing pending opposite signal")
                        self._record_signal_status(flip_signal, execution_status="FLIP_EXIT_FILLED")
                        # Bypass _should_process_signal cooldown gating; we already closed.
                        self._execute_signal(flip_signal)

            elif str(exit_info.get('type') or '') == 'FLIP_EXIT' and status in ['NEW', 'PARTIALLY_FILLED']:
                # If we tried a post-only flip close, give it a moment to fill as maker, then escalate.
                try:
                    tif = str(exit_info.get('tif') or '')
                    if tif == 'GTX' and self.flip_exit_max_wait_sec:
                        age_ms = get_timestamp() - int(exit_info.get('timestamp') or get_timestamp())
                        if age_ms >= int(self.flip_exit_max_wait_sec) * 1000:
                            self.logger.warning(f"Flip close for {symbol} not filled in time; escalating to IOC")
                            try:
                                self.api_client.cancel_order(symbol=symbol, orderId=order_id)
                            except Exception:
                                pass
                            try:
                                if order_id in self.monitored_exits:
                                    del self.monitored_exits[order_id]
                            except Exception:
                                pass
                            new_oid = self._submit_flip_close_aggressive(symbol)
                            if not new_oid:
                                flip_signal = self.pending_flips.pop(symbol, None)
                                if flip_signal is not None:
                                    self._record_signal_status(
                                        flip_signal,
                                        execution_status="FAILED",
                                        execution_error="flip_exit_escalation_failed",
                                    )
                except Exception:
                    pass

            elif status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                exit_type = exit_info.get('type') or ''
                self.monitored_exits.pop(order_id, None)

                if str(exit_type) == 'FLIP_EXIT':
                    flip_signal = self.pending_flips.pop(symbol, None)
                    if flip_signal is not None:
                        self._record_signal_status(
                            flip_signal,
                            execution_status="FAILED",
                            execution_error=f"flip_exit_{status.lower()}"
                        )
                
        except Exception as e:
            self.logger.error(f"Error checking exit status {order_id}: {e}")
            # If order not found (e.g. 404), remove it
            if "Order does not exist" in str(e) or "Order was not found" in str(e):
                if order_id in self.monitored_exits:
                    # Clean up any pending flip signal if this was a flip exit.
                    try:
                        info = self.monitored_exits.get(order_id) or {}
                        if str(info.get('type')) == 'FLIP_EXIT':
                            flip_signal = self.pending_flips.pop(info.get('symbol'), None)
                            if flip_signal is not None:
                                self._record_signal_status(
                                    flip_signal,
                                    execution_status="FAILED",
                                    execution_error="flip_exit_order_not_found"
                                )
                    except Exception:
                        pass
                    self.monitored_exits.pop(order_id, None)
    
    def _check_stop_loss(self, symbol: str) -> None:
        """
        Check if stop loss is triggered for a position.
        
        Args:
            symbol: Trading symbol
        """
        try:
            # Get current price
            ticker = self.api_client.ticker_price(symbol=symbol)
            current_price = float(ticker['price'])
            
            # Update position price in risk manager
            self.risk_manager.update_position_price(symbol, current_price)
            
            # Check if stop loss is triggered
            if self.risk_manager.check_stop_loss_trigger(symbol, current_price):
                self._execute_stop_loss(symbol)
                
        except Exception as e:
            self.logger.error(f"Error checking stop loss for {symbol}: {e}")
    
    def _execute_stop_loss(self, symbol: str) -> None:
        """
        Execute stop loss for a position.
        
        Args:
            symbol: Trading symbol
        """
        try:
            if symbol not in self.risk_manager.positions:
                return

            # If a stop-loss exit order is already pending for this symbol, don't place duplicates.
            # If it's been pending too long, cancel and replace (avoids getting stuck on a GTC limit).
            now_ts = get_timestamp()
            for existing_order_id, info in list(self.monitored_exits.items()):
                if info.get('symbol') == symbol and info.get('type') == 'STOP_LOSS':
                    age_ms = now_ts - int(info.get('timestamp') or now_ts)
                    if age_ms < int(self.stop_loss_replace_after_sec) * 1000:
                        return
                    try:
                        self.api_client.cancel_order(symbol=symbol, orderId=existing_order_id)
                    except Exception:
                        pass
                    try:
                        del self.monitored_exits[existing_order_id]
                    except Exception:
                        pass
            
            position = self.risk_manager.positions[symbol]
            
            # Determine order side (opposite of position)
            side = "SELL" if position.side == OrderSide.BUY else "BUY"

            # If we have a TP order open on the exchange for this symbol, cancel it before placing SL.
            # This prevents a later TP fill from causing unexpected behavior once SL closes the position.
            for tp_order_id, info in list(self.monitored_exits.items()):
                if info.get('symbol') == symbol and info.get('type') == 'TAKE_PROFIT':
                    try:
                        self.api_client.cancel_order(symbol=symbol, orderId=tp_order_id)
                    except Exception:
                        pass
                    try:
                        del self.monitored_exits[tp_order_id]
                    except Exception:
                        pass
            
            # Format quantity
            # IMPORTANT: For Stop Loss (ReduceOnly), we must NOT round up to minQty if it exceeds our actual position.
            # _format_quantity by default might round up to meet minQty.
            # Here we explicitly format to step size but ensure we don't exceed position.size.
            
            # 1. Format to step size (rounding down)
            quantity = self._format_quantity(symbol, abs(position.size), rounding="down")
            
            # 2. Double check we didn't round up (though rounding="down" shouldn't)
            # and that we don't send 0.
            if quantity > abs(position.size):
                quantity = self._format_quantity(symbol, abs(position.size) * 0.999, rounding="down")
                
            if quantity <= 0:
                self.logger.warning(f"Calculated SL quantity for {symbol} is 0. Position size: {position.size}")
                return

            # --- CHANGED: Use LIMIT order instead of MARKET to save fees (Maker) ---
            # We want to close immediately but as a Maker if possible.
            # However, for a Stop Loss, we usually want guaranteed exit (Taker).
            # But the user specifically asked: "For closing the orders, can you make sure to use limit orders instead as well?"
            # So we will try to place a Limit order at a price that is likely to fill but might be Maker if we are lucky,
            # or at least controlled slippage.
            # Actually, if it's a STOP LOSS trigger from our internal loop, price is already past our stop.
            # To be a Maker, we'd need to place it *away* from market, which means we don't close immediately.
            # That contradicts the concept of a Stop Loss.
            # BUT, if this is a "Take Profit" or "Signal Close", Limit is good.
            # If this is a "Hard Stop", Market is safer.
            # Let's compromise: Use Limit order with a price that crosses the spread (Taker) to ensure fill,
            # OR if the user really wants Maker, we place it at the touch.
            # Given "Stop Loss", safety > fees. But let's use LIMIT at a marketable price (IOC/GTC) to control slippage?
            # No, the user said "use limit orders instead".
            # Let's assume they mean for "normal" closes. For Emergency Stop Loss, Market is still best practice.
            # However, I will change this to a LIMIT order with a price slightly worse than market to ensure fill (effectively Taker but Limit type).
            # Wait, if I use LIMIT at current price, it might not fill if price moves away.
            
            # Let's stick to MARKET for _execute_stop_loss because it's a safety mechanism.
            # I will add a comment explaining why.
            # BUT, I will modify the "Take Profit" logic (which I added in _handle_order_fill) to be Maker.
            # And if there is a generic "Close Position" method, I should check that.
            
            # Re-reading user request: "For closing the orders, can you make sure to use limit orders instead as well?"
            # This might apply to when the bot decides to close based on signal reversal or TP.
            # Since this method is specifically `_execute_stop_loss`, I should probably keep it as Market or aggressive Limit.
            # Let's use an aggressive Limit order (Limit Taker) to satisfy "use limit orders" but keep safety.
            
            # Get current price (ticker)
            ticker = self.api_client.ticker_price(symbol=symbol)
            current_price = float(ticker['price'])
            
            # For SELL (closing Long), we want to sell lower than current to ensure fill.
            # For BUY (closing Short), we want to buy higher than current.
            slippage = float(self.stop_loss_limit_slippage_pct)  # default 1% slippage allowance
            if side == "SELL":
                limit_price = current_price * (1 - slippage)
            else:
                limit_price = current_price * (1 + slippage)
                
            params: Dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                # IOC prevents stale stop-loss limits from sitting on the book while the position stays open.
                "timeInForce": "IOC",
                "quantity": quantity,
                "price": self._format_price(symbol, limit_price)
            }
            if self._is_hedge_mode():
                # Close the correct side in Hedge Mode.
                params["positionSide"] = "LONG" if position.side == OrderSide.BUY else "SHORT"
            else:
                params["reduceOnly"] = "true"

            order = self.api_client.new_order(**params)
            self.logger.warning(f"Stop loss (Limit) submitted for {symbol}: {order}")

            # Track stop-loss order for fill monitoring and performance logging.
            if order and 'orderId' in order:
                self.monitored_exits[order['orderId']] = {
                    'symbol': symbol,
                    'side': OrderSide(side),
                    'type': 'STOP_LOSS',
                    'timestamp': get_timestamp()
                }
            
        except Exception as e:
            self.logger.error(f"Error executing stop loss for {symbol}: {e}")
    
    def _format_quantity(self, symbol: str, quantity: float, rounding: str = "down") -> float:
        """
        Format quantity according to symbol precision.
        
        Args:
            symbol: Trading symbol
            quantity: Raw quantity
            
        Returns:
            Formatted quantity
        """
        try:
            meta = self._get_symbol_meta(symbol)
            step_size = float(meta.get("step_size") or 0.0)
            min_qty = float(meta.get("min_qty") or 0.0)

            if step_size <= 0:
                return round(float(quantity), 6)

            qty_d = Decimal(str(quantity))
            step_d = Decimal(str(step_size))

            rounding_mode = ROUND_DOWN if str(rounding).lower() != "up" else ROUND_UP
            steps = (qty_d / step_d).to_integral_value(rounding=rounding_mode)
            quantized = steps * step_d

            # Enforce minQty (round up to the next step if needed)
            # NOTE: This logic is dangerous for ReduceOnly orders if the position size is exactly minQty
            # but slightly less due to float precision.
            if min_qty > 0:
                min_d = Decimal(str(min_qty))
                if quantized < min_d:
                    # Only round up if we are NOT explicitly rounding down (which implies we want to stay under a cap)
                    # However, the 'rounding' param controls the step rounding, not this minQty check.
                    # We'll assume if the user asked for 'down', they might be sensitive to caps, 
                    # but usually minQty is a hard exchange requirement.
                    # For Stop Loss, we handle this in _execute_stop_loss by checking against position size.
                    steps = (min_d / step_d).to_integral_value(rounding=ROUND_UP)
                    quantized = steps * step_d

            # Normalize to avoid float artifacts like 1.23000000004
            return float(quantized.normalize())
        except Exception as e:
            self.logger.error(f"Error formatting quantity for {symbol}: {e}")
            return round(float(quantity), 6)
    
    def _format_price(self, symbol: str, price: float) -> str:
        """
        Format price according to symbol precision.
        
        Args:
            symbol: Trading symbol
            price: Raw price
            
        Returns:
            Formatted price string
        """
        try:
            meta = self._get_symbol_meta(symbol)
            tick_size = float(meta.get("tick_size") or 0.0)
            if tick_size <= 0:
                return format_price(price)

            price_d = Decimal(str(price))
            tick_d = Decimal(str(tick_size))
            ticks = (price_d / tick_d).to_integral_value(rounding=ROUND_DOWN)
            quantized = ticks * tick_d

            precision = max(0, len(str(tick_size).rstrip('0').split('.')[-1]))
            return f"{float(quantized):.{precision}f}"
        except Exception as e:
            self.logger.error(f"Error formatting price for {symbol}: {e}")
            return format_price(price)

    def _get_symbol_meta(self, symbol: str) -> Dict[str, float]:
        """Fetch and cache exchange metadata (step/tick/minNotional) for a symbol."""
        symbol = str(symbol).upper()
        cached = self._symbol_meta.get(symbol)
        if cached:
            return cached

        meta: Dict[str, float] = {
            "step_size": 0.0,
            "min_qty": 0.0,
            "tick_size": 0.0,
            "min_notional": 0.0,
        }
        try:
            info = self.api_client.exchange_info()
            for s in info.get("symbols", []):
                if s.get("symbol") != symbol:
                    continue
                for f in s.get("filters", []):
                    if not isinstance(f, dict):
                        continue
                    ft = f.get("filterType")
                    if ft == "LOT_SIZE":
                        meta["step_size"] = float(f.get("stepSize", 0) or 0)
                        meta["min_qty"] = float(f.get("minQty", 0) or 0)
                    elif ft == "PRICE_FILTER":
                        meta["tick_size"] = float(f.get("tickSize", 0) or 0)
                    elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                        # Different connector versions expose different keys
                        meta["min_notional"] = float(
                            f.get("notional")
                            or f.get("minNotional")
                            or f.get("minNotionalValue")
                            or 0
                        )
                break
        except Exception as e:
            self.logger.debug(f"Failed to load exchangeInfo for {symbol}: {e}")

        self._symbol_meta[symbol] = meta
        return meta

    def _get_min_notional(self, symbol: str) -> float:
        try:
            return float(self._get_symbol_meta(symbol).get("min_notional") or 0.0)
        except Exception:
            return 0.0

    def _is_hedge_mode(self) -> bool:
        """Return True if user's futures account is in Hedge Mode (dualSidePosition)."""
        if self._dual_side_position is not None:
            return bool(self._dual_side_position)

        # Best-effort detection; fall back to one-way if API doesn't support it.
        try:
            getter = getattr(self.api_client, "get_position_mode", None)
            if callable(getter):
                resp = getter()
                if isinstance(resp, dict) and "dualSidePosition" in resp:
                    self._dual_side_position = bool(resp.get("dualSidePosition"))
                    return bool(self._dual_side_position)
        except Exception as e:
            self.logger.debug(f"Could not detect position mode: {e}")

        self._dual_side_position = False
        return False

    def _resolve_position_side(self, signal: TradingSignal) -> Optional[str]:
        """Map a TradingSignal direction to Binance futures positionSide for Hedge Mode."""
        if signal.direction == SignalDirection.LONG:
            return "LONG"
        if signal.direction == SignalDirection.SHORT:
            return "SHORT"
        return None
    
    def _log_execution(self, symbol: str, side: OrderSide, quantity: float, price: float, type: str, commission: float = 0.0, commission_asset: str = "USDT") -> None:
        """
        Log trade execution.
        
        Args:
            symbol: Trading symbol
            side: Order side
            quantity: Execution quantity
            price: Execution price
            type: Execution type
            commission: Commission paid
            commission_asset: Asset commission was paid in
        """
        execution_data = {
            'symbol': symbol,
            'side': side.value,
            'quantity': quantity,
            'price': price,
            'type': type,
            'commission': commission,
            'commission_asset': commission_asset,
            'timestamp': get_timestamp()
        }
        
        # Store in Redis
        self.redis_manager.set_data(f"execution:{symbol}:{get_timestamp()}", execution_data, expiry=3600)
        
        # Publish to subscribers
        self.redis_manager.publish("trade_executions", execution_data)
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the Execution Manager.
        
        Returns:
            Health status information
        """
        return {
            'is_running': self.is_running,
            'active_orders': len(self.active_orders),
            'pending_signals': len(self.pending_signals),
            'orders_executed': self.orders_executed,
            'execution_errors': self.execution_errors,
            'current_leverage': self.leverage
        }
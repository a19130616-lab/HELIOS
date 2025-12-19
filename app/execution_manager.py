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
            
        # Trading cooldown
        try:
            self.trading_cooldown_ms = self.config.get('trading', 'trading_cooldown_minutes', int, fallback=0) * 60 * 1000
        except Exception:
            self.trading_cooldown_ms = 0
            
        # Execution state
        self.is_running = False
        self.signal_queue = Queue()
        self._stop_event = threading.Event()
        self._pubsub = None
        self.subscription_thread = None
        
        # Order tracking
        self.active_orders = {}  # order_id -> order_info
        self.pending_signals = {}  # symbol -> signal
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
                for trade in trades:
                    commission += float(trade.get('commission', 0.0))
                    commission_asset = trade.get('commissionAsset', commission_asset)
                self.logger.info(f"Order {order_id} commission: {commission} {commission_asset}")
            except Exception as e:
                self.logger.error(f"Error fetching commission for order {order_id}: {e}")
            
            # Add position to risk manager
            self.risk_manager.add_position(
                symbol=symbol,
                side=order_info['order_side'],
                size=filled_qty,
                entry_price=avg_price
            )
            
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
        """Cancel all active orders."""
        for order_id in list(self.active_orders.keys()):
            self._cancel_order(order_id)
    
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
            
            position = self.risk_manager.positions[symbol]
            
            # Determine order side (opposite of position)
            side = "SELL" if position.side == OrderSide.BUY else "BUY"
            
            # Format quantity
            quantity = self._format_quantity(symbol, abs(position.size))
            
            # Place market order to close position
            params: Dict[str, Any] = {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": quantity,
            }
            if self._is_hedge_mode():
                # Close the correct side in Hedge Mode.
                params["positionSide"] = "LONG" if position.side == OrderSide.BUY else "SHORT"

            order = self.api_client.new_order(**params)
            
            self.logger.warning(f"Stop loss executed for {symbol}: {order}")

            # Fetch commission
            commission = 0.0
            commission_asset = "USDT"
            try:
                trades = self.api_client.get_account_trades(symbol=symbol, orderId=order['orderId'])
                for trade in trades:
                    commission += float(trade.get('commission', 0.0))
                    commission_asset = trade.get('commissionAsset', commission_asset)
            except Exception as e:
                self.logger.error(f"Error fetching commission for SL {symbol}: {e}")
            
            # Remove position from risk manager
            self.risk_manager.remove_position(symbol, position.current_price, "STOP_LOSS")
            
            # Log execution
            self._log_execution(symbol, position.side, quantity, position.current_price, "STOP_LOSS", commission, commission_asset)
            
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
            if min_qty > 0:
                min_d = Decimal(str(min_qty))
                if quantized < min_d:
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
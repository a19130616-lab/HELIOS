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
        
        # Order tracking
        self.active_orders = {}  # order_id -> order_info
        self.pending_signals = {}  # symbol -> signal
        self.last_trade_time = {}  # symbol -> timestamp_ms
        
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
        
        # Cancel all active orders
        self._cancel_all_orders()
        
        self.logger.info("Execution Manager stopped")
    
    def _setup_signal_subscription(self) -> None:
        """Set up subscription to trading signals from Redis."""
        def signal_handler():
            pubsub = self.redis_manager.subscribe(["trading_signals"])
            for message in pubsub.listen():
                if not self.is_running:
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
                            entry_price=signal_data['entry_price']
                        )
                        self.signal_queue.put(signal)
                    except Exception as e:
                        self.logger.error(f"Error processing signal: {e}")
        
        # Start signal subscription thread
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
            return False
        
        # Check if we already have a position in this symbol
        if signal.symbol in self.risk_manager.positions:
            self.logger.info(f"Ignoring signal for {signal.symbol} - position already open")
            return False
            
        # Check trading cooldown
        if self.trading_cooldown_ms > 0:
            last_time = self.last_trade_time.get(signal.symbol, 0)
            if get_timestamp() - last_time < self.trading_cooldown_ms:
                self.logger.debug(f"Ignoring signal for {signal.symbol} - cooldown active")
                return False
        
        # Check if we have a pending order for this symbol
        if signal.symbol in self.pending_signals:
            self.logger.info(f"Ignoring signal for {signal.symbol} - order already pending")
            return False
        
        # Check signal age (ignore signals older than 30 seconds)
        signal_age = get_timestamp() - signal.timestamp
        if signal_age > 30000:  # 30 seconds
            self.logger.warning(f"Ignoring stale signal for {signal.symbol} - age: {signal_age}ms")
            return False
        
        return True
    
    def _execute_signal(self, signal: TradingSignal) -> None:
        """
        Execute a trading signal.
        
        Args:
            signal: Trading signal to execute
        """
        try:
            symbol = signal.symbol
            self.logger.info(f"Executing signal: {symbol} {signal.direction.value} @ {signal.entry_price}")
            
            # Set leverage for the symbol
            self._set_leverage(symbol)
            
            # Get position size from risk manager
            # Calculate preliminary stop loss for position sizing
            entry_price = signal.entry_price
            preliminary_stop = self._calculate_preliminary_stop_loss(signal)
            
            position_size = self.risk_manager.get_position_size(symbol, entry_price, preliminary_stop)
            
            if position_size <= 0:
                self.logger.warning(f"Risk manager returned zero position size for {symbol}")
                return
            
            # Format position size according to symbol precision
            formatted_size = self._format_quantity(symbol, position_size)
            
            if formatted_size <= 0:
                self.logger.warning(f"Formatted position size is zero for {symbol}")
                return
            
            # Determine order side
            side = "BUY" if signal.direction == SignalDirection.LONG else "SELL"
            order_side = OrderSide.BUY if side == "BUY" else OrderSide.SELL
            
            # Place limit order with small offset to act as maker
            order_price = self._calculate_order_price(signal)
            formatted_price = self._format_price(symbol, order_price)
            
            # Place the order
            order = self._place_limit_order(symbol, side, formatted_size, formatted_price)
            
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
            
        except Exception as e:
            self.logger.error(f"Error executing signal for {signal.symbol}: {e}")
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
    
    def _place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Optional[Dict[str, Any]]:
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
            order = self.api_client.new_order(
                symbol=symbol,
                side=side,
                type="LIMIT",
                timeInForce="GTC",  # Good Till Cancelled
                quantity=quantity,
                price=price
            )
            
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
            order = self.api_client.new_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=quantity
            )
            
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
    
    def _format_quantity(self, symbol: str, quantity: float) -> float:
        """
        Format quantity according to symbol precision.
        
        Args:
            symbol: Trading symbol
            quantity: Raw quantity
            
        Returns:
            Formatted quantity
        """
        try:
            # Get symbol info
            exchange_info = self.api_client.exchange_info()
            
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == symbol:
                    for filter_info in symbol_info['filters']:
                        if filter_info['filterType'] == 'LOT_SIZE':
                            step_size = float(filter_info['stepSize'])
                            min_qty = float(filter_info['minQty'])
                            
                            # Round to step size
                            rounded_qty = round(quantity / step_size) * step_size
                            
                            # Ensure minimum quantity
                            return max(rounded_qty, min_qty)
            
            # Fallback - round to 6 decimal places
            return round(quantity, 6)
            
        except Exception as e:
            self.logger.error(f"Error formatting quantity for {symbol}: {e}")
            return round(quantity, 6)
    
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
            # Get symbol info
            exchange_info = self.api_client.exchange_info()
            
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == symbol:
                    for filter_info in symbol_info['filters']:
                        if filter_info['filterType'] == 'PRICE_FILTER':
                            tick_size = float(filter_info['tickSize'])
                            
                            # Round to tick size
                            rounded_price = round(price / tick_size) * tick_size
                            
                            # Format with appropriate precision
                            precision = len(str(tick_size).rstrip('0').split('.')[-1])
                            return f"{rounded_price:.{precision}f}"
            
            # Fallback
            return format_price(price)
            
        except Exception as e:
            self.logger.error(f"Error formatting price for {symbol}: {e}")
            return format_price(price)
    
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
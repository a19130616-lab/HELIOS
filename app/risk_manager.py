"""
Risk Manager for Helios Trading System
Implements Kelly Criterion position sizing and multi-level risk management.
"""

import logging
import time
import threading
from typing import Dict, List, Optional, Any, Tuple
from binance.um_futures import UMFutures

from app.models import Position, AccountInfo, RiskAssessment, RiskLevel, OrderSide, SystemMetrics
from app.utils import kelly_criterion, calculate_atr, get_timestamp
from app.config_manager import get_config

class RiskManager:
    """
    Comprehensive risk management system implementing:
    1. Kelly Criterion position sizing
    2. Dynamic trailing stops
    3. Account-level liquidation avoidance
    """
    
    def __init__(self, api_client: UMFutures):
        """
        Initialize the Risk Manager.
        
        Args:
            api_client: Binance futures API client
        """
        self.api_client = api_client
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Configuration parameters
        self.kelly_fraction = self.config.get('risk', 'kelly_fraction', float)
        self.leverage = self.config.get('risk', 'leverage', int)
        self.trailing_stop_atr_multiple = self.config.get('risk', 'trailing_stop_atr_multiple', float)
        self.warning_margin_ratio = self.config.get('risk', 'warning_margin_ratio', float)
        self.critical_margin_ratio = self.config.get('risk', 'critical_margin_ratio', float)
        
        # Risk monitoring state
        self.is_running = False
        self.positions = {}  # symbol -> Position
        self.account_info = None
        self.last_margin_check = 0
        
        # Performance tracking for Kelly calculation
        self.trade_history = []
        self.win_rate = 0.6  # Default optimistic assumption
        self.avg_win = 0.01  # Default 1% win
        self.avg_loss = 0.005  # Default 0.5% loss
        
        # Emergency state
        self.emergency_mode = False
        self.last_emergency_action = 0
        
    def start(self) -> None:
        """Start the risk management system."""
        self.logger.info("Starting Risk Manager...")
        self.is_running = True
        
        # Start liquidation avoidance monitoring
        self.monitor_thread = threading.Thread(target=self._run_liquidation_monitor, daemon=True)
        self.monitor_thread.start()
        
        # Start trailing stop management
        self.trailing_stop_thread = threading.Thread(target=self._manage_trailing_stops, daemon=True)
        self.trailing_stop_thread.start()
        
        self.logger.info("Risk Manager started")
    
    def stop(self) -> None:
        """Stop the risk management system."""
        self.logger.info("Stopping Risk Manager...")
        self.is_running = False
        self.logger.info("Risk Manager stopped")
    
    def get_position_size(self, symbol: str, entry_price: float, stop_loss: float) -> float:
        """
        Calculate optimal position size using Fractional Kelly Criterion.
        
        Args:
            symbol: Trading symbol
            entry_price: Planned entry price
            stop_loss: Stop loss price
            
        Returns:
            Position size in base currency units
        """
        try:
            # Get account balance
            account_info = self._get_account_info()
            if not account_info:
                self.logger.warning("Could not get account info for position sizing")
                return 0.0
            
            available_balance = account_info.available_balance
            
            # Calculate Kelly percentage
            kelly_pct = kelly_criterion(self.win_rate, self.avg_win, self.avg_loss)
            
            # Apply fraction and safety limits
            fractional_kelly = kelly_pct * self.kelly_fraction
            
            # Limit to maximum 10% of balance per trade
            max_risk_pct = min(fractional_kelly, 0.10)
            
            # Calculate risk amount
            risk_amount = available_balance * max_risk_pct
            
            # Calculate position size based on stop loss distance
            if entry_price == 0 or stop_loss == 0 or entry_price == stop_loss:
                return 0.0
            
            stop_distance = abs(entry_price - stop_loss)
            stop_distance_pct = stop_distance / entry_price
            
            # Position size calculation
            position_value = risk_amount / stop_distance_pct
            position_size = position_value / entry_price
            
            # Apply leverage
            leveraged_size = position_size * self.leverage
            
            # Safety check - ensure we don't exceed account limits
            max_position_value = available_balance * self.leverage * 0.8  # 80% of max
            if leveraged_size * entry_price > max_position_value:
                leveraged_size = max_position_value / entry_price
            
            self.logger.info(f"Position sizing for {symbol}: Kelly={kelly_pct:.3f}, "
                           f"Fractional={fractional_kelly:.3f}, Size={leveraged_size:.6f}")
            
            return leveraged_size
            
        except Exception as e:
            self.logger.error(f"Error calculating position size: {e}")
            return 0.0
    
    def add_position(self, symbol: str, side: OrderSide, size: float, entry_price: float) -> None:
        """
        Add a new position for tracking.
        
        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            size: Position size
            entry_price: Entry price
        """
        try:
            # Calculate initial stop loss based on ATR
            stop_loss = self._calculate_initial_stop_loss(symbol, entry_price, side)
            
            position = Position(
                symbol=symbol,
                side=side,
                size=size,
                entry_price=entry_price,
                current_price=entry_price,
                timestamp=get_timestamp(),
                stop_loss=stop_loss,
                high_water_mark=entry_price
            )
            
            self.positions[symbol] = position
            self.logger.info(f"Added position: {symbol} {side.value} {size} @ {entry_price}")
            
        except Exception as e:
            self.logger.error(f"Error adding position {symbol}: {e}")
    
    def update_position_price(self, symbol: str, current_price: float) -> None:
        """
        Update position with current price and manage trailing stop.
        
        Args:
            symbol: Trading symbol
            current_price: Current market price
        """
        if symbol not in self.positions:
            return
        
        position = self.positions[symbol]
        position.current_price = current_price
        
        # Update high water mark and trailing stop
        self._update_trailing_stop(position)
    
    def remove_position(self, symbol: str, exit_price: float, reason: str = "Manual") -> None:
        """
        Remove a position and record trade outcome.
        
        Args:
            symbol: Trading symbol
            exit_price: Exit price
            reason: Reason for exit
        """
        if symbol not in self.positions:
            return
        
        position = self.positions[symbol]
        
        # Calculate trade outcome
        if position.side == OrderSide.BUY:
            pnl = (exit_price - position.entry_price) * position.size
            pnl_pct = (exit_price - position.entry_price) / position.entry_price
        else:
            pnl = (position.entry_price - exit_price) * position.size
            pnl_pct = (position.entry_price - exit_price) / position.entry_price
        
        # Record trade for performance tracking
        self._record_trade(position, pnl, pnl_pct)
        
        # Remove position
        del self.positions[symbol]
        
        self.logger.info(f"Removed position {symbol}: PnL={pnl:.4f} ({pnl_pct*100:.2f}%) - {reason}")
    
    def check_stop_loss_trigger(self, symbol: str, current_price: float) -> bool:
        """
        Check if current price has triggered stop loss.
        
        Args:
            symbol: Trading symbol
            current_price: Current market price
            
        Returns:
            True if stop loss is triggered
        """
        if symbol not in self.positions:
            return False
        
        position = self.positions[symbol]
        
        if position.stop_loss is None:
            return False
        
        # Check stop loss trigger
        if position.side == OrderSide.BUY:
            return current_price <= position.stop_loss
        else:
            return current_price >= position.stop_loss
    
    def _calculate_initial_stop_loss(self, symbol: str, entry_price: float, side: OrderSide) -> float:
        """
        Calculate initial stop loss based on ATR.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            side: Order side
            
        Returns:
            Stop loss price
        """
        try:
            # Get recent kline data for ATR calculation
            klines = self.api_client.klines(symbol=symbol, interval="1m", limit=20)
            
            if len(klines) < 14:
                # Fallback to 1% stop loss
                multiplier = 0.01
            else:
                # Calculate ATR
                highs = [float(kline[2]) for kline in klines]
                lows = [float(kline[3]) for kline in klines]
                closes = [float(kline[4]) for kline in klines]
                
                atr = calculate_atr(highs, lows, closes)
                multiplier = atr / entry_price * self.trailing_stop_atr_multiple
                
                # Ensure reasonable bounds (0.5% to 3%)
                multiplier = max(0.005, min(0.03, multiplier))
            
            if side == OrderSide.BUY:
                return entry_price * (1 - multiplier)
            else:
                return entry_price * (1 + multiplier)
                
        except Exception as e:
            self.logger.error(f"Error calculating stop loss for {symbol}: {e}")
            # Fallback stop loss
            return entry_price * (0.99 if side == OrderSide.BUY else 1.01)
    
    def _update_trailing_stop(self, position: Position) -> None:
        """
        Update trailing stop loss for a position.
        
        Args:
            position: Position to update
        """
        try:
            current_price = position.current_price
            
            # Update high water mark
            if position.side == OrderSide.BUY:
                if current_price > position.high_water_mark:
                    position.high_water_mark = current_price
            else:
                if current_price < position.high_water_mark:
                    position.high_water_mark = current_price
            
            # Calculate new trailing stop
            if position.high_water_mark is None:
                return
            
            # Get ATR-based distance
            try:
                klines = self.api_client.klines(symbol=position.symbol, interval="1m", limit=20)
                if len(klines) >= 14:
                    highs = [float(kline[2]) for kline in klines]
                    lows = [float(kline[3]) for kline in klines]
                    closes = [float(kline[4]) for kline in klines]
                    
                    atr = calculate_atr(highs, lows, closes)
                    atr_distance = atr * self.trailing_stop_atr_multiple
                else:
                    atr_distance = position.high_water_mark * 0.01  # 1% fallback
            except:
                atr_distance = position.high_water_mark * 0.01
            
            # Calculate new stop loss
            if position.side == OrderSide.BUY:
                new_stop = position.high_water_mark - atr_distance
                # Only update if new stop is higher (trailing up)
                if position.stop_loss is None or new_stop > position.stop_loss:
                    position.stop_loss = new_stop
            else:
                new_stop = position.high_water_mark + atr_distance
                # Only update if new stop is lower (trailing down)
                if position.stop_loss is None or new_stop < position.stop_loss:
                    position.stop_loss = new_stop
                    
        except Exception as e:
            self.logger.error(f"Error updating trailing stop for {position.symbol}: {e}")
    
    def _run_liquidation_monitor(self) -> None:
        """Run the liquidation avoidance monitoring loop."""
        while self.is_running:
            try:
                self._check_account_margin()
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                self.logger.error(f"Error in liquidation monitor: {e}")
                time.sleep(10)
    
    def _check_account_margin(self) -> None:
        """Check account margin ratio and take action if necessary."""
        try:
            account_info = self._get_account_info()
            if not account_info:
                return
            
            self.account_info = account_info
            margin_ratio = account_info.margin_ratio
            
            # Check warning level
            if account_info.is_warning_level and not self.emergency_mode:
                self.logger.warning(f"Margin ratio at warning level: {margin_ratio:.2f}%")
                self._handle_warning_level()
            
            # Check critical level
            if account_info.is_critical_level:
                self.logger.critical(f"Margin ratio at critical level: {margin_ratio:.2f}%")
                self._handle_critical_level()
            
            # Reset emergency mode if margin is healthy
            if margin_ratio > self.warning_margin_ratio and self.emergency_mode:
                self.emergency_mode = False
                self.logger.info("Margin ratio recovered, exiting emergency mode")
            
        except Exception as e:
            self.logger.error(f"Error checking account margin: {e}")
    
    def _handle_warning_level(self) -> None:
        """Handle warning level margin ratio."""
        # Stop taking new positions (this would be coordinated with execution manager)
        self.emergency_mode = True
        
        # Log warning
        self.logger.warning("Entering emergency mode - no new positions allowed")
    
    def _handle_critical_level(self) -> None:
        """Handle critical level margin ratio - emergency de-risking."""
        current_time = get_timestamp()
        
        # Prevent rapid repeated actions
        if current_time - self.last_emergency_action < 10000:  # 10 seconds
            return
        
        self.last_emergency_action = current_time
        
        try:
            # Step 1: Reduce leverage on all positions
            self._reduce_leverage()
            
            # Step 2: Close losing positions
            self._close_losing_positions()
            
            # Step 3: If still critical, close profitable positions
            account_info = self._get_account_info()
            if account_info and account_info.is_critical_level:
                self._close_profitable_positions()
                
        except Exception as e:
            self.logger.error(f"Error in emergency de-risking: {e}")
    
    def _reduce_leverage(self) -> None:
        """Reduce leverage on all positions."""
        try:
            for symbol in self.positions.keys():
                self.api_client.change_leverage(symbol=symbol, leverage=1)
                self.logger.info(f"Reduced leverage to 1x for {symbol}")
                time.sleep(0.1)  # Rate limiting
        except Exception as e:
            self.logger.error(f"Error reducing leverage: {e}")
    
    def _close_losing_positions(self) -> None:
        """Close positions with unrealized losses."""
        try:
            losing_positions = [
                (symbol, pos) for symbol, pos in self.positions.items()
                if pos.unrealized_pnl < 0
            ]
            
            # Sort by largest loss first
            losing_positions.sort(key=lambda x: x[1].unrealized_pnl)
            
            for symbol, position in losing_positions:
                self._emergency_close_position(symbol, position)
                
        except Exception as e:
            self.logger.error(f"Error closing losing positions: {e}")
    
    def _close_profitable_positions(self) -> None:
        """Close profitable positions to free up margin."""
        try:
            profitable_positions = [
                (symbol, pos) for symbol, pos in self.positions.items()
                if pos.unrealized_pnl > 0
            ]
            
            # Sort by smallest profit first (preserve large winners)
            profitable_positions.sort(key=lambda x: x[1].unrealized_pnl)
            
            for symbol, position in profitable_positions:
                self._emergency_close_position(symbol, position)
                
        except Exception as e:
            self.logger.error(f"Error closing profitable positions: {e}")
    
    def _emergency_close_position(self, symbol: str, position: Position) -> None:
        """
        Emergency close a position with market order.
        
        Args:
            symbol: Trading symbol
            position: Position to close
        """
        try:
            # Determine order side (opposite of position)
            side = "SELL" if position.side == OrderSide.BUY else "BUY"
            
            # Place market order to close
            order = self.api_client.new_order(
                symbol=symbol,
                side=side,
                type="MARKET",
                quantity=abs(position.size)
            )
            
            self.logger.warning(f"Emergency closed position {symbol}: {order}")
            
            # Remove from tracking
            if symbol in self.positions:
                del self.positions[symbol]
                
        except Exception as e:
            self.logger.error(f"Error emergency closing position {symbol}: {e}")
    
    def _manage_trailing_stops(self) -> None:
        """Manage trailing stops for all positions."""
        while self.is_running:
            try:
                for symbol, position in self.positions.items():
                    # Get current price and update trailing stop
                    ticker = self.api_client.ticker_price(symbol=symbol)
                    current_price = float(ticker['price'])
                    
                    self.update_position_price(symbol, current_price)
                
                time.sleep(1)  # Update every second
                
            except Exception as e:
                self.logger.error(f"Error managing trailing stops: {e}")
                time.sleep(5)
    
    def _get_account_info(self) -> Optional[AccountInfo]:
        """Get current account information."""
        try:
            account = self.api_client.account()
            
            total_balance = float(account['totalWalletBalance'])
            available_balance = float(account['availableBalance'])
            used_margin = float(account['totalInitialMargin'])
            
            # Calculate margin ratio
            if used_margin > 0:
                margin_ratio = (available_balance / used_margin) * 100
            else:
                margin_ratio = 100.0
            
            # Get positions
            positions = []
            for pos_data in account.get('positions', []):
                if float(pos_data['positionAmt']) != 0:
                    position = Position(
                        symbol=pos_data['symbol'],
                        side=OrderSide.BUY if float(pos_data['positionAmt']) > 0 else OrderSide.SELL,
                        size=abs(float(pos_data['positionAmt'])),
                        entry_price=float(pos_data['entryPrice']),
                        current_price=float(pos_data['markPrice']),
                        timestamp=get_timestamp()
                    )
                    positions.append(position)
            
            return AccountInfo(
                total_balance=total_balance,
                available_balance=available_balance,
                used_margin=used_margin,
                margin_ratio=margin_ratio,
                positions=positions,
                timestamp=get_timestamp()
            )
            
        except Exception as e:
            self.logger.error(f"Error getting account info: {e}")
            return None
    
    def _record_trade(self, position: Position, pnl: float, pnl_pct: float) -> None:
        """
        Record trade outcome for performance tracking.
        
        Args:
            position: Closed position
            pnl: Profit/loss amount
            pnl_pct: Profit/loss percentage
        """
        trade_record = {
            'symbol': position.symbol,
            'side': position.side.value,
            'entry_price': position.entry_price,
            'exit_price': position.current_price,
            'size': position.size,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'duration': get_timestamp() - position.timestamp,
            'timestamp': get_timestamp()
        }
        
        self.trade_history.append(trade_record)
        
        # Keep only last 100 trades
        if len(self.trade_history) > 100:
            self.trade_history = self.trade_history[-100:]
        
        # Update performance metrics
        self._update_performance_metrics()
    
    def _update_performance_metrics(self) -> None:
        """Update win rate and win/loss ratio from trade history."""
        if len(self.trade_history) < 10:
            return
        
        recent_trades = self.trade_history[-50:]  # Last 50 trades
        
        wins = [t for t in recent_trades if t['pnl'] > 0]
        losses = [t for t in recent_trades if t['pnl'] < 0]
        
        if len(recent_trades) > 0:
            self.win_rate = len(wins) / len(recent_trades)
        
        if len(wins) > 0:
            self.avg_win = sum(abs(t['pnl_pct']) for t in wins) / len(wins)
        
        if len(losses) > 0:
            self.avg_loss = sum(abs(t['pnl_pct']) for t in losses) / len(losses)
        
        self.logger.info(f"Updated performance: Win Rate={self.win_rate:.3f}, "
                        f"Avg Win={self.avg_win:.4f}, Avg Loss={self.avg_loss:.4f}")
    
    def get_risk_assessment(self) -> RiskAssessment:
        """
        Get current risk assessment.
        
        Returns:
            Risk assessment
        """
        if not self.account_info:
            return RiskAssessment(
                timestamp=get_timestamp(),
                level=RiskLevel.HIGH,
                margin_ratio=0.0,
                position_count=0,
                total_exposure=0.0,
                max_single_position_risk=0.0,
                message="No account information available"
            )
        
        # Determine risk level
        if self.account_info.is_critical_level:
            level = RiskLevel.CRITICAL
            message = "Critical margin level - emergency de-risking active"
        elif self.account_info.is_warning_level:
            level = RiskLevel.HIGH
            message = "Warning margin level - new positions disabled"
        elif len(self.positions) > 5:
            level = RiskLevel.MEDIUM
            message = "Multiple positions open"
        else:
            level = RiskLevel.LOW
            message = "Risk levels normal"
        
        # Calculate metrics
        position_count = len(self.positions)
        total_exposure = sum(pos.size * pos.current_price for pos in self.positions.values())
        max_position_risk = max((pos.size * pos.current_price for pos in self.positions.values()), default=0.0)
        
        return RiskAssessment(
            timestamp=get_timestamp(),
            level=level,
            margin_ratio=self.account_info.margin_ratio,
            position_count=position_count,
            total_exposure=total_exposure,
            max_single_position_risk=max_position_risk,
            message=message
        )
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the Risk Manager.
        
        Returns:
            Health status information
        """
        return {
            'is_running': self.is_running,
            'emergency_mode': self.emergency_mode,
            'positions_tracked': len(self.positions),
            'win_rate': self.win_rate,
            'avg_win': self.avg_win,
            'avg_loss': self.avg_loss,
            'trades_recorded': len(self.trade_history),
            'kelly_fraction': self.kelly_fraction,
            'current_leverage': self.leverage,
            'margin_ratio': self.account_info.margin_ratio if self.account_info else None
        }
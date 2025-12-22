"""
Performance Tracker for Helios Trading System
Tracks and reports trading performance, profitability metrics, and system health.
"""

import logging
import time
import threading
import json
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

from app.models import Position, OrderSide
from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config

@dataclass
class TradeRecord:
    """Individual trade record for performance tracking."""
    symbol: str
    side: str
    entry_price: float
    exit_price: float
    quantity: float
    entry_time: int
    exit_time: int
    pnl: float
    pnl_percentage: float
    duration_seconds: int
    exit_reason: str
    commission: float = 0.0
    net_pnl: float = 0.0
    sentiment_score: Optional[float] = None
    trend_score: Optional[float] = None

@dataclass
class PerformanceMetrics:
    """Performance metrics summary."""
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    total_fees: float
    net_pnl: float
    total_pnl_percentage: float
    average_win: float
    average_loss: float
    largest_win: float
    largest_loss: float
    profit_factor: float
    sharpe_ratio: float
    max_drawdown: float
    current_drawdown: float
    average_trade_duration: float
    trades_per_day: float
    start_time: int
    end_time: int

class PerformanceTracker:
    """
    Tracks trading performance and generates reports.
    Monitors profitability, win rates, and system health metrics.
    """
    
    def __init__(self, redis_manager: RedisManager):
        """
        Initialize the Performance Tracker.
        
        Args:
            redis_manager: Redis manager instance
        """
        self.redis_manager = redis_manager
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.report_interval = self.config.get('performance', 'report_interval_minutes', int, 30)
        self.max_trades_memory = self.config.get('performance', 'max_trades_memory', int, 1000)
        
        # State
        self.is_running = False
        self.trade_records: List[TradeRecord] = []
        self.daily_pnl = {}  # date -> pnl
        self.portfolio_value_history = []  # (timestamp, value)
        self.peak_portfolio_value = 0.0
        self.start_time = get_timestamp()
        self.open_positions = {} # symbol -> position_data

        # NOTE: Trade execution subscription must be started after `start()` sets
        # `is_running=True`. Otherwise the subscription thread can exit immediately
        # and never process executions.
        self.subscription_thread = None
    
    def start(self) -> None:
        """Start the performance tracker (fresh session metrics only)."""
        self.logger.info("Starting Performance Tracker...")
        self.is_running = True

        # Subscribe to trade executions (must happen after is_running=True)
        self._setup_trade_subscription()

        # IMPORTANT: Explicitly reset all in‑memory state so a hot-restart (without full process exit)
        # does NOT retain prior run's trade history / portfolio stats.
        self.trade_records = []
        self.daily_pnl = {}
        self.portfolio_value_history = []
        self.peak_portfolio_value = 0.0
        self.start_time = get_timestamp()

        # Fresh session: intentionally DO NOT load historical trade_records so that
        # performance metrics reflect only the current runtime.
        # (Historical trade_record:* data remains in Redis for inspection if needed.)
        # self._load_historical_data()

        # Remove any persisted historical aggregate metrics so dashboard does not show stale history.
        try:
            self.redis_manager.redis_client.delete("performance_metrics")
        except Exception as e:
            self.logger.warning(f"Could not delete old performance_metrics key: {e}")

        # Publish a baseline zeroed metrics snapshot immediately so UI shows a clean slate.
        baseline = PerformanceMetrics(
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            win_rate=0.0,
            total_pnl=0.0,
            total_fees=0.0,
            net_pnl=0.0,
            total_pnl_percentage=0.0,
            average_win=0.0,
            average_loss=0.0,
            largest_win=0.0,
            largest_loss=0.0,
            profit_factor=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0,
            current_drawdown=0.0,
            average_trade_duration=0.0,
            trades_per_day=0.0,
            start_time=self.start_time,
            end_time=self.start_time
        )
        baseline_dict = asdict(baseline)
        # UI compatibility: dashboard expects max_drawdown_pct; provide both.
        baseline_dict['max_drawdown_pct'] = baseline.max_drawdown
        # Add explicit session start marker
        baseline_dict['session_start_ts'] = self.start_time
        try:
            self.redis_manager.set_data("performance_metrics", baseline_dict, expiry=3600)
            self.redis_manager.publish("performance_updates", baseline_dict)
        except Exception as e:
            self.logger.warning(f"Failed to publish baseline performance metrics: {e}")

        # Start reporting thread
        self.report_thread = threading.Thread(target=self._run_periodic_reports, daemon=True)
        self.report_thread.start()

        self.logger.info("Performance Tracker started (fresh session metrics only)")
    
    def stop(self) -> None:
        """Stop the performance tracker."""
        self.logger.info("Stopping Performance Tracker...")
        self.is_running = False
        
        # Save final report
        self._generate_performance_report()
        
        self.logger.info("Performance Tracker stopped")
    
    def _setup_trade_subscription(self) -> None:
        """Set up subscription to trade executions."""
        # Idempotent: avoid starting multiple subscriber threads on repeated start() calls.
        if self.subscription_thread is not None and self.subscription_thread.is_alive():
            return

        def trade_handler():
            pubsub = self.redis_manager.subscribe(["trade_executions"])
            for message in pubsub.listen():
                if not self.is_running:
                    break
                
                if message['type'] == 'message':
                    try:
                        trade_data = json.loads(message['data'])
                        self._process_trade_execution(trade_data)
                    except Exception as e:
                        self.logger.error(f"Error processing trade execution: {e}")
        
        # Start subscription thread
        self.subscription_thread = threading.Thread(target=trade_handler, daemon=True)
        self.subscription_thread.start()
    
    def _process_trade_execution(self, trade_data: Dict[str, Any]) -> None:
        """
        Process trade execution and update performance metrics.
        
        Args:
            trade_data: Trade execution data
        """
        try:
            # Only process events that represent an execution affecting position state.
            # SYNC_* are emitted by RiskManager when it discovers positions opened/closed
            # on the exchange outside this process (restart/manual close/liquidation/etc.).
            if trade_data['type'] not in ['FILLED', 'TAKE_PROFIT', 'STOP_LOSS', 'SYNC_OPEN', 'SYNC_CLOSE']:
                return

            symbol = trade_data['symbol']
            side = trade_data['side']
            quantity = float(trade_data['quantity'])
            price = float(trade_data['price'])
            timestamp = trade_data['timestamp']
            commission = float(trade_data.get('commission', 0.0))

            # Check if we have an open position for this symbol
            if symbol in self.open_positions:
                position = self.open_positions[symbol]
                
                # Check if this trade is in the same direction (Adding to position)
                if position['side'] == side:
                    # Update average entry price
                    total_cost = (position['price'] * position['quantity']) + (price * quantity)
                    total_quantity = position['quantity'] + quantity
                    position['price'] = total_cost / total_quantity
                    position['quantity'] = total_quantity
                    self.logger.info(f"Updated position for {symbol}: Increased size to {total_quantity}")
                
                # Trade is in opposite direction (Closing/Reducing position)
                else:
                    # Calculate PnL on the portion being closed
                    close_quantity = min(quantity, position['quantity'])
                    
                    if position['side'] == 'BUY': # Long position, Selling to close
                        pnl = (price - position['price']) * close_quantity
                        pnl_pct = (price - position['price']) / position['price']
                    else: # Short position, Buying to close
                        pnl = (position['price'] - price) * close_quantity
                        pnl_pct = (position['price'] - price) / position['price']
                    
                    # Record the trade
                    self._record_completed_trade(
                        symbol=symbol,
                        side=position['side'], # Record the side of the position being closed
                        entry_price=position['price'],
                        exit_price=price,
                        quantity=close_quantity,
                        entry_time=position['timestamp'],
                        exit_time=timestamp,
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        exit_reason=trade_data['type'],
                        commission=commission
                    )
                    
                    # Update remaining position
                    remaining_quantity = position['quantity'] - close_quantity
                    if remaining_quantity > 0.00000001: # Float tolerance
                        position['quantity'] = remaining_quantity
                        self.logger.info(f"Updated position for {symbol}: Reduced size to {remaining_quantity}")
                    else:
                        del self.open_positions[symbol]
                        self.logger.info(f"Closed position for {symbol}")
            
            else:
                # No open position, this is a new entry
                self.open_positions[symbol] = {
                    'symbol': symbol,
                    'side': side,
                    'price': price,
                    'quantity': quantity,
                    'timestamp': timestamp
                }
                self.logger.info(f"Opened new position tracking for {symbol} {side} @ {price}")

            # Update portfolio value tracking
            self._update_portfolio_tracking(trade_data)
                
        except Exception as e:
            self.logger.error(f"Error processing trade execution: {e}")

    def _record_completed_trade(self, symbol: str, side: str, entry_price: float, exit_price: float, 
                              quantity: float, entry_time: int, exit_time: int, pnl: float, 
                              pnl_pct: float, exit_reason: str, commission: float) -> None:
        """
        Record a completed trade.
        """
        try:
            # Calculate Net PnL (simplified commission handling)
            # Assuming commission is passed for the exit trade. 
            # Ideally we should track entry commission too, but for now let's use what we have.
            net_pnl = pnl - commission

            # Create trade record
            trade_record = TradeRecord(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                exit_price=exit_price,
                quantity=quantity,
                entry_time=entry_time,
                exit_time=exit_time,
                pnl=pnl,
                pnl_percentage=pnl_pct * 100,
                duration_seconds=(exit_time - entry_time) // 1000,
                exit_reason=exit_reason,
                commission=commission,
                net_pnl=net_pnl
            )
            
            # Add to records
            self.trade_records.append(trade_record)
            
            # Limit memory usage
            if len(self.trade_records) > self.max_trades_memory:
                self.trade_records = self.trade_records[-self.max_trades_memory:]
            
            # Update daily PnL
            self._update_daily_pnl(trade_record)
            
            # Store in Redis
            self._store_trade_record(trade_record)

            # Publish fresh aggregate metrics immediately so the dashboard updates
            # without waiting for the periodic report interval.
            self._generate_performance_report()
            
            self.logger.info(f"Recorded trade: {symbol} PnL=${pnl:.2f} ({pnl_pct*100:.2f}%)")
            
        except Exception as e:
            self.logger.error(f"Error recording completed trade: {e}")
    
    def _update_daily_pnl(self, trade_record: TradeRecord) -> None:
        """
        Update daily PnL tracking.
        
        Args:
            trade_record: Completed trade record
        """
        try:
            # Get date from exit time
            exit_date = datetime.fromtimestamp(trade_record.exit_time / 1000).strftime('%Y-%m-%d')
            
            if exit_date not in self.daily_pnl:
                self.daily_pnl[exit_date] = 0.0
            
            self.daily_pnl[exit_date] += trade_record.net_pnl
            
        except Exception as e:
            self.logger.error(f"Error updating daily PnL: {e}")
    
    def _update_portfolio_tracking(self, trade_data: Dict[str, Any]) -> None:
        """
        Update portfolio value tracking for drawdown calculation.
        
        Args:
            trade_data: Trade execution data
        """
        try:
            # This is simplified - in a real system you'd get actual portfolio value
            current_time = get_timestamp()
            
            # Estimate portfolio value (this would be replaced with actual account value)
            total_net_pnl = sum(record.net_pnl for record in self.trade_records)
            starting_balance = float(self.config.get('simulation', 'starting_balance', fallback='10000'))
            estimated_value = starting_balance + total_net_pnl
            
            # Update tracking
            self.portfolio_value_history.append((current_time, estimated_value))
            
            # Update peak value
            if estimated_value > self.peak_portfolio_value:
                self.peak_portfolio_value = estimated_value
            
            # Limit history size
            if len(self.portfolio_value_history) > 1000:
                self.portfolio_value_history = self.portfolio_value_history[-1000:]
                
        except Exception as e:
            self.logger.error(f"Error updating portfolio tracking: {e}")
    
    def _store_trade_record(self, trade_record: TradeRecord) -> None:
        """
        Store trade record in Redis.
        
        Args:
            trade_record: Trade record to store
        """
        try:
            key = f"trade_record:{trade_record.symbol}:{trade_record.exit_time}"
            self.redis_manager.set_data(key, asdict(trade_record), expiry=86400 * 30)  # 30 days
            
        except Exception as e:
            self.logger.error(f"Error storing trade record: {e}")
    
    def _load_historical_data(self) -> None:
        """Load historical trade records from Redis."""
        try:
            keys = self.redis_manager.get_keys("trade_record:*")
            
            for key in keys:
                try:
                    data = self.redis_manager.get_data(key)
                    if data:
                        trade_record = TradeRecord(**data)
                        self.trade_records.append(trade_record)
                except Exception as e:
                    self.logger.warning(f"Error loading trade record {key}: {e}")
            
            # Sort by exit time
            self.trade_records.sort(key=lambda x: x.exit_time)
            
            # Limit to max memory
            if len(self.trade_records) > self.max_trades_memory:
                self.trade_records = self.trade_records[-self.max_trades_memory:]
            
            self.logger.info(f"Loaded {len(self.trade_records)} historical trade records")
            
        except Exception as e:
            self.logger.error(f"Error loading historical data: {e}")
    
    def _run_periodic_reports(self) -> None:
        """Run periodic performance reporting."""
        while self.is_running:
            try:
                # Generate and log performance report
                self._generate_performance_report()
                
                # Sleep until next report
                time.sleep(self.report_interval * 60)
                
            except Exception as e:
                self.logger.error(f"Error in periodic reporting: {e}")
                time.sleep(60)
    
    def _generate_performance_report(self) -> None:
        """Generate and log performance report."""
        try:
            if len(self.trade_records) == 0:
                self.logger.info("No trades recorded yet for performance analysis")
                return

            # Calculate metrics
            metrics = self.calculate_performance_metrics()

            # Prepare dict and add UI compatibility / session fields
            metrics_dict = asdict(metrics)
            # Dashboard expects max_drawdown_pct (legacy field name); mirror value
            metrics_dict['max_drawdown_pct'] = metrics.max_drawdown
            # Include explicit session start timestamp for clarity
            metrics_dict['session_start_ts'] = self.start_time

            # Log summary
            self.logger.info(
                f"Performance Report - Trades: {metrics.total_trades}, "
                f"Win Rate: {metrics.win_rate:.1f}%, "
                f"Total PnL: ${metrics.total_pnl:.2f}, "
                f"Profit Factor: {metrics.profit_factor:.2f}"
            )

            # Store detailed metrics in Redis
            self.redis_manager.set_data("performance_metrics", metrics_dict, expiry=3600)

            # Publish performance update
            self.redis_manager.publish("performance_updates", metrics_dict)

        except Exception as e:
            self.logger.error(f"Error generating performance report: {e}")
    
    def calculate_performance_metrics(self) -> PerformanceMetrics:
        """
        Calculate comprehensive performance metrics.
        
        Returns:
            Performance metrics
        """
        if not self.trade_records:
            return PerformanceMetrics(
                total_trades=0, winning_trades=0, losing_trades=0, win_rate=0.0,
                total_pnl=0.0, total_fees=0.0, net_pnl=0.0, total_pnl_percentage=0.0, average_win=0.0, average_loss=0.0,
                largest_win=0.0, largest_loss=0.0, profit_factor=0.0, sharpe_ratio=0.0,
                max_drawdown=0.0, current_drawdown=0.0, average_trade_duration=0.0,
                trades_per_day=0.0, start_time=self.start_time, end_time=get_timestamp()
            )
        
        # Basic counts
        total_trades = len(self.trade_records)
        winning_trades = len([t for t in self.trade_records if t.pnl > 0])
        losing_trades = len([t for t in self.trade_records if t.pnl < 0])
        
        # Win rate
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0.0
        
        # PnL metrics
        total_pnl = sum(t.pnl for t in self.trade_records)
        total_fees = sum(t.commission for t in self.trade_records)
        net_pnl = total_pnl - total_fees
        total_pnl_percentage = sum(t.pnl_percentage for t in self.trade_records)
        
        # Win/Loss averages
        wins = [t.pnl for t in self.trade_records if t.pnl > 0]
        losses = [t.pnl for t in self.trade_records if t.pnl < 0]
        
        average_win = sum(wins) / len(wins) if wins else 0.0
        average_loss = sum(losses) / len(losses) if losses else 0.0
        
        largest_win = max(wins) if wins else 0.0
        largest_loss = min(losses) if losses else 0.0
        
        # Profit factor
        gross_profit = sum(wins) if wins else 0.0
        gross_loss = abs(sum(losses)) if losses else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # Sharpe ratio (simplified)
        if len(self.trade_records) > 1:
            returns = [t.pnl_percentage for t in self.trade_records]
            avg_return = sum(returns) / len(returns)
            std_return = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
            sharpe_ratio = avg_return / std_return if std_return > 0 else 0.0
        else:
            sharpe_ratio = 0.0
        
        # Drawdown calculation
        max_drawdown, current_drawdown = self._calculate_drawdown()
        
        # Duration metrics
        durations = [t.duration_seconds for t in self.trade_records]
        average_trade_duration = sum(durations) / len(durations) if durations else 0.0
        
        # Trading frequency
        time_span_days = (get_timestamp() - self.start_time) / (1000 * 86400)
        trades_per_day = total_trades / time_span_days if time_span_days > 0 else 0.0
        
        return PerformanceMetrics(
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            total_pnl=total_pnl,
            total_fees=total_fees,
            net_pnl=net_pnl,
            total_pnl_percentage=total_pnl_percentage,
            average_win=average_win,
            average_loss=average_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            profit_factor=profit_factor,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            current_drawdown=current_drawdown,
            average_trade_duration=average_trade_duration,
            trades_per_day=trades_per_day,
            start_time=self.start_time,
            end_time=get_timestamp()
        )

    
    def _calculate_drawdown(self) -> Tuple[float, float]:
        """
        Calculate maximum and current drawdown.
        
        Returns:
            (max_drawdown, current_drawdown) as percentages
        """
        if len(self.portfolio_value_history) < 2:
            return 0.0, 0.0
        
        peak = self.portfolio_value_history[0][1]
        max_drawdown = 0.0
        current_drawdown = 0.0
        
        for timestamp, value in self.portfolio_value_history:
            if value > peak:
                peak = value
            
            drawdown = (peak - value) / peak * 100 if peak > 0 else 0.0
            max_drawdown = max(max_drawdown, drawdown)
        
        # Current drawdown
        current_value = self.portfolio_value_history[-1][1]
        current_drawdown = (self.peak_portfolio_value - current_value) / self.peak_portfolio_value * 100 if self.peak_portfolio_value > 0 else 0.0
        
        return max_drawdown, current_drawdown
    
    def get_daily_pnl_report(self, days: int = 7) -> Dict[str, float]:
        """
        Get daily PnL report for the last N days.
        
        Args:
            days: Number of days to include
            
        Returns:
            Daily PnL data
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            daily_report = {}
            for i in range(days):
                date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
                daily_report[date] = self.daily_pnl.get(date, 0.0)
            
            return daily_report
            
        except Exception as e:
            self.logger.error(f"Error generating daily PnL report: {e}")
            return {}
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the Performance Tracker.
        
        Returns:
            Health status information
        """
        try:
            metrics = self.calculate_performance_metrics() if self.trade_records else None
            
            return {
                'is_running': self.is_running,
                'trades_recorded': len(self.trade_records),
                'daily_pnl_days': len(self.daily_pnl),
                'portfolio_history_points': len(self.portfolio_value_history),
                'current_total_pnl': metrics.total_pnl if metrics else 0.0,
                'current_win_rate': metrics.win_rate if metrics else 0.0,
                'report_interval_minutes': self.report_interval,
                'system_uptime_hours': (get_timestamp() - self.start_time) / (1000 * 3600)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting health status: {e}")
            return {
                'is_running': self.is_running,
                'error': str(e)
            }
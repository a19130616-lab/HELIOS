import csv
import os
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from app.utils import RedisManager

class CSVDecisionLogger:
    """
    Logs detailed trading decisions and market state to a CSV file for analysis.
    """
    def __init__(self, redis_manager: Optional[RedisManager] = None, mode_resolver: Optional[Callable[[], str]] = None, log_dir: str = "logs"):
        self.log_dir = log_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Create a new log file for each day
        date_str = datetime.now().strftime("%Y-%m-%d")
        self.filename = os.path.join(log_dir, f"decisions_{date_str}.csv")
        
        # Define columns
        self.columns = [
            "timestamp", "symbol", "price", "nobi", "rsi", 
            "volume_ratio", "vwap_gap_pct", "ml_confidence", 
            "decision", "reason", "entry_price", "stop_loss", 
            "take_profit", "realized_pnl", "position_size", 
            "capital_used_usd", "mode"
        ]
        
        # Initialize file with headers if it doesn't exist
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.columns)

    def log_decision(self, data: Dict[str, Any]):
        """
        Log a single decision event.
        
        Args:
            data: Dictionary containing values for all columns
        """
        try:
            # Ensure file exists (handle day rollover if needed)
            current_date = datetime.now().strftime("%Y-%m-%d")
            if current_date not in self.filename:
                self.filename = os.path.join(self.log_dir, f"decisions_{current_date}.csv")
                if not os.path.exists(self.filename):
                    with open(self.filename, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(self.columns)

            with open(self.filename, 'a', newline='') as f:
                writer = csv.writer(f)
                # Handle missing keys gracefully
                row = [data.get(col, "") for col in self.columns]
                writer.writerow(row)
                
        except Exception as e:
            print(f"Error logging decision: {e}")

def clear_daily_log(log_dir: str = "logs"):
    """Clear the current day's log file on startup."""
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = os.path.join(log_dir, f"decisions_{date_str}.csv")
        
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Cleared daily log file: {filename}")
    except Exception as e:
        print(f"Error clearing daily log: {e}")

import json
import logging
import threading
import time
from typing import Callable, Optional, Dict, Any, List
from copy import deepcopy

from app.models import DecisionLog, SignalDirection
from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config


class DecisionLogger:
    """
    Consumes published trading signals and stores normalized DecisionLog
    entries in Redis (list key: 'decision_logs').
    """

    def __init__(
        self,
        redis_manager: RedisManager,
        mode_resolver: Callable[[], str],
        list_key: str = "decision_logs",
        max_entries: int = 500,
        ttl_seconds: int = 86400,
    ):
        """
        Initialize DecisionLogger.

        Args:
            redis_manager: Redis manager instance
            mode_resolver: Callable returning current mode string ('live'|'public'|'synthetic')
            list_key: Redis list key for decision logs
            max_entries: Maximum number of decision logs to retain
            ttl_seconds: Expiry for the list key (resets on each push)
        """
        self.redis_manager = redis_manager
        self.mode_resolver = mode_resolver
        self.list_key = list_key
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds

        # Open positions tracked for simulated/public PnL reconstruction
        # Structure: { symbol: { "direction": SignalDirection, "entry_price": float,
        #                        "timestamp": int, "size": float } }
        self._open_positions: Dict[str, Dict[str, Any]] = {}

        cfg = None
        try:
            cfg = get_config()
        except Exception:
            pass

        # Base simulated position size (legacy fallback)
        self.position_size: float = 1.0
        if cfg:
            try:
                self.position_size = cfg.get("simulation", "position_size", float, fallback=1.0)
            except Exception:
                pass

        # Guardrail / risk parameters (all optional / safe fallbacks)
        def g(section, key, cast, fb):
            if not cfg:
                return fb
            try:
                return cfg.get(section, key, cast, fallback=fb)
            except Exception:
                return fb

        self.capital_per_trade: float = g("simulation", "capital_per_trade", float, 0.0)
        self.cooldown_ms: int = int(g("simulation", "cooldown_ms", int, 60000))
        self.loss_cooldown_ms: int = int(g("simulation", "loss_cooldown_ms", int, 120000))
        self.max_concurrent_positions: int = int(g("simulation", "max_concurrent_positions", int, 3))
        self.max_hold_minutes: int = int(g("simulation", "max_hold_minutes", int, 15))
        self.stop_loss_pct: float = g("simulation", "stop_loss_pct", float, 1.0)  # percent
        self.take_profit_pct: float = g("simulation", "take_profit_pct", float, 1.5)
        self.starting_balance: float = g("simulation", "starting_balance", float, 10000.0)
        
        # Futures trading parameters
        self.futures_mode: bool = g("simulation", "futures_mode", bool, False)
        self.leverage: float = g("simulation", "leverage", float, 1.0)
        self.maker_fee_pct: float = g("simulation", "maker_fee_pct", float, 0.02)
        self.taker_fee_pct: float = g("simulation", "taker_fee_pct", float, 0.04)
        
        # Trading Mode Logic
        self.trading_mode: str = g("simulation", "mode", str, "HF_MARKET")
        
        # Apply Mode Overrides
        if "LF" in self.trading_mode:
            # Low Frequency Mode - Increase cooldowns significantly
            # 15 minutes entry cooldown, 30 minutes loss cooldown
            self.cooldown_ms = max(self.cooldown_ms, 900000)
            self.loss_cooldown_ms = max(self.loss_cooldown_ms, 1800000)
            logging.getLogger(__name__).info(f"Low Frequency Mode ({self.trading_mode}) active: Cooldowns increased to {self.cooldown_ms}ms / {self.loss_cooldown_ms}ms")

        # Daily loss limit (for small capital protection)
        self.daily_loss_limit_pct: float = g("simulation", "daily_loss_limit_pct", float, 5.0)
        self.max_drawdown_alert_pct: float = g("simulation", "max_drawdown_alert_pct", float, 15.0)
        
        # Track daily PnL for loss limit
        self._daily_pnl: float = 0.0
        self._last_reset_date: Optional[str] = None

        # Internal guardrail state
        self._last_entry_time_per_symbol: Dict[str, int] = {}
        self._last_exit_info_per_symbol: Dict[str, Dict[str, Any]] = {}  # {symbol: {'ts': int, 'pnl': float}}
        self._stop_event = threading.Event()

        # Streak tracking (for enhanced metrics)
        self._current_win_streak = 0
        self._current_loss_streak = 0
        self._max_win_streak = 0
        self._max_loss_streak = 0
        self._sum_wins = 0.0
        self._sum_losses = 0.0

        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self._decisions_logged = 0
        self._last_decision_ts: Optional[int] = None

        self._pubsub = None

        # Guardrail policing thread (time exits / protective stops)
        self._guardrail_thread: Optional[threading.Thread] = None
        self._guardrail_interval_sec = 5

        # Lock for position / summary updates
        self._lock = threading.RLock()
        
        # CSV Logger for postmortem analysis
        self.csv_logger = CSVDecisionLogger(log_dir="logs")

    # ------------------------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------------------------
    def start(self) -> None:
        """Start the DecisionLogger subscriber and guardrail thread."""
        if self.is_running:
            self.logger.warning("DecisionLogger already running")
            return

        try:
            self.logger.info("Starting DecisionLogger (subscribing to 'trading_signals')...")
            self._pubsub = self.redis_manager.subscribe(["trading_signals"])
            self.is_running = True
            self._stop_event.clear()
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            # Guardrail supervisor
            self._guardrail_thread = threading.Thread(target=self._risk_guardrail_loop, daemon=True)
            self._guardrail_thread.start()
            self.logger.info("DecisionLogger started")
        except Exception as e:
            self.logger.error(f"Failed to start DecisionLogger: {e}")
            self.is_running = False
            raise

    def stop(self) -> None:
        """Stop the DecisionLogger."""
        if not self.is_running:
            return
        self.logger.info("Stopping DecisionLogger...")
        self.is_running = False
        self._stop_event.set()
        try:
            if self._pubsub:
                try:
                    self._pubsub.close()
                except Exception:
                    pass
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=5)
            if self._guardrail_thread and self._guardrail_thread.is_alive():
                self._guardrail_thread.join(timeout=5)
        except Exception as e:
            self.logger.error(f"Error stopping DecisionLogger: {e}")
        self.logger.info("DecisionLogger stopped")

    # ------------------------------------------------------------------------------------
    # Subscriber Loop
    # ------------------------------------------------------------------------------------
    def _run(self) -> None:
        """Internal subscriber loop."""
        try:
            for message in self._pubsub.listen():
                if not self.is_running or self._stop_event.is_set():
                    break
                if message.get("type") != "message":
                    continue
                raw = message.get("data")
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                except Exception as parse_err:
                    self.logger.error(f"Failed to parse signal payload: {parse_err}")
                    continue

                self._handle_signal_payload(payload)
        except Exception as e:
            if self.is_running:
                self.logger.error(f"DecisionLogger subscriber error: {e}")

    # ------------------------------------------------------------------------------------
    # Guardrail Supervisor
    # ------------------------------------------------------------------------------------
    def _risk_guardrail_loop(self) -> None:
        """Periodic loop to enforce time-based and percent protective exits."""
        while self.is_running and not self._stop_event.is_set():
            try:
                now = get_timestamp()
                max_hold_ms = self.max_hold_minutes * 60 * 1000
                symbols = list(self._open_positions.keys())
                for symbol in symbols:
                    with self._lock:
                        pos = self._open_positions.get(symbol)
                        if not pos:
                            continue
                        entry_ts = pos["timestamp"]
                        direction: SignalDirection = pos["direction"]
                        entry_price = pos["entry_price"]
                        size = pos.get("size", self.position_size)

                    # Time-based exit
                    if max_hold_ms > 0 and now - entry_ts >= max_hold_ms:
                        current_price = self._fetch_mid_price(symbol) or entry_price
                        self._synthesize_exit(symbol, direction, entry_price, current_price, size,
                                              reason="time_stop")
                        continue  # Skip further checks if exited

                    # Protective stops (percent)
                    current_price = self._fetch_mid_price(symbol)
                    if current_price:
                        if self._percent_stop_triggered(direction, entry_price, current_price):
                            self._synthesize_exit(symbol, direction, entry_price, current_price, size,
                                                  reason="protective_stop")
            except Exception as e:
                self.logger.error(f"Guardrail loop error: {e}")
            finally:
                time.sleep(self._guardrail_interval_sec)

    def _fetch_mid_price(self, symbol: str) -> Optional[float]:
        """Fetch current mid price from order book cache."""
        try:
            ob = self.redis_manager.get_data(f"orderbook:{symbol}")
            if not ob:
                return None
            return float(ob.get("mid_price") or 0) or None
        except Exception:
            return None

    def _percent_stop_triggered(self, direction: SignalDirection, entry_price: float, current_price: float) -> bool:
        """Check percent stop conditions."""
        if entry_price <= 0 or current_price <= 0:
            return False
        sl_mult = (100 - self.stop_loss_pct) / 100.0
        tp_mult = (100 + self.take_profit_pct) / 100.0
        if direction == SignalDirection.LONG:
            if current_price <= entry_price * sl_mult:
                return True
            if current_price >= entry_price * tp_mult:
                return True
        elif direction == SignalDirection.SHORT:
            if current_price >= entry_price * (2 - sl_mult):  # (entry * (1 + stop_loss_pct%))
                return True
            if current_price <= entry_price * (2 - tp_mult):  # (entry * (1 - take_profit_pct%))
                return True
        return False

    def _get_fee_rate(self, order_type: str) -> float:
        """
        Calculate fee rate based on trading mode and order type.
        
        Args:
            order_type: "ENTRY", "EXIT_NORMAL", "EXIT_STOP", "EXIT_TIME"
            
        Returns:
            Fee rate as a decimal (e.g. 0.0004 for 0.04%)
        """
        # Default to Taker Fee (Market Order)
        fee_pct = self.taker_fee_pct
        
        # Check for Limit Order Modes
        if "LIMIT" in self.trading_mode:
            if order_type == "ENTRY":
                fee_pct = self.maker_fee_pct
            elif order_type == "EXIT_NORMAL":
                # Take Profit or Signal Exit -> Limit Order
                fee_pct = self.maker_fee_pct
            elif order_type == "EXIT_STOP":
                # Hard Stop Loss -> Market Order (Taker)
                fee_pct = self.taker_fee_pct
            elif order_type == "EXIT_TIME":
                # Time Exit -> Market Order (Taker) to ensure exit
                fee_pct = self.taker_fee_pct
                
        return fee_pct / 100.0

    # ------------------------------------------------------------------------------------
    # Signal Handling
    # ------------------------------------------------------------------------------------
    def _handle_signal_payload(self, payload: Dict[str, Any]) -> None:
        """Convert raw/EXIT signal payload into DecisionLog, manage open positions, compute simulated PnL."""
        try:
            symbol = payload.get("symbol")
            if not symbol:
                return
            direction_raw = payload.get("direction")
            timestamp = int(payload.get("timestamp", get_timestamp()))
            confidence = float(payload.get("confidence", 0.0))
            nobi_value = float(payload.get("nobi_value", 0.0))
            px = payload.get("entry_price") or payload.get("price")
            try:
                entry_price = float(px) if px is not None else 0.0
            except Exception:
                entry_price = 0.0

            # Accept extended enum (LONG, SHORT, EXIT_LONG, EXIT_SHORT)
            valid_dirs = {d.name for d in SignalDirection}
            if direction_raw not in valid_dirs:
                self.logger.debug(f"Skipping decision with invalid direction: {direction_raw}")
                return

            direction = SignalDirection[direction_raw]
            mode = self._safe_mode()

            realized_pnl: Optional[float] = None
            pos_size: Optional[float] = None
            total_fees: Optional[float] = None

            with self._lock:
                # Check and reset daily PnL if new day
                self._check_daily_reset()
                
                # Check daily loss limit
                if self._is_daily_loss_limit_reached():
                    self.logger.warning(f"Daily loss limit reached ({self.daily_loss_limit_pct}% = ${abs(self._daily_pnl):.2f}) - Logging only, trading simulation continues")
                    # return  <-- Disabled to keep trading in simulation
                
                # ENTRY directions
                if direction in (SignalDirection.LONG, SignalDirection.SHORT):
                    # Guardrail: max concurrent positions
                    if symbol not in self._open_positions and len(self._open_positions) >= self.max_concurrent_positions:
                        self.logger.debug(f"Skipping entry for {symbol}: max_concurrent_positions reached")
                        return

                    # Guardrail: cooldown (adaptive if last exit was a loss)
                    last_entry_ts = self._last_entry_time_per_symbol.get(symbol, 0)
                    last_exit = self._last_exit_info_per_symbol.get(symbol)
                    adaptive_cooldown = self.cooldown_ms
                    if last_exit and last_exit.get("pnl", 0) < 0:
                        adaptive_cooldown = max(adaptive_cooldown, self.loss_cooldown_ms)
                    if timestamp - last_entry_ts < adaptive_cooldown:
                        self.logger.debug(f"Skipping entry for {symbol}: cooldown active")
                        return

                    existing = self._open_positions.get(symbol)
                    if existing and existing["direction"] == direction:
                        # Same-direction suppression
                        self.logger.debug(f"Suppressing duplicate {direction.name} for {symbol}")
                        return

                    # Implicit opposite exit
                    if existing and existing["direction"] != direction:
                        existing_dir: SignalDirection = existing["direction"]
                        pos_size = float(existing.get("size", self.position_size))
                        e_price = float(existing.get("entry_price", entry_price))
                        
                        # Calculate PnL with leverage and fees
                        if existing_dir == SignalDirection.LONG:
                            base_pnl = (entry_price - e_price) * pos_size
                            exit_dir = SignalDirection.EXIT_LONG
                        else:
                            base_pnl = (e_price - entry_price) * pos_size
                            exit_dir = SignalDirection.EXIT_SHORT
                        
                        # Apply leverage for futures
                        if self.futures_mode:
                            base_pnl *= self.leverage
                        
                        # Calculate and deduct trading fees
                        entry_value = e_price * pos_size
                        exit_value = entry_price * pos_size
                        
                        entry_fee_rate = self._get_fee_rate("ENTRY")
                        exit_fee_rate = self._get_fee_rate("EXIT_NORMAL")
                        
                        entry_fee = entry_value * entry_fee_rate
                        exit_fee = exit_value * exit_fee_rate
                        total_fees = entry_fee + exit_fee
                        
                        # We already deducted entry_fee from summary at entry time.
                        # So we only deduct exit_fee and add gross_pnl now.
                        # realized_impact = base_pnl - exit_fee
                        
                        realized_net_trade = base_pnl - total_fees
                        
                        self._open_positions.pop(symbol, None)
                        self._update_simulated_summary(base_pnl - exit_fee, commission=exit_fee, is_trade_close=True)
                        self._record_exit(symbol, realized_net_trade)
                        exit_decision = DecisionLog(
                            symbol=symbol,
                            direction=exit_dir,
                            timestamp=timestamp,
                            confidence=confidence,
                            nobi_value=nobi_value,
                            entry_price=entry_price,  # exit price context
                            mode=mode,
                            reason="implicit_exit_opposite_signal",
                            realized_pnl=realized_net_trade,
                            position_size=pos_size,
                            commission=total_fees,
                        )
                        # Augment decision with USD notional / capital used for dashboard visibility
                        dec_dict = exit_decision.to_dict()
                        try:
                            dec_dict['position_value_usd'] = (dec_dict.get('position_size') or 0) * (dec_dict.get('entry_price') or 0)
                            if self.futures_mode:
                                dec_dict['capital_used_usd'] = dec_dict['position_value_usd'] / (self.leverage or 1.0)
                        except Exception:
                            pass
                        self._push_decision(dec_dict)

                    # Determine position size
                    size = self.position_size
                    
                    # Calculate current equity for dynamic sizing
                    summary = self.redis_manager.get_data("simulated_pnl_summary") or {}
                    total_pnl = float(summary.get("total_pnl", 0.0))
                    current_equity = self.starting_balance + total_pnl
                    
                    if self.capital_per_trade > 0:
                        # Fixed capital per trade
                        effective_capital = self.capital_per_trade
                        if self.futures_mode:
                            effective_capital = self.capital_per_trade * self.leverage
                        if entry_price > 0:
                            size = effective_capital / entry_price
                    elif self.capital_per_trade <= 0:
                        # Dynamic sizing: 1% of current equity
                        risk_pct = 0.01
                        effective_capital = current_equity * risk_pct
                        if self.futures_mode:
                            effective_capital = effective_capital * self.leverage
                        if entry_price > 0:
                            size = effective_capital / entry_price
                            
                    pos_size = size

                    # Calculate entry fee for reporting
                    entry_value = entry_price * pos_size
                    fee_rate = self._get_fee_rate("ENTRY")
                    entry_fee = entry_value * fee_rate
                    
                    # Deduct entry fee from summary immediately
                    self._update_simulated_summary(-entry_fee, commission=entry_fee, is_trade_close=False)

                    # Open / overwrite
                    self._open_positions[symbol] = {
                        "direction": direction,
                        "entry_price": entry_price,
                        "timestamp": timestamp,
                        "size": pos_size,
                    }
                    self._last_entry_time_per_symbol[symbol] = timestamp

                # Explicit EXIT directions
                elif direction in (SignalDirection.EXIT_LONG, SignalDirection.EXIT_SHORT):
                    existing = self._open_positions.get(symbol)
                    if existing:
                        entry_dir: SignalDirection = existing["direction"]
                        if (entry_dir == SignalDirection.LONG and direction == SignalDirection.EXIT_LONG) or \
                           (entry_dir == SignalDirection.SHORT and direction == SignalDirection.EXIT_SHORT):
                            pos_size = float(existing.get("size", self.position_size))
                            e_price = float(existing.get("entry_price", entry_price))
                            
                            # Calculate PnL with leverage and fees
                            if entry_dir == SignalDirection.LONG:
                                base_pnl = (entry_price - e_price) * pos_size
                            else:
                                base_pnl = (e_price - entry_price) * pos_size
                            
                            # Apply leverage for futures
                            if self.futures_mode:
                                base_pnl *= self.leverage
                            
                            # Calculate and deduct trading fees
                            entry_value = e_price * pos_size
                            exit_value = entry_price * pos_size
                            
                            entry_fee_rate = self._get_fee_rate("ENTRY")
                            exit_fee_rate = self._get_fee_rate("EXIT_NORMAL")
                            
                            entry_fee = entry_value * entry_fee_rate
                            exit_fee = exit_value * exit_fee_rate
                            total_fees = entry_fee + exit_fee
                            
                            realized_pnl = base_pnl - total_fees
                            
                            self._open_positions.pop(symbol, None)
                            self._update_simulated_summary(base_pnl - exit_fee, commission=exit_fee, is_trade_close=True)
                            self._record_exit(symbol, realized_pnl)
                    # Orphan EXIT otherwise just logs

            decision = DecisionLog(
                symbol=symbol,
                direction=direction,
                timestamp=timestamp,
                confidence=confidence,
                nobi_value=nobi_value,
                entry_price=entry_price,
                mode=mode,
                reason=payload.get("reason"),
                stop_loss=payload.get("stop_loss"),
                take_profit=payload.get("take_profit"),
                notes=payload.get("notes"),
                realized_pnl=realized_pnl,
                position_size=pos_size if pos_size is not None else (
                    self.position_size if direction in (SignalDirection.LONG, SignalDirection.SHORT) else None
                ),
                commission=entry_fee if direction in (SignalDirection.LONG, SignalDirection.SHORT) else total_fees,
            )
    
            decision_dict = decision.to_dict()
            # Add dollar amount metadata for dashboard: notional / capital used
            try:
                decision_dict['position_value_usd'] = (decision_dict.get('position_size') or 0) * (decision_dict.get('entry_price') or 0)
                if self.futures_mode:
                    decision_dict['capital_used_usd'] = decision_dict['position_value_usd'] / (self.leverage or 1.0)
            except Exception:
                pass
    
            self._push_decision(decision_dict)
            self._persist_open_positions()

            self._decisions_logged += 1
            self._last_decision_ts = timestamp

        except Exception as e:
            self.logger.error(f"Error handling decision payload: {e}")

    # ------------------------------------------------------------------------------------
    # Synthetic Exit Helper
    # ------------------------------------------------------------------------------------
    def _synthesize_exit(self,
                         symbol: str,
                         direction: SignalDirection,
                         entry_price: float,
                         exit_price: float,
                         size: float,
                         reason: str) -> None:
        """Emit a synthetic EXIT decision (time / protective stop)."""
        with self._lock:
            existing = self._open_positions.get(symbol)
            if not existing:
                return
            # Calculate PnL with leverage and fees
            if direction == SignalDirection.LONG:
                base_pnl = (exit_price - entry_price) * size
                exit_dir = SignalDirection.EXIT_LONG
            else:
                base_pnl = (entry_price - exit_price) * size
                exit_dir = SignalDirection.EXIT_SHORT
            
            # Apply leverage for futures
            if self.futures_mode:
                base_pnl *= self.leverage
            
            # Calculate and deduct trading fees
            entry_value = entry_price * size
            exit_value = exit_price * size
            
            entry_fee_rate = self._get_fee_rate("ENTRY")
            
            # Determine exit type based on reason
            exit_type = "EXIT_NORMAL"
            if "protective_stop" in reason.lower():
                 exit_type = "EXIT_STOP"
            elif "time" in reason.lower():
                 exit_type = "EXIT_TIME"
            
            exit_fee_rate = self._get_fee_rate(exit_type)
            
            entry_fee = entry_value * entry_fee_rate
            exit_fee = exit_value * exit_fee_rate
            total_fees = entry_fee + exit_fee
            
            realized = base_pnl - total_fees
            
            self._open_positions.pop(symbol, None)
            self._update_simulated_summary(base_pnl - exit_fee, commission=exit_fee, is_trade_close=True)
            self._record_exit(symbol, realized)

        exit_decision = DecisionLog(
            symbol=symbol,
            direction=exit_dir,
            timestamp=get_timestamp(),
            confidence=existing.get("confidence", 0.0) if existing else 0.0,
            nobi_value=0.0,
            entry_price=exit_price,
            mode=self._safe_mode(),
            reason=reason,
            realized_pnl=realized,
            position_size=size,
            commission=total_fees,
        )
        # Augment with USD notional for dashboard
        ed = exit_decision.to_dict()
        try:
            ed['position_value_usd'] = (ed.get('position_size') or 0) * (ed.get('entry_price') or 0)
            if self.futures_mode:
                ed['capital_used_usd'] = ed['position_value_usd'] / (self.leverage or 1.0)
        except Exception:
            pass
        self._push_decision(ed)
        self._persist_open_positions()

    # ------------------------------------------------------------------------------------
    # Mode / Summary / Persistence
    # ------------------------------------------------------------------------------------
    def _safe_mode(self) -> str:
        """Resolve mode safely with fallback."""
        try:
            mode = self.mode_resolver()
            if mode not in ("live", "public", "synthetic"):
                return "public"
            return mode
        except Exception:
            return "public"

    def _record_exit(self, symbol: str, realized_pnl: float) -> None:
        """Store last exit info for cooldown adaptation."""
        self._last_exit_info_per_symbol[symbol] = {
            "ts": get_timestamp(),
            "pnl": realized_pnl,
        }

    def _update_simulated_summary(self, realized_pnl: Optional[float], commission: float = 0.0, is_trade_close: bool = True) -> None:
        """Update aggregated simulated PnL summary in Redis on each realized exit.

        Drawdown baseline fix: equity = starting_balance + total_pnl
        Enhanced metrics: profit_factor, avg_pnl, streaks.
        """
        if realized_pnl is None:
            return
        
        # Update daily PnL tracking
        self._daily_pnl += realized_pnl
        
        key = "simulated_pnl_summary"
        try:
            with self._lock:
                summary = self.redis_manager.get_data(key) or {}
                total_pnl = float(summary.get("total_pnl", 0.0)) + realized_pnl
                total_fees = float(summary.get("total_fees", 0.0)) + commission
                
                # Only update trade counts and streaks if this is a trade close
                total_trades = int(summary.get("total_trades", 0))
                wins = int(summary.get("wins", 0))
                losses = int(summary.get("losses", 0))
                
                if is_trade_close:
                    total_trades += 1
                    if realized_pnl >= 0:
                        wins += 1
                        self._current_win_streak += 1
                        self._sum_wins += realized_pnl
                        self._current_loss_streak = 0
                        if self._current_win_streak > self._max_win_streak:
                            self._max_win_streak = self._current_win_streak
                    else:
                        losses += 1
                        self._current_loss_streak += 1
                        self._sum_losses += realized_pnl  # negative
                        self._current_win_streak = 0
                        if self._current_loss_streak > self._max_loss_streak:
                            self._max_loss_streak = self._current_loss_streak

                # Equity tracking
                equity = self.starting_balance + total_pnl
                peak_equity = float(summary.get("peak_equity", self.starting_balance))
                if equity > peak_equity:
                    peak_equity = equity
                max_drawdown_pct = float(summary.get("max_drawdown_pct", 0.0))
                if peak_equity > 0:
                    current_dd = (peak_equity - equity) / peak_equity * 100.0
                    if current_dd > max_drawdown_pct:
                        max_drawdown_pct = current_dd

                win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
                avg_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
                avg_win = (self._sum_wins / wins) if wins > 0 else 0.0
                avg_loss = (self._sum_losses / losses) if losses > 0 else 0.0  # negative
                profit_factor = (self._sum_wins / abs(self._sum_losses)) if self._sum_losses != 0 else 0.0
                current_streak = max(self._current_win_streak, self._current_loss_streak) * (
                    1 if self._current_win_streak > 0 else -1 if self._current_loss_streak > 0 else 0
                )

                new_summary = {
                    "total_pnl": total_pnl,
                    "total_fees": total_fees,
                    "total_trades": total_trades,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": win_rate,
                    "max_drawdown_pct": max_drawdown_pct,
                    "peak_equity": peak_equity,
                    "equity": equity,
                    "starting_balance": self.starting_balance,
                    "avg_pnl": avg_pnl,
                    "avg_win": avg_win,
                    "avg_loss": avg_loss,
                    "profit_factor": profit_factor,
                    "current_win_streak": self._current_win_streak,
                    "current_loss_streak": self._current_loss_streak,
                    "max_win_streak": self._max_win_streak,
                    "max_loss_streak": self._max_loss_streak,
                    "current_streak": current_streak,
                    "futures_mode": self.futures_mode,
                    "leverage": self.leverage if self.futures_mode else 1.0,
                    "daily_pnl": self._daily_pnl,
                    "updated_ts": get_timestamp(),
                }
                self.redis_manager.set_data(key, new_summary, expiry=7 * 24 * 3600)
                
                # Check for drawdown alert
                if max_drawdown_pct > self.max_drawdown_alert_pct:
                    self.logger.warning(f"ALERT: Max drawdown {max_drawdown_pct:.2f}% exceeds alert threshold {self.max_drawdown_alert_pct}%")
        except Exception as e:
            self.logger.error(f"Failed updating simulated_pnl_summary: {e}")

    # ---- Helpers for persistence / logging ----
    def _persist_open_positions(self) -> None:
        """Persist current open positions to Redis for external components (unrealized PnL calc).

        BUGFIX (retained): use correct kwarg 'expiry' so key actually gets stored.
        """
        try:
            with self._lock:
                serializable = {}
                for sym, pos in self._open_positions.items():
                    serializable[sym] = {
                        "direction": pos["direction"].name if isinstance(pos["direction"], SignalDirection) else pos["direction"],
                        "entry_price": pos["entry_price"],
                        "timestamp": pos["timestamp"],
                        "size": pos.get("size", self.position_size),
                    }
            self.redis_manager.set_data("open_positions", serializable, expiry=3600)
        except Exception:
            pass

    def _push_decision(self, decision_dict: Dict[str, Any]) -> None:
        """Push a single decision record to Redis list with trimming & TTL."""
        try:
            # Log to CSV for postmortem
            self.csv_logger.log_decision(decision_dict)
            
            pipe = self.redis_manager.redis_client.pipeline()
            pipe.lpush(self.list_key, json.dumps(decision_dict))
            pipe.ltrim(self.list_key, 0, self.max_entries - 1)
            pipe.expire(self.list_key, self.ttl_seconds)
            pipe.execute()
        except Exception as e:
            self.logger.error(f"Failed to push decision log: {e}")

    # ------------------------------------------------------------------------------------
    # Public Health
    # ------------------------------------------------------------------------------------
    def get_health_status(self) -> Dict[str, Any]:
        """Return component health/status snapshot."""
        return {
            "is_running": self.is_running,
            "decisions_logged": self._decisions_logged,
            "last_decision_ts": self._last_decision_ts,
            "list_key": self.list_key,
            "max_entries": self.max_entries,
            "ttl_seconds": self.ttl_seconds,
            "open_positions": len(self._open_positions),
            "guardrails": {
                "capital_per_trade": self.capital_per_trade,
                "cooldown_ms": self.cooldown_ms,
                "loss_cooldown_ms": self.loss_cooldown_ms,
                "max_concurrent_positions": self.max_concurrent_positions,
                "max_hold_minutes": self.max_hold_minutes,
                "stop_loss_pct": self.stop_loss_pct,
                "take_profit_pct": self.take_profit_pct,
                "futures_mode": self.futures_mode,
                "leverage": self.leverage,
                "maker_fee_pct": self.maker_fee_pct,
                "taker_fee_pct": self.taker_fee_pct,
                "daily_loss_limit_pct": self.daily_loss_limit_pct,
                "daily_pnl": self._daily_pnl,
            },
        }
    
    # ------------------------------------------------------------------------------------
    # Daily Loss Limit Management
    # ------------------------------------------------------------------------------------
    def _check_daily_reset(self) -> None:
        """Check if we need to reset daily PnL tracking (new trading day)."""
        import datetime
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        if self._last_reset_date != current_date:
            self._daily_pnl = 0.0
            self._last_reset_date = current_date
            self.logger.info(f"Daily PnL reset for new trading day: {current_date}")
    
    def _is_daily_loss_limit_reached(self) -> bool:
        """Check if daily loss limit has been reached."""
        if self.daily_loss_limit_pct <= 0:
            return False  # No limit set
        
        daily_loss_limit = self.starting_balance * (self.daily_loss_limit_pct / 100.0)
        return self._daily_pnl <= -daily_loss_limit
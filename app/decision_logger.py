"""
Decision Logger component for Helios Trading System.

Subscribes to the 'trading_signals' Redis pub/sub channel and persists
recent decision logs (signals) into a Redis list for UI/API retrieval
and later auditing.

Key Features:
- Works in all modes (live/public/synthetic)
- Stores bounded list (default keep last 500)
- Applies TTL (default 24h) so old data expires automatically
- Lightweight subscriber thread (non-blocking to main system)
"""

import json
import logging
import threading
import time
from typing import Callable, Optional, Dict, Any

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
        # Structure: { symbol: { "direction": SignalDirection, "entry_price": float, "timestamp": int } }
        self._open_positions: Dict[str, Dict[str, Any]] = {}
        # Configurable simulated position size
        self.position_size: float = 1.0
        try:
            cfg = get_config()
            # Optional section; fallback if absent
            self.position_size = cfg.get("simulation", "position_size", float, fallback=1.0)
        except Exception:
            pass

        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self._decisions_logged = 0
        self._last_decision_ts: Optional[int] = None

        self._pubsub = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the DecisionLogger subscriber."""
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
        except Exception as e:
            self.logger.error(f"Error stopping DecisionLogger: {e}")
        self.logger.info("DecisionLogger stopped")

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

            # Enhanced position / PnL handling:
            # 1. Opposite entry acts as an implicit EXIT of existing position (auto-closing)
            # 2. Explicit EXIT_* still supported
            # 3. Open positions persisted to Redis for unrealized PnL display
            if direction in (SignalDirection.LONG, SignalDirection.SHORT):
                existing = self._open_positions.get(symbol)
                if existing:
                    existing_dir: SignalDirection = existing["direction"]
                    if existing_dir != direction:
                        # Implicit exit before opening new position
                        pos_size = float(existing.get("size", self.position_size))
                        e_price = float(existing.get("entry_price", entry_price))
                        if existing_dir == SignalDirection.LONG:
                            realized = (entry_price - e_price) * pos_size
                            exit_dir = SignalDirection.EXIT_LONG
                        else:
                            realized = (e_price - entry_price) * pos_size
                            exit_dir = SignalDirection.EXIT_SHORT
                        # Remove old position
                        self._open_positions.pop(symbol, None)
                        # Update aggregates
                        self._update_simulated_summary(realized)
                        # Emit synthetic EXIT decision log for audit
                        exit_decision = DecisionLog(
                            symbol=symbol,
                            direction=exit_dir,
                            timestamp=timestamp,
                            confidence=confidence,
                            nobi_value=nobi_value,
                            entry_price=entry_price,  # exit price context
                            mode=mode,
                            reason="implicit_exit_opposite_signal",
                            realized_pnl=realized,
                            position_size=pos_size,
                        )
                        self._push_decision(exit_decision.to_dict())
                # Open (or overwrite same-side) position
                self._open_positions[symbol] = {
                    "direction": direction,
                    "entry_price": entry_price,
                    "timestamp": timestamp,
                    "size": self.position_size,
                }
            elif direction in (SignalDirection.EXIT_LONG, SignalDirection.EXIT_SHORT):
                existing = self._open_positions.get(symbol)
                if existing:
                    entry_dir: SignalDirection = existing["direction"]
                    if (entry_dir == SignalDirection.LONG and direction == SignalDirection.EXIT_LONG) or \
                       (entry_dir == SignalDirection.SHORT and direction == SignalDirection.EXIT_SHORT):
                        pos_size = float(existing.get("size", self.position_size))
                        e_price = float(existing.get("entry_price", entry_price))
                        if entry_dir == SignalDirection.LONG:
                            realized_pnl = (entry_price - e_price) * pos_size
                        else:
                            realized_pnl = (e_price - entry_price) * pos_size
                        self._open_positions.pop(symbol, None)
                        self._update_simulated_summary(realized_pnl)
                # Else: orphan EXIT -> logged with no realized PnL

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
                position_size=pos_size if pos_size is not None else (self.position_size if direction in (SignalDirection.LONG, SignalDirection.SHORT) else None),
            )

            decision_dict = decision.to_dict()
            self._push_decision(decision_dict)
            self._persist_open_positions()

            self._decisions_logged += 1
            self._last_decision_ts = timestamp

        except Exception as e:
            self.logger.error(f"Error handling decision payload: {e}")

    def _safe_mode(self) -> str:
        """Resolve mode safely with fallback."""
        try:
            mode = self.mode_resolver()
            if mode not in ("live", "public", "synthetic"):
                return "public"
            return mode
        except Exception:
            return "public"

    def _update_simulated_summary(self, realized_pnl: Optional[float]) -> None:
        """Update aggregated simulated PnL summary in Redis on each realized exit."""
        if realized_pnl is None:
            return
        key = "simulated_pnl_summary"
        try:
            summary = self.redis_manager.get_data(key) or {}
            total_pnl = float(summary.get("total_pnl", 0.0)) + realized_pnl
            total_trades = int(summary.get("total_trades", 0)) + 1
            wins = int(summary.get("wins", 0))
            losses = int(summary.get("losses", 0))
            if realized_pnl >= 0:
                wins += 1
            else:
                losses += 1

            peak_equity = float(summary.get("peak_equity", 0.0))
            if total_pnl > peak_equity:
                peak_equity = total_pnl
            max_drawdown_pct = float(summary.get("max_drawdown_pct", 0.0))
            if peak_equity > 0:
                current_dd = (peak_equity - total_pnl) / peak_equity * 100.0
                if current_dd > max_drawdown_pct:
                    max_drawdown_pct = current_dd

            win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0

            new_summary = {
                "total_pnl": total_pnl,
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "max_drawdown_pct": max_drawdown_pct,
                "peak_equity": peak_equity,
                "updated_ts": get_timestamp(),
            }
            self.redis_manager.set_data(key, new_summary, expire_seconds=7 * 24 * 3600)
        except Exception as e:
            self.logger.error(f"Failed updating simulated_pnl_summary: {e}")

    # ---- Helpers for persistence / logging ----
    def _persist_open_positions(self) -> None:
        """Persist current open positions to Redis for external components (unrealized PnL calc)."""
        try:
            serializable = {}
            for sym, pos in self._open_positions.items():
                serializable[sym] = {
                    "direction": pos["direction"].name if isinstance(pos["direction"], SignalDirection) else pos["direction"],
                    "entry_price": pos["entry_price"],
                    "timestamp": pos["timestamp"],
                    "size": pos.get("size", self.position_size),
                }
            self.redis_manager.set_data("open_positions", serializable, expire_seconds=3600)
        except Exception:
            pass

    def _push_decision(self, decision_dict: Dict[str, Any]) -> None:
        """Push a single decision record to Redis list with trimming & TTL."""
        try:
            pipe = self.redis_manager.redis_client.pipeline()
            pipe.lpush(self.list_key, json.dumps(decision_dict))
            pipe.ltrim(self.list_key, 0, self.max_entries - 1)
            pipe.expire(self.list_key, self.ttl_seconds)
            pipe.execute()
        except Exception as e:
            self.logger.error(f"Failed to push decision log: {e}")

    def get_health_status(self) -> Dict[str, Any]:
        """Return component health/status snapshot."""
        return {
            "is_running": self.is_running,
            "decisions_logged": self._decisions_logged,
            "last_decision_ts": self._last_decision_ts,
            "list_key": self.list_key,
            "max_entries": self.max_entries,
            "ttl_seconds": self.ttl_seconds,
        }
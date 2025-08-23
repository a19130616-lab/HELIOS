"""
Public Price Ingestor for Helios Trading System
Polls unauthenticated public REST endpoints (Binance Futures primary, Binance Spot fallback)
to supply market data (order book, latest trade, klines) when authenticated websocket
streaming is unavailable (e.g., no API keys). Normalizes outputs to the same Redis schema
used by DataIngestor so downstream components (SignalEngine, dashboard) work unchanged.

Modes:
- Primary: Binance USDT-M Futures REST (fapi)
- Fallback: Binance Spot REST (api) when futures endpoint fails for a symbol

Data Stored:
- orderbook:{SYMBOL}
- trade:{SYMBOL}
- kline:{SYMBOL} and klines:{SYMBOL} (list, last 200)

All timestamps in ms epoch.
"""

import threading
import time
import logging
import json
import requests
from typing import List, Dict, Any, Optional

from app.models import OrderBookSnapshot, OrderBookLevel, TradeData, OrderSide
from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config


class PublicPriceIngestor:
    """
    Poll-based public price ingestor (no authentication).
    Provides minimal market data sufficient for analytics & signal generation
    when trading (execution) is disabled.
    """

    def __init__(
        self,
        symbols: List[str],
        redis_manager: RedisManager,
        depth_limit: int = 20,
        poll_interval_sec: float = 1.0,
        kline_interval_sec: int = 60,
    ):
        self.symbols = symbols
        self.redis_manager = redis_manager
        self.depth_limit = depth_limit
        self.poll_interval_sec = poll_interval_sec
        self.kline_interval_sec = kline_interval_sec
        self.logger = logging.getLogger(__name__)
        self.config = get_config()

        self.is_running = False
        self.thread: Optional[threading.Thread] = None

        # In-memory caches
        self.order_books: Dict[str, OrderBookSnapshot] = {}
        self.latest_trades: Dict[str, TradeData] = {}
        self.kline_data: Dict[str, List[Dict[str, Any]]] = {}

        self.last_kline_fetch: Dict[str, float] = {}

        # Metrics
        self.request_failures = 0
        self.last_primary_success = 0
        self.last_fallback_success = 0

        # Endpoints
        self.base_primary = "https://fapi.binance.com"
        # Using spot as fallback (lower complexity vs CoinGecko symbol mapping)
        self.base_fallback = "https://api.binance.com"

    def start(self) -> None:
        if self.is_running:
            return
        self.logger.info("Starting PublicPriceIngestor (public mode)...")
        
        # Fetch historical data on startup to fill gaps
        self.logger.info("Fetching historical data to populate price trends...")
        self._fetch_historical_data()
        
        self.is_running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.logger.info("Stopping PublicPriceIngestor...")
        self.is_running = False

    def _fetch_historical_data(self) -> None:
        """
        Fetch historical klines on startup to populate price history.

        Enhanced logging added to aid diagnosis of empty Redis klines:* lists:
          - Per-symbol timing, counts, first/last kline timestamps
          - Redis list length verification after population
          - Aggregate summary at end
        """
        started_at = time.time()
        total_loaded = 0
        for symbol in self.symbols:
            sym_start = time.time()
            try:
                self.logger.info(f"[HIST] Fetching last 200 1m klines for {symbol} (futures primary, spot fallback)...")

                klines_json = self._fetch_with_fallback(
                    primary=lambda: self._get_json(
                        f"{self.base_primary}/fapi/v1/klines",
                        params={"symbol": symbol, "interval": "1m", "limit": 200},
                    ),
                    fallback=lambda: self._get_json(
                        f"{self.base_fallback}/api/v3/klines",
                        params={"symbol": symbol, "interval": "1m", "limit": 200},
                    ),
                    context=f"historical_klines:{symbol}",
                )

                if not klines_json or not isinstance(klines_json, list):
                    self.logger.warning(f"[HIST] No historical data returned for {symbol} (type={type(klines_json).__name__})")
                    continue

                count = len(klines_json)
                self.kline_data[symbol] = []
                first_ts = None
                last_ts = None

                for idx, kline_arr in enumerate(klines_json):
                    try:
                        ts_val = int(kline_arr[6]) if len(kline_arr) > 6 else int(kline_arr[0])
                        kline = {
                            "symbol": symbol,
                            "timestamp": ts_val,
                            "open": float(kline_arr[1]),
                            "high": float(kline_arr[2]),
                            "low": float(kline_arr[3]),
                            "close": float(kline_arr[4]),
                            "volume": float(kline_arr[5]),
                            "trades": int(kline_arr[8]) if len(kline_arr) > 8 else 0,
                        }
                        if first_ts is None or ts_val < first_ts:
                            first_ts = ts_val
                        if last_ts is None or ts_val > last_ts:
                            last_ts = ts_val
                        self.kline_data[symbol].append(kline)
                        self._store_kline(kline)

                        # Lightweight progress log every 50 inserts
                        if (idx + 1) % 50 == 0:
                            self.logger.debug(f"[HIST] {symbol} inserted {idx + 1}/{count} klines...")
                    except Exception as inner_e:
                        self.logger.warning(f"[HIST] Parse/store error {symbol} idx={idx}: {inner_e}")

                # Verify Redis list population
                list_key = f"klines:{symbol}"
                try:
                    redis_len = self.redis_manager.redis_client.llen(list_key)
                except Exception as rl_err:
                    redis_len = -1
                    self.logger.error(f"[HIST] Could not read Redis length for {list_key}: {rl_err}")

                span_min = ((last_ts - first_ts) / 60000.0) if (first_ts and last_ts) else 0
                self.logger.info(
                    f"[HIST] Loaded {len(self.kline_data[symbol])}/{count} klines for {symbol} "
                    f"(redis_len={redis_len}, span_min≈{span_min:.1f}, first_ts={first_ts}, last_ts={last_ts}, "
                    f"elapsed={time.time() - sym_start:.2f}s)"
                )
                self.last_kline_fetch[symbol] = time.time()
                total_loaded += len(self.kline_data[symbol])

            except Exception as e:
                self.logger.error(f"[HIST] Error fetching historical data for {symbol}: {e}")

        self.logger.info(
            f"[HIST] Historical backfill complete symbols={len(self.symbols)} total_klines={total_loaded} "
            f"duration={time.time() - started_at:.2f}s"
        )

    # ---------------- Main Loop ---------------- #

    def _run_loop(self) -> None:
        # Align kline fetch to minute boundary
        next_loop = time.time()
        while self.is_running:
            loop_start = time.time()
            try:
                for symbol in self.symbols:
                    self._poll_symbol(symbol)

                # Sleep granularity
                next_loop += self.poll_interval_sec
                sleep_for = max(0.0, next_loop - time.time())
                time.sleep(sleep_for)
            except Exception as e:
                self.logger.error(f"PublicPriceIngestor loop error: {e}")
                time.sleep(1.0)

    def _poll_symbol(self, symbol: str) -> None:
        # Depth + latest trade every loop, kline every kline_interval_sec
        now = time.time()
        # Depth
        depth_json = self._fetch_with_fallback(
            primary=lambda: self._get_json(
                f"{self.base_primary}/fapi/v1/depth",
                params={"symbol": symbol, "limit": self.depth_limit},
            ),
            fallback=lambda: self._get_json(
                f"{self.base_fallback}/api/v3/depth",
                params={"symbol": symbol, "limit": self.depth_limit},
            ),
            context=f"depth:{symbol}",
        )
        if depth_json:
            self._process_depth(symbol, depth_json)

        # Latest trade (use recent trades limit=1).
        trade_json_list = self._fetch_with_fallback(
            primary=lambda: self._get_json(
                f"{self.base_primary}/fapi/v1/trades", params={"symbol": symbol, "limit": 1}
            ),
            fallback=lambda: self._get_json(
                f"{self.base_fallback}/api/v3/trades", params={"symbol": symbol, "limit": 1}
            ),
            context=f"trade:{symbol}",
        )
        if trade_json_list and isinstance(trade_json_list, list) and trade_json_list:
            self._process_trade(symbol, trade_json_list[0])

        # Kline: every kline_interval_sec
        last_k = self.last_kline_fetch.get(symbol, 0)
        if now - last_k >= self.kline_interval_sec - 0.5:
            kline_json = self._fetch_with_fallback(
                primary=lambda: self._get_json(
                    f"{self.base_primary}/fapi/v1/klines",
                    params={"symbol": symbol, "interval": "1m", "limit": 1},
                ),
                fallback=lambda: self._get_json(
                    f"{self.base_fallback}/api/v3/klines",
                    params={"symbol": symbol, "interval": "1m", "limit": 1},
                ),
                context=f"kline:{symbol}",
            )
            if kline_json and isinstance(kline_json, list) and kline_json:
                self._process_kline(symbol, kline_json[0])
                self.last_kline_fetch[symbol] = now

    # ---------------- HTTP Helpers ---------------- #

    def _get_json(self, url: str, params: Dict[str, Any]) -> Optional[Any]:
        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code} body={resp.text[:120]}")
            return resp.json()
        except Exception as e:
            self.logger.warning(f"Request error {url}: {e}")
            return None

    def _fetch_with_fallback(self, primary, fallback, context: str) -> Optional[Any]:
        data = primary()
        if data is not None:
            self.last_primary_success = get_timestamp()
            return data
        self.request_failures += 1
        self.logger.debug(f"Primary failed for {context}, attempting fallback")
        data = fallback()
        if data is not None:
            self.last_fallback_success = get_timestamp()
            return data
        self.request_failures += 1
        self.logger.error(f"Both primary and fallback failed for {context}")
        return None

    # ---------------- Processing ---------------- #

    def _process_depth(self, symbol: str, payload: Dict[str, Any]) -> None:
        try:
            bids_raw = payload.get("bids") or payload.get("b", [])
            asks_raw = payload.get("asks") or payload.get("a", [])
            bids = [
                OrderBookLevel(price=float(b[0]), quantity=float(b[1]))
                for b in bids_raw
            ]
            asks = [
                OrderBookLevel(price=float(a[0]), quantity=float(a[1]))
                for a in asks_raw
            ]
            ob = OrderBookSnapshot(
                symbol=symbol,
                timestamp=get_timestamp(),
                bids=sorted(bids, key=lambda x: x.price, reverse=True),
                asks=sorted(asks, key=lambda x: x.price),
            )
            self.order_books[symbol] = ob
            self._store_order_book(ob)
        except Exception as e:
            self.logger.error(f"Depth process error {symbol}: {e}")

    def _process_trade(self, symbol: str, trade_obj: Dict[str, Any]) -> None:
        try:
            # Futures trade fields: id, price, qty, time, isBuyerMaker
            # Spot trades: id, price, qty, time, isBuyerMaker
            trade = TradeData(
                symbol=symbol,
                timestamp=int(trade_obj.get("time") or trade_obj.get("T") or get_timestamp()),
                price=float(trade_obj.get("price") or trade_obj.get("p", 0)),
                quantity=float(trade_obj.get("qty") or trade_obj.get("q", 0)),
                side=OrderSide.SELL if trade_obj.get("isBuyerMaker") else OrderSide.BUY,
                trade_id=str(trade_obj.get("id") or trade_obj.get("a") or ""),
            )
            self.latest_trades[symbol] = trade
            self._store_trade(trade)
        except Exception as e:
            self.logger.error(f"Trade process error {symbol}: {e}")

    def _process_kline(self, symbol: str, kline_arr: List[Any]) -> None:
        try:
            # Kline array format (spot/futures): [
            # 0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume,
            # 6 closeTime, 7 quoteAssetVolume, 8 trades, ...
            # ]
            kline = {
                "symbol": symbol,
                "timestamp": int(kline_arr[6]) if len(kline_arr) > 6 else get_timestamp(),
                "open": float(kline_arr[1]),
                "high": float(kline_arr[2]),
                "low": float(kline_arr[3]),
                "close": float(kline_arr[4]),
                "volume": float(kline_arr[5]),
                "trades": int(kline_arr[8]) if len(kline_arr) > 8 else 0,
            }
            if symbol not in self.kline_data:
                self.kline_data[symbol] = []
            self.kline_data[symbol].append(kline)
            if len(self.kline_data[symbol]) > 200:
                self.kline_data[symbol] = self.kline_data[symbol][-200:]
            self._store_kline(kline)
        except Exception as e:
            self.logger.error(f"Kline process error {symbol}: {e}")

    # ---------------- Redis Storage (mirrors DataIngestor) ---------------- #

    def _store_order_book(self, order_book: OrderBookSnapshot) -> None:
        key = f"orderbook:{order_book.symbol}"
        data = {
            "symbol": order_book.symbol,
            "timestamp": order_book.timestamp,
            "bids": [[lvl.price, lvl.quantity] for lvl in order_book.bids],
            "asks": [[lvl.price, lvl.quantity] for lvl in order_book.asks],
            "mid_price": order_book.get_mid_price(),
            "spread": order_book.get_spread(),
        }
        self.redis_manager.set_data(key, data, expiry=60)
        self.redis_manager.publish(
            "orderbook_updates",
            {
                "symbol": order_book.symbol,
                "timestamp": order_book.timestamp,
                "mid_price": order_book.get_mid_price(),
            },
        )

    def _store_trade(self, trade: TradeData) -> None:
        key = f"trade:{trade.symbol}"
        data = {
            "symbol": trade.symbol,
            "timestamp": trade.timestamp,
            "price": trade.price,
            "quantity": trade.quantity,
            "side": trade.side.value,
            "trade_id": trade.trade_id,
        }
        self.redis_manager.set_data(key, data, expiry=30)
        self.redis_manager.publish("trade_updates", data)

    def _store_kline(self, kline: Dict[str, Any]) -> None:
        key = f"kline:{kline['symbol']}"
        self.redis_manager.set_data(key, kline, expiry=300)
        list_key = f"klines:{kline['symbol']}"
        self.redis_manager.redis_client.lpush(list_key, json.dumps(kline))
        self.redis_manager.redis_client.ltrim(list_key, 0, 199)
        self.redis_manager.redis_client.expire(list_key, 3600)

    # ---------------- Accessors ---------------- #

    def get_latest_order_book(self, symbol: str) -> Optional[OrderBookSnapshot]:
        return self.order_books.get(symbol)

    def get_latest_trade(self, symbol: str) -> Optional[TradeData]:
        return self.latest_trades.get(symbol)

    def get_kline_history(self, symbol: str, count: int = 100) -> List[Dict[str, Any]]:
        klines = self.kline_data.get(symbol, [])
        return klines[-count:] if len(klines) > count else klines

    # ---------------- Health ---------------- #

    def get_health_status(self) -> Dict[str, Any]:
        return {
            "is_running": self.is_running,
            "symbols_monitored": len(self.symbols),
            "symbols": self.symbols,
            "request_failures": self.request_failures,
            "last_primary_success": self.last_primary_success,
            "last_fallback_success": self.last_fallback_success,
            "mode": "public",
        }
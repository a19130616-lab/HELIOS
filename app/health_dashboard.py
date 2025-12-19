"""
Health Dashboard for Helios Trading System
Provides a simple web interface for monitoring system health and status.
"""

import logging
import json
import time
import threading
import base64
from typing import Dict, Any, Optional
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config

class HealthDashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the health dashboard."""
    
    def __init__(self, *args, redis_manager=None, system_reference=None, **kwargs):
        self.redis_manager = redis_manager
        self.system_reference = system_reference
        super().__init__(*args, **kwargs)
    
    def _check_auth(self):
        """Check for Basic Authentication."""
        config = get_config()
        # Check if auth is enabled (default to false if not specified)
        auth_enabled_str = str(config.get('dashboard', 'enabled', fallback='false')).lower()
        auth_enabled = auth_enabled_str == 'true'
        
        if not auth_enabled:
            return True
            
        auth_header = self.headers.get('Authorization')
        if not auth_header:
            return False
            
        try:
            auth_type, auth_token = auth_header.split(' ', 1)
            if auth_type.lower() != 'basic':
                return False
                
            decoded = base64.b64decode(auth_token).decode('utf-8')
            username, password = decoded.split(':', 1)
            
            valid_user = config.get('dashboard', 'username', fallback='admin')
            valid_pass = config.get('dashboard', 'password', fallback='admin')
            
            return username == valid_user and password == valid_pass
        except Exception:
            return False

    def do_GET(self):
        """Handle GET requests."""
        if not self._check_auth():
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="Helios Dashboard"')
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'Authentication required')
            return

        try:
            parsed_path = urlparse(self.path)

            if parsed_path.path == '/':
                self._serve_dashboard()
            elif parsed_path.path == '/api/health':
                self._serve_health_api()
            elif parsed_path.path == '/api/metrics':
                self._serve_metrics_api()
            elif parsed_path.path == '/api/performance':
                self._serve_performance_api()
            elif parsed_path.path == '/api/prices':
                self._serve_prices_api()
            elif parsed_path.path == '/api/decisions':
                self._serve_decisions_api(parsed_path)
            elif parsed_path.path == '/api/candles':
                self._serve_candles_api(parsed_path)
            else:
                self._serve_404()

        except Exception as e:
            logging.error(f"Error handling request: {e}")
            self._serve_500()

    def do_POST(self):
        """Handle POST requests."""
        if not self._check_auth():
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="Helios Dashboard"')
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'Authentication required')
            return

        try:
            parsed_path = urlparse(self.path)

            if parsed_path.path == '/api/control':
                self._handle_control_api()
            else:
                self._serve_404()

        except Exception as e:
            logging.error(f"Error handling POST request: {e}")
            self._serve_500()

    def _handle_control_api(self):
        """Handle system control API (start/stop trading)."""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            action = data.get('action')
            
            if not self.system_reference:
                self.send_response(503)
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'System reference not available'}).encode())
                return

            if action == 'start':
                self.system_reference.start_trading()
                message = 'Trading started'
            elif action == 'stop':
                self.system_reference.stop_trading()
                message = 'Trading stopped'
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Invalid action'}).encode())
                return
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'success', 'message': message}).encode())
            
        except Exception as e:
            logging.error(f"Error in control API: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())
    
    def _serve_dashboard(self):
        """Serve the main dashboard HTML."""
        html_content = self._generate_dashboard_html()
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html_content.encode())
    
    def _serve_health_api(self):
        """Serve health status API."""
        try:
            health_data = self._get_system_health()
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(health_data, indent=2).encode())
            
        except Exception as e:
            logging.error(f"Error serving health API: {e}")
            self._serve_500()
    
    def _serve_metrics_api(self):
        """Serve metrics API."""
        try:
            metrics_data = self._get_system_metrics()
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(metrics_data, indent=2).encode())
            
        except Exception as e:
            logging.error(f"Error serving metrics API: {e}")
            self._serve_500()
    
    def _serve_performance_api(self):
        """Serve performance metrics API."""
        try:
            performance_data = self._get_performance_metrics()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(performance_data, indent=2).encode())

        except Exception as e:
            logging.error(f"Error serving performance API: {e}")
            self._serve_500()

    def _serve_prices_api(self):
        """Serve live prices for watchlist symbols."""
        try:
            prices_data = self._get_prices_data()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(prices_data, indent=2).encode())

        except Exception as e:
            logging.error(f"Error serving prices API: {e}")
            self._serve_500()

    def _serve_decisions_api(self, parsed_path):
        """Serve recent decision logs."""
        try:
            # Parse query params for limit
            qs = parse_qs(parsed_path.query)
            try:
                limit = int(qs.get('limit', ['50'])[0])
            except ValueError:
                limit = 50
            limit = max(1, min(limit, 200))

            decisions_data = self._get_decisions_data(limit)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(decisions_data, indent=2).encode())

        except Exception as e:
            logging.error(f"Error serving decisions API: {e}")
            self._serve_500()

    def _serve_candles_api(self, parsed_path):
        """Serve candlestick OHLC data with historical prices."""
        try:
            # Parse query params
            qs = parse_qs(parsed_path.query)
            symbol = qs.get('symbol', [''])[0].upper()
            try:
                interval_minutes = int(qs.get('interval', ['5'])[0])
                limit = int(qs.get('limit', ['100'])[0])
            except ValueError:
                interval_minutes = 5
                limit = 100
            limit = max(1, min(limit, 500))
            interval_minutes = max(1, min(interval_minutes, 60))

            candles_data = self._get_candles_data(symbol, interval_minutes, limit)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(candles_data, indent=2).encode())

        except Exception as e:
            logging.error(f"Error serving candles API: {e}")
            self._serve_500()

    def _serve_404(self):
        """Serve 404 Not Found."""
        self.send_response(404)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b'<h1>404 Not Found</h1>')
    
    def _serve_500(self):
        """Serve 500 Internal Server Error."""
        self.send_response(500)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b'<h1>500 Internal Server Error</h1>')
    
    def _get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health data."""
        try:
            health_data = {
                'timestamp': get_timestamp(),
                'system_status': 'unknown',
                'components': {},
                'uptime_seconds': 0
            }
            
            if self.system_reference:
                system_status = self.system_reference.get_system_status()
                health_data.update(system_status)
                health_data['system_status'] = 'running' if system_status.get('is_running') else 'stopped'
                # Include explicit testnet flag so the dashboard can indicate TESTNET vs PROD
                try:
                    # self.config is initialized in HealthDashboard.__init__
                    health_data['binance_testnet'] = bool(self.config.get('binance', 'testnet', fallback=False))
                except Exception:
                    # Fallback: try to read from system_reference config if available
                    try:
                        cfg = get_config()
                        health_data['binance_testnet'] = bool(cfg.get('binance', 'testnet', fallback=False))
                    except Exception:
                        health_data['binance_testnet'] = False
            
            return health_data
            
        except Exception as e:
            logging.error(f"Error getting system health: {e}")
            return {'error': str(e), 'timestamp': get_timestamp()}
    
    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics from Redis."""
        try:
            if not self.redis_manager:
                return {'error': 'Redis not available'}
            
            # Get metrics from monitoring agent
            metrics = self.redis_manager.get_data('performance_metrics')
            if not metrics:
                metrics = {'message': 'No metrics available yet'}
            
            return {
                'timestamp': get_timestamp(),
                'metrics': metrics
            }
            
        except Exception as e:
            logging.error(f"Error getting system metrics: {e}")
            return {'error': str(e), 'timestamp': get_timestamp()}
    
    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics with simulated fallback + unrealized PnL.

        Layers:
        1. Try live execution metrics (performance_metrics).
        2. If absent and mode in (public, synthetic) fallback to simulated_pnl_summary (realized PnL).
        3. Always augment with unrealized PnL from any open_positions (persisted by DecisionLogger).
           - open_positions key: { symbol: { direction, entry_price, size, timestamp } }
           - Current price resolution: trade:{SYMBOL} price -> orderbook:{SYMBOL} mid_price
        4. Return enriched schema:
           performance: {
             total_pnl (realized),
             unrealized_pnl,
             total_equity_pnl = total_pnl + unrealized_pnl,
             open_positions_count,
             open_positions: [ {symbol,direction,entry_price,current_price,size,unrealized_pnl,age_ms} ],
             ... existing fields ...
           }
        """
        try:
            if not self.redis_manager:
                return {'error': 'Redis not available'}

            # Determine current mode
            mode = 'unknown'
            try:
                if self.system_reference:
                    mode = getattr(self.system_reference, 'operational_mode', None) or 'unknown'
            except Exception:
                mode = 'unknown'

            # Base performance (live)
            performance = self.redis_manager.get_data('performance_metrics') or {}

            # Recent realized trades (if any)
            trade_keys = self.redis_manager.get_keys('trade_record:*')
            recent_trades = []
            for key in trade_keys[-10:]:
                trade_data = self.redis_manager.get_data(key)
                if trade_data:
                    recent_trades.append(trade_data)

            # Fallback to simulated realized summary if needed
            need_fallback = (
                (not performance or not performance.get('total_trades')) and
                mode in ('public', 'synthetic')
            )
            if need_fallback:
                sim_summary = self.redis_manager.get_data('simulated_pnl_summary')
                if sim_summary:
                    # DecisionLogger stores Net PnL in 'total_pnl'.
                    # We reconstruct Gross PnL for consistency with PerformanceTracker.
                    net_val = float(sim_summary.get('total_pnl', 0.0))
                    fees_val = float(sim_summary.get('total_fees', 0.0))
                    
                    performance = {
                        'total_pnl': net_val + fees_val,  # Gross PnL
                        'total_fees': fees_val,
                        'net_pnl': net_val,
                        'total_trades': sim_summary.get('total_trades', 0),
                        'win_rate': sim_summary.get('win_rate', 0.0),
                        'max_drawdown_pct': sim_summary.get('max_drawdown_pct', 0.0),
                        'simulated': True,
                        'source': 'simulated_pnl_summary',
                        'updated_ts': sim_summary.get('updated_ts'),
                        # Include futures trading parameters
                        'futures_mode': sim_summary.get('futures_mode', False),
                        'leverage': sim_summary.get('leverage', 1.0),
                        'daily_pnl': sim_summary.get('daily_pnl', 0.0),
                        'daily_loss_limit': sim_summary.get('daily_loss_limit', 0.0)
                    }
                    # No real trades list in simulated fallback
                    recent_trades = []

            # Augment with open positions (unrealized PnL)
            # NOTE: In LIVE mode, DecisionLogger's open_positions are simulated and can be misleading.
            # Only display them in public/synthetic modes unless explicitly enabled.
            show_simulated_positions = mode in ('public', 'synthetic')
            try:
                cfg = get_config()
                show_simulated_positions = bool(
                    show_simulated_positions or
                    cfg.get('dashboard', 'show_simulated_positions_in_live', bool, fallback=False)
                )
            except Exception:
                pass

            open_positions = (self.redis_manager.get_data('open_positions') or {}) if show_simulated_positions else {}
            unrealized_total = 0.0
            enriched_positions = []
            now = get_timestamp()

            for sym, pos in open_positions.items():
                direction = pos.get('direction')
                entry_price = float(pos.get('entry_price', 0.0) or 0.0)
                size = float(pos.get('size', 0.0) or 0.0)
                opened_ts = int(pos.get('timestamp', now))

                current_price = None
                sym_upper = sym.upper()
                trade_data = self.redis_manager.get_data(f"trade:{sym_upper}")
                if trade_data and 'price' in trade_data:
                    current_price = trade_data.get('price')
                    price_ts = trade_data.get('timestamp') or trade_data.get('ts')
                if current_price is None:
                    ob_data = self.redis_manager.get_data(f"orderbook:{sym_upper}")
                    if ob_data and ob_data.get('mid_price') is not None:
                        current_price = ob_data.get('mid_price')
                        price_ts = ob_data.get('timestamp') or ob_data.get('ts')

                if current_price is None:
                    continue  # cannot evaluate

                try:
                    cp = float(current_price)
                except Exception:
                    continue

                if direction == 'LONG':
                    unrealized = (cp - entry_price) * size
                elif direction == 'SHORT':
                    unrealized = (entry_price - cp) * size
                else:
                    unrealized = 0.0

                unrealized_total += unrealized
                age_ms = now - opened_ts
                enriched_positions.append({
                    'symbol': sym_upper,
                    'direction': direction,
                    'entry_price': entry_price,
                    'current_price': cp,
                    'size': size,
                    'unrealized_pnl': unrealized,
                    'age_ms': age_ms
                })

            realized_gross = float(performance.get('total_pnl', 0.0)) if performance else 0.0
            # Use net_pnl for equity calculation if available, otherwise fallback to gross (legacy behavior)
            realized_net = float(performance.get('net_pnl', realized_gross))
            
            performance.setdefault('total_pnl', realized_gross)
            performance['unrealized_pnl'] = unrealized_total
            performance['total_equity_pnl'] = realized_net + unrealized_total
            performance['open_positions_count'] = len(enriched_positions)
            performance['open_positions'] = enriched_positions
            # Derive simulated account value from starting balance (config) + equity PnL
            try:
                starting_balance = float(get_config().get('simulation', 'starting_balance', fallback='200'))
            except Exception:
                starting_balance = 200.0
            performance['starting_balance'] = starting_balance
            performance['account_value'] = starting_balance + performance.get('total_equity_pnl', 0.0)

            # Try to get real account info from Redis (published by RiskManager)
            real_account_info = self.redis_manager.get_data('real_account_info')
            if real_account_info:
                performance['account_value'] = float(real_account_info.get('total_balance', performance['account_value']))
                performance['available_balance'] = float(real_account_info.get('available_balance', 0.0))
                performance['used_margin'] = float(real_account_info.get('used_margin', 0.0))
                performance['margin_ratio'] = float(real_account_info.get('margin_ratio', 0.0))
                performance['is_real_balance'] = True
            else:
                performance['is_real_balance'] = False
            
            # Ensure futures parameters are included even if not in sim_summary
            if 'futures_mode' not in performance:
                # Try to get from simulated_pnl_summary if not already present
                sim_summary = self.redis_manager.get_data('simulated_pnl_summary')
                if sim_summary:
                    performance['futures_mode'] = sim_summary.get('futures_mode', False)
                    performance['leverage'] = sim_summary.get('leverage', 1.0)
                    performance['daily_pnl'] = sim_summary.get('daily_pnl', 0.0)
                    performance['daily_loss_limit'] = sim_summary.get('daily_loss_limit', 0.0)
            
            # Final fallback to config if still missing
            if 'futures_mode' not in performance:
                try:
                    cfg = get_config()
                    performance['futures_mode'] = cfg.get('simulation', 'futures_mode', bool, False)
                    performance['leverage'] = cfg.get('simulation', 'leverage', float, 1.0)
                    performance['daily_loss_limit'] = cfg.get('simulation', 'daily_loss_limit_pct', float, 5.0)
                except Exception:
                    pass

            # Add Trading Mode
            try:
                cfg = get_config()
                performance['trading_mode'] = cfg.get('simulation', 'mode', fallback='HF_MARKET')
            except Exception:
                performance['trading_mode'] = 'HF_MARKET'

            return {
                'timestamp': get_timestamp(),
                'performance': performance,
                'recent_trades': recent_trades
            }
            
        except Exception as e:
            logging.error(f"Error getting performance metrics: {e}")
            return {'error': str(e), 'timestamp': get_timestamp()}
    
    def _get_prices_data(self) -> Dict[str, Any]:
        """Assemble latest price data for watchlist symbols from Redis.
        
        Priority:
            1. Last trade price (trade:{SYMBOL})
            2. Order book mid price (orderbook:{SYMBOL})
        
        Adds:
            - age_ms: Milliseconds since price timestamp
            - stale: True if age exceeds threshold (currently 60s)
        """
        try:
            if not self.redis_manager:
                return {
                    'error': 'Redis not available',
                    'timestamp': get_timestamp(),
                    'prices': []
                }
            
            now = get_timestamp()
            STALE_THRESHOLD_MS = 60_000  # 60 seconds
            
            # Determine watchlist
            watchlist = []
            if self.system_reference:
                # Prefer direct attribute
                if hasattr(self.system_reference, 'watchlist'):
                    try:
                        watchlist = list(self.system_reference.watchlist)
                    except Exception:
                        watchlist = []
                else:
                    # Fallback via system status
                    try:
                        status = self.system_reference.get_system_status()
                        watchlist = status.get('watchlist', [])
                    except Exception:
                        watchlist = []
            
            prices = []
            for symbol in watchlist:
                symbol_upper = symbol.upper()
                trade_key = f"trade:{symbol_upper}"
                ob_key = f"orderbook:{symbol_upper}"
                
                price = None
                source = None
                ts = None
                
                # Try trade data first
                trade_data = self.redis_manager.get_data(trade_key)
                if trade_data and 'price' in trade_data:
                    price = trade_data.get('price')
                    ts = trade_data.get('timestamp') or trade_data.get('ts')
                    source = 'trade'
                
                # Fallback to order book mid price
                if price is None:
                    ob_data = self.redis_manager.get_data(ob_key)
                    if ob_data and ob_data.get('mid_price') is not None:
                        price = ob_data.get('mid_price')
                        ts = ob_data.get('timestamp') or ob_data.get('ts')
                        source = 'orderbook'
                
                if price is not None and ts is not None:
                    age_ms = now - ts
                    prices.append({
                        'symbol': symbol_upper,
                        'price': price,
                        'source': source,
                        'age_ms': age_ms,
                        'stale': age_ms > STALE_THRESHOLD_MS
                    })
            
            # Determine mode using centralized operational_mode if available to avoid heuristic drift
            mode = 'unknown'
            try:
                if self.system_reference:
                    mode = getattr(self.system_reference, 'operational_mode', None) or 'unknown'
            except Exception:
                mode = 'unknown'
            
            if mode == 'unknown':
                # Backward compatibility heuristic (will be removed after all components migrated)
                api_client = self.system_reference and getattr(self.system_reference, 'api_client', None)
                if api_client:
                    mode = 'live'
                elif self.system_reference and getattr(self.system_reference, 'public_price_ingestor', None):
                    mode = 'public'
                else:
                    mode = 'synthetic'

            return {
                'timestamp': now,
                'mode': mode,
                'prices': prices
            }
        
        except Exception as e:
            logging.error(f"Error building prices data: {e}")
            return {
                'error': str(e),
                'timestamp': get_timestamp(),
                'prices': []
            }

    def _get_decisions_data(self, limit: int) -> Dict[str, Any]:
        """Retrieve recent decision logs from Redis list.

        Args:
            limit: Maximum number of decision entries to return (already validated in caller)

        Returns:
            Dict containing timestamp, count and decisions list
        """
        try:
            if not self.redis_manager:
                return {
                    'timestamp': get_timestamp(),
                    'count': 0,
                    'decisions': [],
                    'error': 'Redis not available'
                }

            # Fetch list entries (most recent first)
            raw_entries = self.redis_manager.redis_client.lrange('decision_logs', 0, limit - 1)
            decisions = []
            for raw in raw_entries:
                try:
                    obj = json.loads(raw)
                    # Basic validation of expected fields
                    if 'symbol' in obj and 'direction' in obj and 'timestamp' in obj:
                        # In live mode, enrich with real execution status if available
                        sid = obj.get('signal_id')
                        status = None
                        if sid:
                            try:
                                status = self.redis_manager.get_data(f"signal_status:{sid}")
                            except Exception:
                                status = None

                        if isinstance(status, dict):
                            # Merge status fields (execution_status, error, order id, attempted qty/price, etc.)
                            for k, v in status.items():
                                if k not in obj or obj.get(k) in (None, '', 0):
                                    obj[k] = v

                            # Prefer execution-derived notional/capital if present
                            if status.get('position_value_usd') is not None:
                                obj['position_value_usd'] = status.get('position_value_usd')
                            if status.get('capital_used_usd') is not None:
                                obj['capital_used_usd'] = status.get('capital_used_usd')

                            # Make non-success execution explicit in the existing Reason column
                            exec_status = str(status.get('execution_status') or '').upper()
                            exec_err = status.get('execution_error')
                            if exec_status and exec_status not in ('ORDER_PLACED', 'FILLED'):
                                prefix = f"[{exec_status}]"
                                if exec_err:
                                    prefix = f"{prefix} {exec_err}"
                                base_reason = obj.get('reason') or ''
                                obj['reason'] = f"{prefix} {base_reason}".strip()
                        else:
                            # If this is a live decision and we have no status yet, show it as pending.
                            try:
                                if str(obj.get('mode') or '').lower() == 'live':
                                    base_reason = obj.get('reason') or ''
                                    obj['reason'] = f"[PENDING] {base_reason}".strip()
                            except Exception:
                                pass

                        decisions.append(obj)
                except Exception:
                    continue

            return {
                'timestamp': get_timestamp(),
                'count': len(decisions),
                'decisions': decisions
            }
        except Exception as e:
            logging.error(f"Error retrieving decisions data: {e}")
            return {
                'timestamp': get_timestamp(),
                'count': 0,
                'decisions': [],
                'error': str(e)
            }

    def _get_candles_data(self, symbol: str, interval_minutes: int, limit: int) -> Dict[str, Any]:
        """Get OHLC candle data for a symbol.
        
        This implementation:
        1. Fetches historical tick data from Redis (trade:{SYMBOL}_history)
        2. If no history, fetches from Binance public API for initial seed
        3. Aggregates ticks into OHLC candles based on interval
        4. Caches result in Redis for performance
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            interval_minutes: Candle interval in minutes
            limit: Maximum number of candles to return
            
        Returns:
            Dict with candles array and metadata
        """
        try:
            if not self.redis_manager or not symbol:
                return {
                    'symbol': symbol,
                    'interval_minutes': interval_minutes,
                    'candles': [],
                    'error': 'Invalid parameters'
                }

            now = get_timestamp()
            cache_key = f"candles:{symbol}:{interval_minutes}"
            
            # Try to get cached candles first
            cached = self.redis_manager.get_data(cache_key)
            if cached and cached.get('timestamp', 0) > now - 60000:  # 1 minute cache
                return cached

            # Get historical prices from multiple sources
            prices = []
            
            # 1. Try to get recent tick history from Redis
            history_key = f"price_history:{symbol}"
            tick_history = self.redis_manager.get_data(history_key) or []
            
            # 1b. If no tick history, reconstruct from stored 1m klines (PublicPriceIngestor)
            if not tick_history:
                try:
                    kline_raw_list = self.redis_manager.redis_client.lrange(f"klines:{symbol}", 0, 199)
                    if kline_raw_list:
                        reconstructed = []
                        # lpush stores newest first; reverse to chronological
                        for raw in reversed(kline_raw_list):
                            try:
                                k = json.loads(raw)
                                ts_val = int(k.get("timestamp") or k.get("ts") or 0)
                                close_val = float(k.get("close") or k.get("price") or 0)
                                reconstructed.append({
                                    'ts': ts_val,
                                    'price': close_val,
                                    'open': float(k.get('open', close_val)),
                                    'high': float(k.get('high', close_val)),
                                    'low': float(k.get('low', close_val)),
                                    'volume': float(k.get('volume', 0.0))
                                })
                            except Exception:
                                continue
                        if reconstructed:
                            tick_history = reconstructed
                            # Cache into price_history for quicker subsequent access
                            self.redis_manager.set_data(history_key, tick_history, expiry=3600)
                except Exception:
                    pass
            
            # 2. Get current price
            current_price = None
            trade_data = self.redis_manager.get_data(f"trade:{symbol}")
            if trade_data and 'price' in trade_data:
                current_price = float(trade_data['price'])
                current_ts = trade_data.get('timestamp', now)
                tick_history.append({'ts': current_ts, 'price': current_price})
            
            # 3. If we don't have enough history, fetch from Binance public API
            if len(tick_history) < 20:
                historical = self._fetch_binance_klines(symbol, interval_minutes, limit)
                if historical:
                    tick_history = historical + tick_history
            
            # 4. Store tick history for future use (rolling window of 1000 points)
            if tick_history:
                tick_history = tick_history[-1000:]  # Keep last 1000 ticks
                self.redis_manager.set_data(history_key, tick_history, expiry=3600)  # 1 hour TTL
            
            # 5. Aggregate ticks into OHLC candles
            candles = self._aggregate_to_candles(tick_history, interval_minutes, limit)
            
            result = {
                'symbol': symbol,
                'interval_minutes': interval_minutes,
                'candles': candles,
                'timestamp': now
            }
            
            # Cache the result
            if candles:
                self.redis_manager.set_data(cache_key, result, expiry=60)  # 1 minute cache
            
            return result
            
        except Exception as e:
            logging.error(f"Error getting candles data: {e}")
            return {
                'symbol': symbol,
                'interval_minutes': interval_minutes,
                'candles': [],
                'error': str(e)
            }

    def _fetch_binance_klines(self, symbol: str, interval_minutes: int, limit: int) -> list:
        """Fetch historical kline data from Binance public API.
        
        Args:
            symbol: Trading pair symbol
            interval_minutes: Interval in minutes
            limit: Number of klines to fetch
            
        Returns:
            List of price ticks [{ts, price}, ...]
        """
        try:
            import requests
            
            # Map interval to Binance format
            interval_map = {
                1: '1m', 3: '3m', 5: '5m', 15: '15m',
                30: '30m', 60: '1h'
            }
            interval = interval_map.get(interval_minutes, '5m')
            
            url = f"https://api.binance.com/api/v3/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': min(limit, 100)  # Binance limit
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                klines = response.json()
                # Convert to our format using close price
                ticks = []
                for kline in klines:
                    # Kline format: [open_time, open, high, low, close, volume, ...]
                    ticks.append({
                        'ts': int(kline[0]),  # open time
                        'price': float(kline[4]),  # close price
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'volume': float(kline[5])
                    })
                return ticks
            else:
                logging.warning(f"Failed to fetch Binance klines: {response.status_code}")
                return []
                
        except Exception as e:
            logging.error(f"Error fetching Binance klines: {e}")
            return []

    def _aggregate_to_candles(self, ticks: list, interval_minutes: int, limit: int) -> list:
        """Aggregate tick data into OHLC candles.
        
        Args:
            ticks: List of price ticks [{ts, price}, ...]
            interval_minutes: Candle interval in minutes
            limit: Maximum number of candles
            
        Returns:
            List of OHLC candles
        """
        try:
            if not ticks:
                return []
            
            # Sort ticks by timestamp
            sorted_ticks = sorted(ticks, key=lambda x: x['ts'])
            interval_ms = interval_minutes * 60 * 1000
            
            candles = []
            current_candle = None
            
            for tick in sorted_ticks:
                ts = tick['ts']
                price = float(tick.get('price', 0))
                
                # Check if this is from Binance klines (has OHLC already)
                if 'open' in tick and 'high' in tick and 'low' in tick:
                    # Use precomputed OHLC
                    candle_ts = (ts // interval_ms) * interval_ms
                    if not current_candle or current_candle['ts'] != candle_ts:
                        if current_candle:
                            candles.append(current_candle)
                        current_candle = {
                            'ts': candle_ts,
                            'open': float(tick['open']),
                            'high': float(tick['high']),
                            'low': float(tick['low']),
                            'close': price,
                            'volume': float(tick.get('volume', 0))
                        }
                    else:
                        # Merge with existing candle (shouldn't happen with klines)
                        current_candle['high'] = max(current_candle['high'], float(tick['high']))
                        current_candle['low'] = min(current_candle['low'], float(tick['low']))
                        current_candle['close'] = price
                        current_candle['volume'] += float(tick.get('volume', 0))
                else:
                    # Regular tick data - aggregate into candles
                    candle_ts = (ts // interval_ms) * interval_ms
                    
                    if not current_candle or current_candle['ts'] != candle_ts:
                        # Start new candle
                        if current_candle:
                            candles.append(current_candle)
                        current_candle = {
                            'ts': candle_ts,
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'volume': 0
                        }
                    else:
                        # Update current candle
                        current_candle['high'] = max(current_candle['high'], price)
                        current_candle['low'] = min(current_candle['low'], price)
                        current_candle['close'] = price
            
            # Don't forget the last candle
            if current_candle:
                candles.append(current_candle)
            
            # Return only the requested number of most recent candles
            return candles[-limit:] if candles else []
            
        except Exception as e:
            logging.error(f"Error aggregating candles: {e}")
            return []
    
    def _generate_dashboard_html(self) -> str:
        """Generate the dashboard HTML."""
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Helios Trading System - Health Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #0d1117;
            color: #c9d1d9;
        }
        .container {
            max-width: 1500px;
            margin: 0 auto;
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: linear-gradient(45deg, #1f2937, #374151);
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        }
        .header h1 {
            margin: 0;
            color: #58a6ff;
            font-size: 2.2em;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .status-card {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
            overflow: hidden;
            max-height: 340px;
            display: flex;
            flex-direction: column;
        }
        .status-card h3 {
            margin: 0 0 15px 0;
            color: #f0f6fc;
            border-bottom: 2px solid #21262d;
            padding-bottom: 10px;
            font-size: 1.1em;
        }
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
        .status-running { background-color: #28a745; }
        .status-stopped { background-color: #dc3545; }
        .status-warning { background-color: #ffc107; }
        .metric-row {
            display: flex;
            justify-content: space-between;
            margin: 6px 0;
            padding: 4px 0;
            border-bottom: 1px solid #21262d;
            font-size: 0.9em;
        }
        .metric-label {
            color: #8b949e;
        }
        .metric-value {
            font-weight: 600;
            color: #f0f6fc;
            font-family: monospace;
        }
        .control-btn {
            background: #0366d6;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 1em;
            font-weight: 600;
            margin-left: 10px;
            transition: background-color 0.2s;
        }
        .control-btn:hover {
            background: #0256b4;
        }
        .control-btn.stop {
            background: #d73a49;
        }
        .control-btn.stop:hover {
            background: #b31d28;
        }
        .control-btn:disabled {
            background: #6e7681;
            cursor: not-allowed;
        }
        .refresh-btn {
            background: #238636;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            margin: 10px 5px;
        }
        .refresh-btn:hover {
            background: #2ea043;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.75em;
        }
        th, td {
            padding: 4px 6px;
            text-align: left;
            border-bottom: 1px solid #21262d;
            white-space: nowrap;
        }
        th {
            background-color: #21262d;
            color: #f0f6fc;
            position: sticky;
            top: 0;
            z-index: 1;
        }
        .profit { color: #28a745; }
        .loss { color: #dc3545; }
        .stale { opacity: 0.55; }
        .auto-refresh {
            margin: 15px 0;
            text-align: center;
        }
        .price-up { color: #28a745; }
        .price-down { color: #dc3545; }
        .prices-table-wrapper, .decisions-table-wrapper {
            overflow-y: auto;
            flex: 1;
        }
        .prices-table-wrapper { max-height: 250px; }
        .decisions-table-wrapper { max-height: 250px; }
        .small {
            color: #8b949e;
            font-size: 0.75em;
            margin-left: 4px;
        }
        .badge {
            display: inline-block;
            margin-left: 10px;
            padding: 4px 8px;
            border-radius: 6px;
            font-size: 0.7em;
            font-weight: 600;
            letter-spacing: 0.5px;
            background: #6e7681;
            color: #fff;
        }
        .badge-live {
            background: #1f6feb;
        }
        .badge-public {
            background: #8957e5;
        }
        /* Synthetic mode badge (legacy .badge-demo removed) */
        .badge-synthetic {
            background: #6e7681;
        }
        .dir-long { color: #28a745; font-weight: 600; }
        .dir-short { color: #dc3545; font-weight: 600; }
        .conf-high { color: #58a6ff; }
        .conf-med { color: #c9d1d9; }
        .conf-low { color: #8b949e; }
        .decisions-card { grid-column: span 2; }
        .candle-canvas { background:#161b22; border:1px solid #30363d; border-radius:4px; }
        
        /* Trend boxes styling */
        #trendBoxes {
            display: flex;
            flex-direction: column;
            gap: 14px;
            margin-bottom: 40px;
        }
        .trend-box {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 12px 16px 16px 16px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.25);
            width: 100%;
        }
        .trend-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
            font-size: 0.9em;
            color: #c9d1d9;
        }
        .trend-header .symbol {
            font-weight: 600;
            font-size: 1.05em;
            color: #f0f6fc;
        }
        .trend-canvas {
            width: 100%;
            height: 150px;
            background: #0d1117;
            border: 1px solid #21262d;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Helios Trading System <span id="modeBadge" class="badge badge-public">PUBLIC</span></h1>
            <p>Real-time Health Dashboard</p>
            <div class="auto-refresh">
                <button class="refresh-btn" onclick="refreshAll()">🔄 Refresh All</button>
                <button class="refresh-btn" onclick="toggleAutoRefresh()" id="autoRefreshBtn">▶️ Auto Refresh</button>
                <button class="refresh-btn" onclick="resetTrends()">♻️ Reset Trends</button>
                <button id="controlBtn" class="control-btn" onclick="toggleTrading()">Loading...</button>
                <span id="lastUpdate"></span>
            </div>
        </div>

        <div class="status-grid">
            <div class="status-card">
                <h3>System Status</h3>
                <div id="systemStatus">Loading...</div>
            </div>
            
            <div class="status-card">
                <h3>Live Prices</h3>
                <div id="livePrices" class="prices-table-wrapper">Loading...</div>
            </div>

            <div class="status-card">
                <h3>Component Health</h3>
                <div id="componentHealth">Loading...</div>
            </div>
            
            <div class="status-card">
                <h3>Performance Metrics</h3>
                <div id="performanceMetrics">Loading...</div>
            </div>
            
            <div class="status-card">
                <h3>Recent Trades</h3>
                <div id="recentTrades">Loading...</div>
            </div>

            <div class="status-card">
                <h3>Alarms</h3>
                <div id="portfolioAlarms">Loading...</div>
            </div>

            <div class="status-card decisions-card">
                <h3>Decisions</h3>
                <div id="decisionsLog" class="decisions-table-wrapper">Loading...</div>
            </div>
        </div>
        <h2 style="margin:10px 0 15px 4px; color:#58a6ff; font-size:1.3em;">Price Trends</h2>
        <div id="trendBoxes"></div>
    </div>

    <script>
        let autoRefreshInterval = null;
        let autoRefreshEnabled = false;
        let lastPrices = {};

        async function fetchData(endpoint) {
            try {
                const response = await fetch(endpoint);
                return await response.json();
            } catch (error) {
                console.error('Error fetching data:', error);
                return { error: error.message };
            }
        }

        async function updateSystemStatus() {
            const data = await fetchData('/api/health');
            const element = document.getElementById('systemStatus');
            
            if (data.error) {
                element.innerHTML = `<div class="metric-row"><span class="metric-label">Error:</span><span class="metric-value">${data.error}</span></div>`;
                return;
            }

            const statusIcon = data.system_status === 'running' ?
                '<span class="status-indicator status-running"></span>' :
                '<span class="status-indicator status-stopped"></span>';
            
            const uptime = data.uptime_seconds ? (data.uptime_seconds / 60).toFixed(1) : 0;

            // Update control button
            const btn = document.getElementById('controlBtn');
            if (btn) {
                if (data.is_trading_active) {
                    btn.textContent = 'Stop Trading';
                    btn.className = 'control-btn stop';
                    btn.onclick = () => controlSystem('stop');
                } else {
                    btn.textContent = 'Start Trading';
                    btn.className = 'control-btn';
                    btn.onclick = () => controlSystem('start');
                }
                btn.disabled = false;
            }

            // Update the global mode badge (shows LIVE/TESTNET/PUBLIC/SYNTHETIC)
            const badge = document.getElementById('modeBadge');
            if (badge) {
                const op = (data.operational_mode || '').toLowerCase();
                const isTestnet = data.binance_testnet === true;
                let modeText = (op ? op.toUpperCase() : (data.mode ? data.mode.toUpperCase() : 'UNKNOWN'));

                if (op === 'live') {
                    modeText = isTestnet ? 'LIVE (TESTNET)' : 'LIVE (PROD)';
                } else if (op === 'public') {
                    modeText = 'PUBLIC (READ-ONLY)';
                } else if (op === 'synthetic') {
                    modeText = 'SYNTHETIC';
                }

                badge.textContent = modeText;
                badge.classList.remove('badge-live','badge-public','badge-synthetic');

                if (op === 'live') {
                    badge.classList.add('badge-live');
                } else if (op === 'public') {
                    badge.classList.add('badge-public');
                } else {
                    badge.classList.add('badge-synthetic');
                }
            }
            
            element.innerHTML = `
                <div class="metric-row">
                    <span class="metric-label">Status:</span>
                    <span class="metric-value">${statusIcon}${data.system_status}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Uptime:</span>
                    <span class="metric-value">${uptime} min</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Watchlist:</span>
                    <span class="metric-value">${data.watchlist ? data.watchlist.length : 0} symbols</span>
                </div>
            `;
            
            // If signal engine reports 10-minute metrics, show them compactly
            try {
                if (data.components && data.components.signal_engine) {
                    const se = data.components.signal_engine;
                    const tenMode = se.ten_min_mode ? 'ENABLED' : 'DISABLED';
                    element.innerHTML += `
                        <div class="metric-row">
                            <span class="metric-label">10m Mode:</span>
                            <span class="metric-value">${tenMode}</span>
                        </div>
                    `;
                    if (se.ten_min_metrics) {
                        // show small per-symbol confirmed_signals summary
                        let metricsHtml = '<div style="margin-top:8px">';
                        for (const [sym, m] of Object.entries(se.ten_min_metrics)) {
                            metricsHtml += `
                                <div class="metric-row">
                                    <span class="metric-label">${sym}:</span>
                                    <span class="metric-value">${m.confirmed_signals || 0} confirmed</span>
                                </div>
                            `;
                        }
                        metricsHtml += '</div>';
                        element.innerHTML += metricsHtml;
                    }
                }
            } catch (e) {
                console.error('Error showing 10m metrics', e);
            }
        }

        async function updateComponentHealth() {
            const data = await fetchData('/api/health');
            const element = document.getElementById('componentHealth');
            
            if (data.error || !data.components) {
                element.innerHTML = `<div class="metric-row"><span class="metric-label">Error:</span><span class="metric-value">No data</span></div>`;
                return;
            }

            let html = '';
            for (const [name, status] of Object.entries(data.components)) {
                const isRunning = status.is_running;
                const statusIcon = isRunning ?
                    '<span class="status-indicator status-running"></span>' :
                    '<span class="status-indicator status-stopped"></span>';
                
                html += `
                    <div class="metric-row">
                        <span class="metric-label">${name}:</span>
                        <span class="metric-value">${statusIcon}${isRunning ? 'Running' : 'Stopped'}</span>
                    </div>
                `;
            }
            
            element.innerHTML = html;
        }

        async function updatePerformanceMetrics() {
            const data = await fetchData('/api/performance');
            const element = document.getElementById('performanceMetrics');
            
            if (data.error || !data.performance) {
                element.innerHTML = `<div class="metric-row"><span class="metric-label">Error:</span><span class="metric-value">No data</span></div>`;
                return;
            }

            const perf = data.performance;
            const realized = perf.total_pnl || 0;
            const fees = perf.total_fees || 0;
            const netPnl = perf.net_pnl !== undefined ? perf.net_pnl : (realized - fees);
            const unrealized = perf.unrealized_pnl || 0;
            const equity = perf.total_equity_pnl !== undefined ? perf.total_equity_pnl : (netPnl + unrealized);
            const accountValue = perf.account_value !== undefined ? perf.account_value : ((perf.starting_balance || 10000) + equity);
            
            // Futures trading info
            const futuresMode = perf.futures_mode || false;
            const leverage = perf.leverage || 1.0;
            const dailyPnL = perf.daily_pnl || 0;
            const dailyLimit = perf.daily_loss_limit || 0;
            const tradingMode = perf.trading_mode || 'HF_MARKET';

            let html = '';
            
            // Show futures badge if enabled
            if (futuresMode) {
                html += `
                    <div class="metric-row" style="background:#1f6feb20; padding:6px; border-radius:4px; margin-bottom:8px;">
                        <span class="metric-label" style="color:#58a6ff;">⚡ FUTURES MODE</span>
                        <span class="metric-value" style="color:#58a6ff;">${leverage}x Leverage</span>
                    </div>
                `;
            }
            
            // Show Trading Mode
            html += `
                <div class="metric-row" style="background:#23863620; padding:6px; border-radius:4px; margin-bottom:8px;">
                    <span class="metric-label" style="color:#3fb950;">STRATEGY</span>
                    <span class="metric-value" style="color:#3fb950;">${tradingMode}</span>
                </div>
            `;
            
            html += `
                <div class="metric-row">
                    <span class="metric-label">Account Value:</span>
                    <span class="metric-value ${accountValue >= perf.starting_balance ? 'profit' : 'loss'}">$${accountValue.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Today's PnL:</span>
                    <span class="metric-value ${dailyPnL >= 0 ? 'profit' : 'loss'}">$${dailyPnL.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Total Fees:</span>
                    <span class="metric-value" style="color: #f85149;">-$${fees.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Net PnL:</span>
                    <span class="metric-value ${netPnl >= 0 ? 'profit' : 'loss'}">$${netPnl.toFixed(2)}</span>
                </div>
            `;
            
            // Show daily limit warning if close
            if (futuresMode && dailyLimit > 0 && dailyPnL < 0) {
                const limitPct = (dailyPnL / -dailyLimit) * 100;
                if (limitPct > 50) {
                    html += `
                        <div class="metric-row" style="background:#dc354520; padding:4px; border-radius:4px;">
                            <span class="metric-label" style="color:#f85149;">⚠️ Daily Limit:</span>
                            <span class="metric-value" style="color:#f85149;">${limitPct.toFixed(0)}% Used</span>
                        </div>
                    `;
                }
            }
            
            html += `
                <div class="metric-row">
                    <span class="metric-label">Total Trades:</span>
                    <span class="metric-value">${perf.total_trades || 0}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Win Rate:</span>
                    <span class="metric-value">${perf.win_rate ? (perf.win_rate).toFixed(1) + '%' : '0%'}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Gross Realized PnL:</span>
                    <span class="metric-value ${realized >= 0 ? 'profit' : 'loss'}">$${realized.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Unrealized PnL:</span>
                    <span class="metric-value ${unrealized >= 0 ? 'profit' : 'loss'}">$${unrealized.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Open Positions:</span>
                    <span class="metric-value">${perf.open_positions_count || 0}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Max Drawdown:</span>
                    <span class="metric-value">${perf.max_drawdown_pct ? perf.max_drawdown_pct.toFixed(2) + '%' : '0%'}</span>
                </div>
            `;
            
            element.innerHTML = html;
        }

        async function updateRecentTrades() {
            const data = await fetchData('/api/performance');
            const element = document.getElementById('recentTrades');
            
            if (data.error || !data.recent_trades || data.recent_trades.length === 0) {
                element.innerHTML = '<p>No recent trades</p>';
                return;
            }
    
            let tableHtml = `
                <table>
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Side</th>
                            <th>Gross PnL</th>
                            <th>Fee</th>
                            <th>Net PnL</th>
                            <th>Exit Reason</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
    
            data.recent_trades.slice(-5).forEach(trade => {
                const pnlClass = trade.pnl >= 0 ? 'profit' : 'loss';
                const netPnlVal = trade.net_pnl !== undefined ? trade.net_pnl : trade.pnl;
                const netPnlClass = netPnlVal >= 0 ? 'profit' : 'loss';
                const fee = trade.commission !== undefined ? `$${trade.commission.toFixed(4)}` : '-';
                const netPnl = trade.net_pnl !== undefined ? `$${trade.net_pnl.toFixed(2)}` : '-';

                tableHtml += `
                    <tr>
                        <td>${trade.symbol}</td>
                        <td>${trade.side}</td>
                        <td class="${pnlClass}">$${trade.pnl.toFixed(2)}</td>
                        <td>${fee}</td>
                        <td class="${netPnlClass}">${netPnl}</td>
                        <td>${trade.exit_reason}</td>
                    </tr>
                `;
            });
    
            tableHtml += '</tbody></table>';
            element.innerHTML = tableHtml;
        }
    
        // Render portfolio alarms (stop-loss / circuit-breaker events)
        async function updatePortfolioAlarms() {
            const perf = await fetchData('/api/performance');
            const element = document.getElementById('portfolioAlarms');
    
            if (perf.error || !perf.performance) {
                element.innerHTML = '<p>No alarms data</p>';
                return;
            }
    
            const alarms = perf.performance.alarms || [];
            if (!alarms || alarms.length === 0) {
                element.innerHTML = '<p>No recent alarms</p>';
                return;
            }
    
            let html = `<table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Symbol</th>
                        <th>Type</th>
                        <th>Price</th>
                        <th>Size</th>
                    </tr>
                </thead>
                <tbody>
            `;
    
            alarms.slice(0, 20).forEach(a => {
                const dt = new Date(a.timestamp);
                const t = dt.toLocaleTimeString();
                html += `
                    <tr>
                        <td>${t}</td>
                        <td>${a.symbol}</td>
                        <td>${a.type}</td>
                        <td>${a.price !== undefined ? a.price : '-'}</td>
                        <td>${a.size !== undefined ? a.size : '-'}</td>
                    </tr>
                `;
            });
    
            html += '</tbody></table>';
            element.innerHTML = html;
        }

        function formatPrice(p) {
            if (p >= 1000) return p.toFixed(2);
            if (p >= 1) return p.toFixed(3);
            return p.toFixed(6);
        }

        async function updateLivePrices() {
            const element = document.getElementById('livePrices');
            const data = await fetchData('/api/prices');

            if (data.error) {
                element.innerHTML = `<p>Error: ${data.error}</p>`;
                return;
            }

            // Update mode badge
            const badge = document.getElementById('modeBadge');
            if (badge && data.mode) {
                badge.textContent = data.mode.toUpperCase();
                badge.classList.remove('badge-live','badge-public','badge-synthetic');
                if (data.mode === 'live') {
                    badge.classList.add('badge-live');
                } else if (data.mode === 'public') {
                    badge.classList.add('badge-public');
                } else if (data.mode === 'synthetic') {
                    badge.classList.add('badge-synthetic');
                } else {
                    // Unknown -> treat as synthetic for neutral styling
                    badge.classList.add('badge-synthetic');
                }
            }

            if (!data.prices || data.prices.length === 0) {
                element.innerHTML = '<p>No price data</p>';
                return;
            }

            // Build table (chart column removed)
            let html = `<table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Price</th>
                        <th>Src</th>
                        <th>Age(s)</th>
                    </tr>
                </thead>
                <tbody>
            `;

            data.prices
                .sort((a,b) => a.symbol.localeCompare(b.symbol))
                .forEach(p => {
                    const prev = lastPrices[p.symbol];
                    let cls = '';
                    if (prev !== undefined) {
                        if (p.price > prev) cls = 'price-up';
                        else if (p.price < prev) cls = 'price-down';
                    }
                    let rowCls = p.stale ? 'stale' : '';
                    html += `
                        <tr class="${rowCls}">
                            <td>${p.symbol}</td>
                            <td class="${cls}">${formatPrice(p.price)}</td>
                            <td>${p.source}</td>
                            <td>${(p.age_ms/1000).toFixed(1)}${p.stale ? '*' : ''}</td>
                        </tr>
                    `;
                    lastPrices[p.symbol] = p.price;
                });

            html += '</tbody></table>';
            element.innerHTML = html;

            // Update trend series (replace prior inline mini-candles)
            const nowTs = data.timestamp;
            const symbols = data.prices.map(p => p.symbol).sort();
            buildTrendBoxes(symbols);
            data.prices.forEach(p => {
                const ts = nowTs - p.age_ms;
                updateTrendSeries(p.symbol, p.price, ts);
                drawTrend(p.symbol);
            });
        }

        function classifyConfidence(conf) {
            if (conf >= 0.75) return 'conf-high';
            if (conf >= 0.5) return 'conf-med';
            return 'conf-low';
        }

        async function updateDecisions() {
                const element = document.getElementById('decisionsLog');
                const data = await fetchData('/api/decisions?limit=50');
        
                if (data.error) {
                    element.innerHTML = `<p>Error: ${data.error}</p>`;
                    return;
                }
                if (!data.decisions || data.decisions.length === 0) {
                    element.innerHTML = '<p>No decisions logged</p>';
                    return;
                }
        
                let html = `<table>
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Sym</th>
                            <th>Dir</th>
                            <th>Conf%</th>
                            <th>NOBI</th>
                            <th>Notional</th>
                            <th>Capital</th>
                            <th>PnL</th>
                            <th>Fee</th>
                            <th>Mode</th>
                            <th>Reason</th>
                        </tr>
                    </thead>
                    <tbody>
                `;
        
                data.decisions.forEach(d => {
                    const dt = new Date(d.timestamp);
                    const t = dt.toLocaleTimeString();
                    const dir = d.direction;
                    let dirClass = '';
                    if (dir === 'LONG' || dir === 'EXIT_LONG') dirClass = 'dir-long';
                    else if (dir === 'SHORT' || dir === 'EXIT_SHORT') dirClass = 'dir-short';
                    const confPct = (d.confidence * 100).toFixed(1);
                    const confClass = classifyConfidence(d.confidence);
                    const pnl = d.realized_pnl !== null && d.realized_pnl !== undefined ? d.realized_pnl : '';
                    const pnlClass = pnl !== '' ? (pnl >= 0 ? 'profit' : 'loss') : '';
                    const dirDisplay = (dir.startsWith('EXIT_') ? 'EXIT' : dir);
                    const notional = d.position_value_usd ? ('$' + Number(d.position_value_usd).toFixed(2)) : '';
                    const capital = d.capital_used_usd ? ('$' + Number(d.capital_used_usd).toFixed(2)) : '';
                    const fee = d.commission !== null && d.commission !== undefined ? '$' + Number(d.commission).toFixed(4) : '';

                    html += `
                        <tr>
                            <td>${t}</td>
                            <td>${d.symbol}</td>
                            <td class="${dirClass}">${dirDisplay}</td>
                            <td class="${confClass}">${confPct}</td>
                            <td>${(d.nobi_value ?? 0).toFixed(3)}</td>
                            <td>${notional}</td>
                            <td>${capital}</td>
                            <td class="${pnlClass}">${pnl !== '' ? ('$' + pnl.toFixed(2)) : ''}</td>
                            <td>${fee}</td>
                            <td>${d.mode}</td>
                            <td title="${d.reason || ''}">${d.reason ? d.reason.substring(0,18) : ''}</td>
                        </tr>
                    `;
                });
        
                html += '</tbody></table>';
                element.innerHTML = html;
            }

        function toggleTrading() {
            // This function is just a placeholder, the actual action is set by updateSystemStatus
            // But initially it might be called before status update
            const btn = document.getElementById('controlBtn');
            if (btn.textContent.includes('Start')) {
                controlSystem('start');
            } else {
                controlSystem('stop');
            }
        }

        async function controlSystem(action) {
            const btn = document.getElementById('controlBtn');
            const originalText = btn.textContent;
            btn.disabled = true;
            btn.textContent = action === 'start' ? 'Starting...' : 'Stopping...';
            
            try {
                const response = await fetch('/api/control', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ action: action })
                });
                
                const result = await response.json();
                if (result.status === 'success') {
                    // Wait a bit for system to update
                    setTimeout(refreshAll, 1000);
                } else {
                    alert('Error: ' + result.error);
                    btn.disabled = false;
                    btn.textContent = originalText;
                }
            } catch (e) {
                alert('Error: ' + e.message);
                btn.disabled = false;
                btn.textContent = originalText;
            }
        }

        async function refreshAll() {
            // Optional first-load trend reset (keeps initial historical sequence clean)
            if (firstTrendReset) {
                const holder = document.getElementById('trendBoxes');
                if (holder) holder.innerHTML = '';
                for (const k in candleData) { delete candleData[k]; }
                firstTrendReset = false;
            }
            await Promise.all([
                updateSystemStatus(),
                updateLivePrices(),
                updateComponentHealth(),
                updatePerformanceMetrics(),
                updateRecentTrades(),
                updatePortfolioAlarms(),
                updateDecisions()
            ]);
            
            document.getElementById('lastUpdate').textContent =
                `Last updated: ${new Date().toLocaleTimeString()}`;
        }

        function toggleAutoRefresh() {
            const btn = document.getElementById('autoRefreshBtn');
            
            if (autoRefreshEnabled) {
                clearInterval(autoRefreshInterval);
                autoRefreshEnabled = false;
                btn.textContent = '▶️ Auto Refresh';
                btn.style.backgroundColor = '#238636';
            } else {
                autoRefreshInterval = setInterval(refreshAll, 5000); // 5 seconds
                autoRefreshEnabled = true;
                btn.textContent = '⏸️ Stop Auto';
                btn.style.backgroundColor = '#dc3545';
            }
        }

        // --- Candlestick charts (full-width per symbol) ---
        const candleData = {}; // symbol -> candles array from API
        const candleInterval = 5; // 5-minute candles by default
        let firstTrendReset = true;

        function buildTrendBoxes(symbols) {
            const holder = document.getElementById('trendBoxes');
            if (!holder) return;
            const existing = new Set(Array.from(holder.querySelectorAll('.trend-box')).map(e => e.dataset.symbol));
            symbols.forEach(sym => {
                if (!existing.has(sym)) {
                    const div = document.createElement('div');
                    div.className = 'trend-box';
                    div.dataset.symbol = sym;
                    div.innerHTML = `
                        <div class="trend-header">
                            <span class="symbol">${sym}</span>
                            <span id="lastPrice-${sym}" class="price">-</span>
                        </div>
                        <canvas id="trend-${sym}" class="trend-canvas"></canvas>
                    `;
                    holder.appendChild(div);
                    // Fetch initial candle data
                    fetchCandles(sym);
                }
            });
        }

        function resetTrends() {
            const holder = document.getElementById('trendBoxes');
            if (holder) holder.innerHTML = '';
            for (const k in candleData) { delete candleData[k]; }
            firstTrendReset = false;
            // Re-fetch and rebuild via live prices
            updateLivePrices();
        }

        async function fetchCandles(symbol) {
            try {
                const response = await fetch(`/api/candles?symbol=${symbol}&interval=${candleInterval}&limit=50`);
                const data = await response.json();
                if (data.candles && data.candles.length > 0) {
                    candleData[symbol] = data.candles;
                    drawCandlestickChart(symbol);
                }
            } catch (error) {
                console.error(`Error fetching candles for ${symbol}:`, error);
            }
        }

        function updateTrendSeries(symbol, price, ts) {
            // Update the last price display
            const lp = document.getElementById(`lastPrice-${symbol}`);
            if (lp) lp.textContent = formatPrice(Number(price));
            
            // Update the latest candle or create a new one
            if (!candleData[symbol]) {
                // If no candle data yet, fetch it
                fetchCandles(symbol);
                return;
            }
            
            const intervalMs = candleInterval * 60 * 1000;
            const candleTs = Math.floor(ts / intervalMs) * intervalMs;
            const candles = candleData[symbol];
            
            if (candles.length > 0) {
                const lastCandle = candles[candles.length - 1];
                if (lastCandle.ts === candleTs) {
                    // Update existing candle
                    lastCandle.high = Math.max(lastCandle.high, price);
                    lastCandle.low = Math.min(lastCandle.low, price);
                    lastCandle.close = price;
                } else {
                    // Start new candle
                    candles.push({
                        ts: candleTs,
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        volume: 0
                    });
                    // Keep only last 50 candles
                    if (candles.length > 50) {
                        candles.shift();
                    }
                }
                drawCandlestickChart(symbol);
            }
        }

        function drawTrend(symbol) {
            // This is now called drawCandlestickChart
            drawCandlestickChart(symbol);
        }

        function drawCandlestickChart(symbol) {
            const candles = candleData[symbol];
            if (!candles || candles.length === 0) return;
            
            const canvas = document.getElementById(`trend-${symbol}`);
            if (!canvas) return;
            const ctx = canvas.getContext('2d');

            const rect = canvas.getBoundingClientRect();
            canvas.width = rect.width * window.devicePixelRatio;
            canvas.height = rect.height * window.devicePixelRatio;
            ctx.scale(window.devicePixelRatio, window.devicePixelRatio);
            ctx.clearRect(0, 0, rect.width, rect.height);

            const w = rect.width;
            const h = rect.height;
            const leftPadding = 65; // More space for Y-axis labels
            const rightPadding = 20;
            const topPadding = 20;
            const bottomPadding = 30; // More space for X-axis labels
            const chartW = w - leftPadding - rightPadding;
            const chartH = h - topPadding - bottomPadding;

            // Find min/max prices
            let minPrice = Infinity;
            let maxPrice = -Infinity;
            candles.forEach(c => {
                minPrice = Math.min(minPrice, c.low);
                maxPrice = Math.max(maxPrice, c.high);
            });
            // Add 1% padding to price range
            const priceMargin = (maxPrice - minPrice) * 0.01 || minPrice * 0.001;
            minPrice -= priceMargin;
            maxPrice += priceMargin;
            const priceRange = maxPrice - minPrice || 1;

            // Calculate candle width
            const candleWidth = Math.max(1, (chartW / candles.length) * 0.8);
            const candleSpacing = chartW / candles.length;

            // Draw background
            ctx.fillStyle = '#0d1117';
            ctx.fillRect(leftPadding, topPadding, chartW, chartH);

            // Draw grid lines and Y-axis labels
            ctx.strokeStyle = '#21262d';
            ctx.lineWidth = 0.5;
            ctx.setLineDash([2, 2]);
            ctx.font = '11px monospace';
            ctx.fillStyle = '#8b949e';
            
            const gridLines = 5;
            for (let i = 0; i <= gridLines; i++) {
                const y = topPadding + (i / gridLines) * chartH;
                const price = maxPrice - (i / gridLines) * priceRange;
                
                // Grid line
                ctx.beginPath();
                ctx.moveTo(leftPadding, y);
                ctx.lineTo(w - rightPadding, y);
                ctx.stroke();
                
                // Y-axis label
                ctx.fillText(formatPrice(price), 5, y + 3);
            }
            
            // Highlight highest and lowest prices
            ctx.font = 'bold 11px monospace';
            ctx.fillStyle = '#58a6ff';
            ctx.fillText('H: ' + formatPrice(maxPrice - priceMargin), 5, topPadding - 5);
            ctx.fillStyle = '#f85149';
            ctx.fillText('L: ' + formatPrice(minPrice + priceMargin), 5, h - bottomPadding + 15);
            
            ctx.setLineDash([]);

            // Draw X-axis time labels
            ctx.font = '10px monospace';
            ctx.fillStyle = '#8b949e';
            const timeLabels = Math.min(6, candles.length); // Show max 6 time labels
            const labelInterval = Math.max(1, Math.floor(candles.length / timeLabels));
            
            for (let i = 0; i < candles.length; i += labelInterval) {
                const candle = candles[i];
                const x = leftPadding + i * candleSpacing + candleSpacing / 2;
                const date = new Date(candle.ts);
                const timeStr = date.getHours().toString().padStart(2, '0') + ':' +
                               date.getMinutes().toString().padStart(2, '0');
                
                ctx.save();
                ctx.translate(x, h - 5);
                ctx.rotate(-Math.PI / 4); // Rotate 45 degrees
                ctx.fillText(timeStr, 0, 0);
                ctx.restore();
            }

            // Draw candles
            candles.forEach((candle, i) => {
                const x = leftPadding + i * candleSpacing + candleSpacing / 2;
                const openY = topPadding + (1 - (candle.open - minPrice) / priceRange) * chartH;
                const closeY = topPadding + (1 - (candle.close - minPrice) / priceRange) * chartH;
                const highY = topPadding + (1 - (candle.high - minPrice) / priceRange) * chartH;
                const lowY = topPadding + (1 - (candle.low - minPrice) / priceRange) * chartH;

                const isGreen = candle.close >= candle.open;
                const color = isGreen ? '#2ea043' : '#f85149';

                // Draw wick (high-low line)
                ctx.strokeStyle = color;
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(x, highY);
                ctx.lineTo(x, lowY);
                ctx.stroke();

                // Draw body
                ctx.fillStyle = color;
                ctx.globalAlpha = isGreen ? 0.8 : 1.0;
                const bodyTop = Math.min(openY, closeY);
                const bodyHeight = Math.abs(openY - closeY) || 1;
                ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
                ctx.globalAlpha = 1.0;

                // Draw body outline
                ctx.strokeStyle = color;
                ctx.lineWidth = 1;
                ctx.strokeRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
            });

            // Draw current price line
            const lastCandle = candles[candles.length - 1];
            const currentY = topPadding + (1 - (lastCandle.close - minPrice) / priceRange) * chartH;
            ctx.strokeStyle = '#58a6ff';
            ctx.lineWidth = 1.5;
            ctx.setLineDash([4, 2]);
            ctx.beginPath();
            ctx.moveTo(leftPadding, currentY);
            ctx.lineTo(w - rightPadding, currentY);
            ctx.stroke();
            ctx.setLineDash([]);

            // Draw current price label with background
            const priceText = formatPrice(lastCandle.close);
            ctx.font = 'bold 11px monospace';
            const textWidth = ctx.measureText(priceText).width;
            
            // Background for price label
            ctx.fillStyle = '#58a6ff';
            ctx.fillRect(w - rightPadding - textWidth - 10, currentY - 8, textWidth + 8, 16);
            
            // Price text
            ctx.fillStyle = '#ffffff';
            ctx.fillText(priceText, w - rightPadding - textWidth - 6, currentY + 3);
            
            // Draw chart title with last update time
            const lastDate = new Date(candles[candles.length - 1].ts);
            const updateStr = 'Last: ' + lastDate.toLocaleTimeString();
            ctx.font = '10px sans-serif';
            ctx.fillStyle = '#8b949e';
            ctx.fillText(updateStr, leftPadding, 12);
        }

        // Periodic candle refetch (every 60s) to ensure alignment with backend state
        // Covers: missed updates, browser throttling (background tabs), backend restarts repopulating Redis
        let candleRefetchTimer = null;
        function startCandleRefetch() {
            if (candleRefetchTimer) return;
            candleRefetchTimer = setInterval(() => {
                const symbols = Object.keys(candleData);
                if (!symbols || symbols.length === 0) return;
                symbols.forEach(sym => {
                    fetchCandles(sym); // Re-fetch authoritative 5m aggregation (limit=50)
                });
            }, 60000);
        }

        // Initial load + start periodic refetch
        refreshAll();
        startCandleRefetch();
    </script>
</body>
</html>
        """

class HealthDashboard:
    """
    Web-based health dashboard for monitoring system status.
    Provides real-time visibility into system health and performance.
    """
    
    def __init__(self, redis_manager: RedisManager, system_reference=None, port: int = 8080):
        """
        Initialize the health dashboard.
        
        Args:
            redis_manager: Redis manager instance
            system_reference: Reference to main system for health checks
            port: Port to run the dashboard on
        """
        self.redis_manager = redis_manager
        self.system_reference = system_reference
        self.port = port
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Server state
        self.server = None
        self.server_thread = None
        self.is_running = False
    
    def start(self) -> None:
        """Start the health dashboard server."""
        try:
            self.logger.info(f"Starting Health Dashboard on port {self.port}...")
            
            # Create custom handler class with dependencies
            def handler_factory(*args, **kwargs):
                return HealthDashboardHandler(
                    *args, 
                    redis_manager=self.redis_manager,
                    system_reference=self.system_reference,
                    **kwargs
                )
            
            # Create and start server
            # Use ThreadingHTTPServer to handle multiple concurrent requests (prevents blocking)
            self.server = ThreadingHTTPServer(('0.0.0.0', self.port), handler_factory)
            self.server_thread = threading.Thread(target=self._run_server, daemon=True)
            self.server_thread.start()
            
            self.is_running = True
            self.logger.info(f"Health Dashboard started - Access at http://localhost:{self.port}")
            
        except Exception as e:
            self.logger.error(f"Failed to start Health Dashboard: {e}")
            raise
    
    def stop(self) -> None:
        """Stop the health dashboard server."""
        try:
            if self.server:
                self.logger.info("Stopping Health Dashboard...")
                self.server.shutdown()
                self.server.server_close()
                
                if self.server_thread and self.server_thread.is_alive():
                    self.server_thread.join(timeout=5)
                
                self.is_running = False
                self.logger.info("Health Dashboard stopped")
                
        except Exception as e:
            self.logger.error(f"Error stopping Health Dashboard: {e}")
    
    def _run_server(self) -> None:
        """Run the HTTP server."""
        try:
            self.server.serve_forever()
        except Exception as e:
            if self.is_running:  # Only log if not a shutdown
                self.logger.error(f"Health Dashboard server error: {e}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the dashboard.
        
        Returns:
            Health status information
        """
        return {
            'is_running': self.is_running,
            'port': self.port,
            'server_active': self.server is not None,
            'thread_alive': self.server_thread.is_alive() if self.server_thread else False
        }
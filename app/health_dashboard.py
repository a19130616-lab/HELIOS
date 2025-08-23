"""
Health Dashboard for Helios Trading System
Provides a simple web interface for monitoring system health and status.
"""

import logging
import json
import time
import threading
from typing import Dict, Any, Optional
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config

class HealthDashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the health dashboard."""
    
    def __init__(self, *args, redis_manager=None, system_reference=None, **kwargs):
        self.redis_manager = redis_manager
        self.system_reference = system_reference
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
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
            else:
                self._serve_404()

        except Exception as e:
            logging.error(f"Error handling request: {e}")
            self._serve_500()
    
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
                    performance = {
                        'total_pnl': sim_summary.get('total_pnl', 0.0),
                        'total_trades': sim_summary.get('total_trades', 0),
                        'win_rate': sim_summary.get('win_rate', 0.0),
                        'max_drawdown_pct': sim_summary.get('max_drawdown_pct', 0.0),
                        'simulated': True,
                        'source': 'simulated_pnl_summary',
                        'updated_ts': sim_summary.get('updated_ts')
                    }
                    # No real trades list in simulated fallback
                    recent_trades = []

            # Augment with open positions (unrealized PnL)
            open_positions = self.redis_manager.get_data('open_positions') or {}
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

            realized = float(performance.get('total_pnl', 0.0)) if performance else 0.0
            performance.setdefault('total_pnl', realized)
            performance['unrealized_pnl'] = unrealized_total
            performance['total_equity_pnl'] = realized + unrealized_total
            performance['open_positions_count'] = len(enriched_positions)
            performance['open_positions'] = enriched_positions

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
                <h3>Decisions</h3>
                <div id="decisionsLog" class="decisions-table-wrapper">Loading...</div>
            </div>
        </div>
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
            const unrealized = perf.unrealized_pnl || 0;
            const equity = perf.total_equity_pnl !== undefined ? perf.total_equity_pnl : (realized + unrealized);

            element.innerHTML = `
                <div class="metric-row">
                    <span class="metric-label">Total Trades:</span>
                    <span class="metric-value">${perf.total_trades || 0}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Win Rate:</span>
                    <span class="metric-value">${perf.win_rate ? (perf.win_rate).toFixed(1) + '%' : '0%'}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Realized PnL:</span>
                    <span class="metric-value ${realized >= 0 ? 'profit' : 'loss'}">$${realized.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Unrealized PnL:</span>
                    <span class="metric-value ${unrealized >= 0 ? 'profit' : 'loss'}">$${unrealized.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Equity PnL:</span>
                    <span class="metric-value ${equity >= 0 ? 'profit' : 'loss'}">$${equity.toFixed(2)}</span>
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
                            <th>PnL</th>
                            <th>Exit Reason</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            data.recent_trades.slice(-5).forEach(trade => {
                const pnlClass = trade.pnl >= 0 ? 'profit' : 'loss';
                tableHtml += `
                    <tr>
                        <td>${trade.symbol}</td>
                        <td>${trade.side}</td>
                        <td class="${pnlClass}">$${trade.pnl.toFixed(2)}</td>
                        <td>${trade.exit_reason}</td>
                    </tr>
                `;
            });

            tableHtml += '</tbody></table>';
            element.innerHTML = tableHtml;
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

            // Build table
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
                        <th>PnL</th>
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
                html += `
                    <tr>
                        <td>${t}</td>
                        <td>${d.symbol}</td>
                        <td class="${dirClass}">${dirDisplay}</td>
                        <td class="${confClass}">${confPct}</td>
                        <td>${(d.nobi_value ?? 0).toFixed(3)}</td>
                        <td class="${pnlClass}">${pnl !== '' ? ('$' + pnl.toFixed(2)) : ''}</td>
                        <td>${d.mode}</td>
                        <td title="${d.reason || ''}">${d.reason ? d.reason.substring(0,18) : ''}</td>
                    </tr>
                `;
            });

            html += '</tbody></table>';
            element.innerHTML = html;
        }

        async function refreshAll() {
            await Promise.all([
                updateSystemStatus(),
                updateLivePrices(),
                updateComponentHealth(),
                updatePerformanceMetrics(),
                updateRecentTrades(),
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

        // Initial load
        refreshAll();
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
            self.server = HTTPServer(('0.0.0.0', self.port), handler_factory)
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
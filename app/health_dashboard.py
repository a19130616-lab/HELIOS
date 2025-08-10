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
        """Get performance metrics."""
        try:
            if not self.redis_manager:
                return {'error': 'Redis not available'}
            
            # Get performance data
            performance = self.redis_manager.get_data('performance_metrics')
            
            # Get recent trades
            trade_keys = self.redis_manager.get_keys('trade_record:*')
            recent_trades = []
            
            for key in trade_keys[-10:]:  # Last 10 trades
                trade_data = self.redis_manager.get_data(key)
                if trade_data:
                    recent_trades.append(trade_data)
            
            return {
                'timestamp': get_timestamp(),
                'performance': performance or {},
                'recent_trades': recent_trades
            }
            
        except Exception as e:
            logging.error(f"Error getting performance metrics: {e}")
            return {'error': str(e), 'timestamp': get_timestamp()}
    
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
            max-width: 1200px;
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
            font-size: 2.5em;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .status-card {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
        }
        .status-card h3 {
            margin: 0 0 15px 0;
            color: #f0f6fc;
            border-bottom: 2px solid #21262d;
            padding-bottom: 10px;
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
            margin: 8px 0;
            padding: 5px 0;
            border-bottom: 1px solid #21262d;
        }
        .metric-label {
            color: #8b949e;
        }
        .metric-value {
            font-weight: bold;
            color: #f0f6fc;
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
        .trades-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }
        .trades-table th,
        .trades-table td {
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #21262d;
        }
        .trades-table th {
            background-color: #21262d;
            color: #f0f6fc;
        }
        .profit { color: #28a745; }
        .loss { color: #dc3545; }
        .auto-refresh {
            margin: 20px 0;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Helios Trading System</h1>
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
        </div>
    </div>

    <script>
        let autoRefreshInterval = null;
        let autoRefreshEnabled = false;

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
                    <span class="metric-value">${uptime} minutes</span>
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
                    <span class="metric-label">Total PnL:</span>
                    <span class="metric-value ${perf.total_pnl >= 0 ? 'profit' : 'loss'}">$${perf.total_pnl ? perf.total_pnl.toFixed(2) : '0.00'}</span>
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
                <table class="trades-table">
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

        async function refreshAll() {
            await Promise.all([
                updateSystemStatus(),
                updateComponentHealth(),
                updatePerformanceMetrics(),
                updateRecentTrades()
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
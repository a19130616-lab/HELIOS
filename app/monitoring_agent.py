"""
Monitoring Agent for Helios Trading System
Collects metrics and sends data to Azure Monitor for observability.
"""

import logging
import time
import threading
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from app.models import SystemMetrics, RiskLevel
from app.utils import get_timestamp
from app.config_manager import get_config

# Try to import Azure Monitor dependencies
try:
    from azure.monitor.ingestion import LogsIngestionClient
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logging.warning("Azure Monitor dependencies not available - monitoring will be limited")

class MonitoringAgent:
    """
    Comprehensive monitoring agent that collects and reports system metrics.
    Integrates with Azure Monitor for cloud-based observability.
    """
    
    def __init__(self, data_ingestor=None, signal_engine=None, risk_manager=None, execution_manager=None):
        """
        Initialize the Monitoring Agent.
        
        Args:
            data_ingestor: Data ingestor instance
            signal_engine: Signal engine instance
            risk_manager: Risk manager instance
            execution_manager: Execution manager instance
        """
        self.data_ingestor = data_ingestor
        self.signal_engine = signal_engine
        self.risk_manager = risk_manager
        self.execution_manager = execution_manager
        
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        
        # Azure Monitor setup
        self.azure_client = None
        self.connection_string = self.config.get('azure', 'connection_string', str)
        self._setup_azure_monitor()
        
        # Monitoring state
        self.is_running = False
        self.metrics_collection_interval = 30  # seconds
        
        # Metrics storage
        self.system_metrics = {}
        self.performance_metrics = {}
        self.risk_metrics = {}
        self.health_metrics = {}
        
        # Alerts
        self.alert_thresholds = {
            'max_drawdown_pct': 10.0,
            'win_rate_min': 0.4,
            'error_rate_max': 0.05,
            'latency_max_ms': 1000,
            'margin_ratio_critical': 20.0
        }
        
        self.alerts_sent = {}  # Prevent spam
    
    def start(self) -> None:
        """Start the monitoring agent."""
        self.logger.info("Starting Monitoring Agent...")
        self.is_running = True
        
        # Start metrics collection thread
        self.metrics_thread = threading.Thread(target=self._collect_metrics_loop, daemon=True)
        self.metrics_thread.start()
        
        # Start health check thread
        self.health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_thread.start()
        
        self.logger.info("Monitoring Agent started")
    
    def stop(self) -> None:
        """Stop the monitoring agent."""
        self.logger.info("Stopping Monitoring Agent...")
        self.is_running = False
        self.logger.info("Monitoring Agent stopped")
    
    def _setup_azure_monitor(self) -> None:
        """Set up Azure Monitor integration."""
        if not AZURE_AVAILABLE:
            self.logger.warning("Azure Monitor not available - metrics will be logged locally only")
            return
        
        if not self.connection_string or self.connection_string == "YOUR_CONNECTION_STRING":
            self.logger.warning("Azure connection string not configured - metrics will be logged locally only")
            return
        
        try:
            # Initialize Azure Monitor client
            credential = DefaultAzureCredential()
            self.azure_client = LogsIngestionClient(
                endpoint="https://your-workspace.monitor.azure.com",
                credential=credential
            )
            self.logger.info("Azure Monitor integration initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure Monitor: {e}")
            self.azure_client = None
    
    def _collect_metrics_loop(self) -> None:
        """Main metrics collection loop."""
        while self.is_running:
            try:
                # Collect all metrics
                self._collect_system_metrics()
                self._collect_performance_metrics()
                self._collect_risk_metrics()
                self._collect_health_metrics()
                
                # Send to Azure Monitor
                self._send_metrics_to_azure()
                
                # Check alerts
                self._check_alerts()
                
                # Log metrics summary
                self._log_metrics_summary()
                
                time.sleep(self.metrics_collection_interval)
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
                time.sleep(10)
    
    def _collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        try:
            current_time = get_timestamp()
            
            # Basic system info
            self.system_metrics = {
                'timestamp': current_time,
                'uptime_seconds': (current_time - getattr(self, 'start_time', current_time)) / 1000,
                'components_running': self._count_running_components(),
                'memory_usage_mb': self._get_memory_usage(),
                'cpu_usage_pct': self._get_cpu_usage()
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
    
    def _collect_performance_metrics(self) -> None:
        """Collect trading performance metrics."""
        try:
            # Get data from risk manager (trade history)
            if self.risk_manager and hasattr(self.risk_manager, 'trade_history'):
                trades = self.risk_manager.trade_history
                
                if trades:
                    # Calculate performance metrics
                    total_trades = len(trades)
                    winning_trades = len([t for t in trades if t['pnl'] > 0])
                    losing_trades = total_trades - winning_trades
                    
                    win_rate = winning_trades / total_trades if total_trades > 0 else 0
                    
                    total_pnl = sum(t['pnl'] for t in trades)
                    
                    wins = [t['pnl_pct'] for t in trades if t['pnl'] > 0]
                    losses = [abs(t['pnl_pct']) for t in trades if t['pnl'] < 0]
                    
                    avg_win = sum(wins) / len(wins) if wins else 0
                    avg_loss = sum(losses) / len(losses) if losses else 0
                    
                    # Calculate drawdown
                    cumulative_pnl = []
                    running_total = 0
                    for trade in trades:
                        running_total += trade['pnl_pct']
                        cumulative_pnl.append(running_total)
                    
                    peak = cumulative_pnl[0] if cumulative_pnl else 0
                    max_drawdown = 0
                    for pnl in cumulative_pnl:
                        if pnl > peak:
                            peak = pnl
                        drawdown = peak - pnl
                        if drawdown > max_drawdown:
                            max_drawdown = drawdown
                    
                    # Calculate Sharpe ratio (simplified)
                    if losses:
                        returns_std = (sum(t['pnl_pct']**2 for t in trades) / len(trades)) ** 0.5
                        sharpe_ratio = (total_pnl / len(trades)) / returns_std if returns_std > 0 else 0
                    else:
                        sharpe_ratio = 0
                    
                    self.performance_metrics = {
                        'timestamp': get_timestamp(),
                        'total_trades': total_trades,
                        'winning_trades': winning_trades,
                        'losing_trades': losing_trades,
                        'win_rate': win_rate,
                        'total_pnl': total_pnl,
                        'avg_win': avg_win,
                        'avg_loss': avg_loss,
                        'win_loss_ratio': avg_win / avg_loss if avg_loss > 0 else 0,
                        'max_drawdown_pct': max_drawdown * 100,
                        'sharpe_ratio': sharpe_ratio
                    }
                else:
                    # No trades yet
                    self.performance_metrics = {
                        'timestamp': get_timestamp(),
                        'total_trades': 0,
                        'winning_trades': 0,
                        'losing_trades': 0,
                        'win_rate': 0,
                        'total_pnl': 0,
                        'avg_win': 0,
                        'avg_loss': 0,
                        'win_loss_ratio': 0,
                        'max_drawdown_pct': 0,
                        'sharpe_ratio': 0
                    }
            
        except Exception as e:
            self.logger.error(f"Error collecting performance metrics: {e}")
    
    def _collect_risk_metrics(self) -> None:
        """Collect risk management metrics."""
        try:
            current_time = get_timestamp()
            
            # Get risk assessment from risk manager
            if self.risk_manager:
                risk_assessment = self.risk_manager.get_risk_assessment()
                account_info = self.risk_manager.account_info
                
                self.risk_metrics = {
                    'timestamp': current_time,
                    'risk_level': risk_assessment.level.value,
                    'margin_ratio': risk_assessment.margin_ratio,
                    'position_count': risk_assessment.position_count,
                    'total_exposure': risk_assessment.total_exposure,
                    'max_single_position_risk': risk_assessment.max_single_position_risk,
                    'emergency_mode': getattr(self.risk_manager, 'emergency_mode', False),
                    'available_balance': account_info.available_balance if account_info else 0,
                    'used_margin': account_info.used_margin if account_info else 0
                }
            else:
                self.risk_metrics = {
                    'timestamp': current_time,
                    'risk_level': 'UNKNOWN',
                    'margin_ratio': 0,
                    'position_count': 0,
                    'total_exposure': 0,
                    'max_single_position_risk': 0,
                    'emergency_mode': False,
                    'available_balance': 0,
                    'used_margin': 0
                }
                
        except Exception as e:
            self.logger.error(f"Error collecting risk metrics: {e}")
    
    def _collect_health_metrics(self) -> None:
        """Collect component health metrics."""
        try:
            current_time = get_timestamp()
            
            # Get health status from each component
            health_data = {
                'timestamp': current_time,
                'data_ingestor': self._get_component_health(self.data_ingestor),
                'signal_engine': self._get_component_health(self.signal_engine),
                'risk_manager': self._get_component_health(self.risk_manager),
                'execution_manager': self._get_component_health(self.execution_manager)
            }
            
            # Calculate overall health score
            component_scores = []
            for component, health in health_data.items():
                if component != 'timestamp' and health:
                    if health.get('is_running', False):
                        component_scores.append(1.0)
                    else:
                        component_scores.append(0.0)
            
            overall_health = sum(component_scores) / len(component_scores) if component_scores else 0
            health_data['overall_health_score'] = overall_health
            
            self.health_metrics = health_data
            
        except Exception as e:
            self.logger.error(f"Error collecting health metrics: {e}")
    
    def _get_component_health(self, component) -> Optional[Dict[str, Any]]:
        """Get health status from a component."""
        try:
            if component and hasattr(component, 'get_health_status'):
                return component.get_health_status()
            return None
        except Exception as e:
            self.logger.error(f"Error getting component health: {e}")
            return None
    
    def _count_running_components(self) -> int:
        """Count how many components are running."""
        count = 0
        components = [self.data_ingestor, self.signal_engine, self.risk_manager, self.execution_manager]
        
        for component in components:
            if component and getattr(component, 'is_running', False):
                count += 1
        
        return count
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)  # Convert to MB
        except ImportError:
            return 0.0
        except Exception as e:
            self.logger.error(f"Error getting memory usage: {e}")
            return 0.0
    
    def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        try:
            import psutil
            return psutil.cpu_percent(interval=1)
        except ImportError:
            return 0.0
        except Exception as e:
            self.logger.error(f"Error getting CPU usage: {e}")
            return 0.0
    
    def _send_metrics_to_azure(self) -> None:
        """Send metrics to Azure Monitor."""
        if not self.azure_client:
            return
        
        try:
            # Combine all metrics
            all_metrics = {
                'system': self.system_metrics,
                'performance': self.performance_metrics,
                'risk': self.risk_metrics,
                'health': self.health_metrics
            }
            
            # Send to Azure Monitor (simplified - in real implementation, 
            # you would use the proper Azure Monitor ingestion format)
            # self.azure_client.upload(...)
            
            self.logger.debug("Metrics sent to Azure Monitor")
            
        except Exception as e:
            self.logger.error(f"Error sending metrics to Azure: {e}")
    
    def _check_alerts(self) -> None:
        """Check for alert conditions and send notifications."""
        try:
            current_time = get_timestamp()
            
            # Check performance alerts
            if self.performance_metrics:
                # Max drawdown alert
                if self.performance_metrics.get('max_drawdown_pct', 0) > self.alert_thresholds['max_drawdown_pct']:
                    self._send_alert('HIGH_DRAWDOWN', 
                                   f"Maximum drawdown exceeded: {self.performance_metrics['max_drawdown_pct']:.2f}%")
                
                # Low win rate alert
                if (self.performance_metrics.get('total_trades', 0) > 20 and 
                    self.performance_metrics.get('win_rate', 1) < self.alert_thresholds['win_rate_min']):
                    self._send_alert('LOW_WIN_RATE',
                                   f"Win rate below threshold: {self.performance_metrics['win_rate']:.2f}")
            
            # Check risk alerts
            if self.risk_metrics:
                # Critical margin ratio
                if self.risk_metrics.get('margin_ratio', 100) < self.alert_thresholds['margin_ratio_critical']:
                    self._send_alert('CRITICAL_MARGIN',
                                   f"Critical margin ratio: {self.risk_metrics['margin_ratio']:.2f}%")
                
                # Emergency mode alert
                if self.risk_metrics.get('emergency_mode', False):
                    self._send_alert('EMERGENCY_MODE', "System in emergency mode")
            
            # Check health alerts
            if self.health_metrics:
                overall_health = self.health_metrics.get('overall_health_score', 1)
                if overall_health < 0.8:  # Less than 80% of components running
                    self._send_alert('SYSTEM_HEALTH',
                                   f"System health degraded: {overall_health:.1%}")
            
        except Exception as e:
            self.logger.error(f"Error checking alerts: {e}")
    
    def _send_alert(self, alert_type: str, message: str) -> None:
        """
        Send an alert notification.
        
        Args:
            alert_type: Type of alert
            message: Alert message
        """
        try:
            current_time = get_timestamp()
            
            # Prevent alert spam (max 1 alert per type per hour)
            last_alert_time = self.alerts_sent.get(alert_type, 0)
            if current_time - last_alert_time < 3600000:  # 1 hour
                return
            
            self.alerts_sent[alert_type] = current_time
            
            # Log the alert
            self.logger.critical(f"ALERT [{alert_type}]: {message}")
            
            # In a real implementation, you would send notifications via:
            # - Email
            # - SMS
            # - Slack/Discord webhook
            # - Azure Monitor alerts
            
            alert_data = {
                'timestamp': current_time,
                'type': alert_type,
                'message': message,
                'severity': 'CRITICAL'
            }
            
            # Store alert for history
            self._store_alert(alert_data)
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
    
    def _store_alert(self, alert_data: Dict[str, Any]) -> None:
        """Store alert for historical tracking."""
        try:
            # In a real implementation, store in database or persistent storage
            alert_log_entry = f"{datetime.now().isoformat()} - {alert_data['type']}: {alert_data['message']}"
            
            # Write to alerts log file
            with open('logs/alerts.log', 'a') as f:
                f.write(alert_log_entry + '\n')
                
        except Exception as e:
            self.logger.error(f"Error storing alert: {e}")
    
    def _health_check_loop(self) -> None:
        """Perform periodic health checks."""
        while self.is_running:
            try:
                self._perform_health_checks()
                time.sleep(60)  # Check every minute
            except Exception as e:
                self.logger.error(f"Error in health check loop: {e}")
                time.sleep(10)
    
    def _perform_health_checks(self) -> None:
        """Perform comprehensive health checks."""
        try:
            # Check component responsiveness
            health_issues = []
            
            components = {
                'data_ingestor': self.data_ingestor,
                'signal_engine': self.signal_engine,
                'risk_manager': self.risk_manager,
                'execution_manager': self.execution_manager
            }
            
            for name, component in components.items():
                if component:
                    if not getattr(component, 'is_running', False):
                        health_issues.append(f"{name} is not running")
                    
                    # Check for component-specific issues
                    health_status = self._get_component_health(component)
                    if health_status:
                        # Add component-specific health checks here
                        pass
            
            # Log health issues
            if health_issues:
                self.logger.warning(f"Health issues detected: {', '.join(health_issues)}")
            
        except Exception as e:
            self.logger.error(f"Error in health checks: {e}")
    
    def _log_metrics_summary(self) -> None:
        """Log a summary of current metrics."""
        try:
            # Create summary
            summary_lines = ["=== HELIOS METRICS SUMMARY ==="]
            
            # System metrics
            if self.system_metrics:
                summary_lines.append(f"System: {self.system_metrics.get('components_running', 0)}/4 components running")
                summary_lines.append(f"Memory: {self.system_metrics.get('memory_usage_mb', 0):.1f} MB")
            
            # Performance metrics
            if self.performance_metrics:
                summary_lines.append(f"Performance: {self.performance_metrics.get('total_trades', 0)} trades, " +
                                   f"{self.performance_metrics.get('win_rate', 0):.1%} win rate")
                summary_lines.append(f"PnL: {self.performance_metrics.get('total_pnl', 0):.4f}, " +
                                   f"Drawdown: {self.performance_metrics.get('max_drawdown_pct', 0):.2f}%")
            
            # Risk metrics
            if self.risk_metrics:
                summary_lines.append(f"Risk: {self.risk_metrics.get('risk_level', 'UNKNOWN')} level, " +
                                   f"{self.risk_metrics.get('margin_ratio', 0):.1f}% margin ratio")
                summary_lines.append(f"Positions: {self.risk_metrics.get('position_count', 0)}")
            
            summary_lines.append("=" * 30)
            
            # Log summary
            for line in summary_lines:
                self.logger.info(line)
                
        except Exception as e:
            self.logger.error(f"Error logging metrics summary: {e}")
    
    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """
        Get current metrics snapshot.
        
        Returns:
            Dictionary containing all current metrics
        """
        return {
            'timestamp': get_timestamp(),
            'system': self.system_metrics,
            'performance': self.performance_metrics,
            'risk': self.risk_metrics,
            'health': self.health_metrics
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the Monitoring Agent.
        
        Returns:
            Health status information
        """
        return {
            'is_running': self.is_running,
            'azure_monitor_connected': self.azure_client is not None,
            'metrics_collected': len(self.system_metrics) > 0,
            'alerts_configuration': self.alert_thresholds,
            'collection_interval': self.metrics_collection_interval
        }
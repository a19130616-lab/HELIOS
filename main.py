"""
Main entry point for the Helios High-Frequency Trading System.
Orchestrates all components and manages the system lifecycle.
"""

import os
import sys
import logging
import signal
import time
import threading
import json
from typing import Dict, Any, Optional

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from binance.um_futures import UMFutures
from app.config_manager import init_config, get_config
from app.utils import RedisManager, setup_logging
from app.data_ingestor import DataIngestor
from app.public_price_ingestor import PublicPriceIngestor
from app.signal_engine import SignalEngine
from app.risk_manager import RiskManager
from app.execution_manager import ExecutionManager
from app.monitoring_agent import MonitoringAgent
from app.reddit_ingestor import RedditIngestor
from app.performance_tracker import PerformanceTracker
from app.health_dashboard import HealthDashboard
from app.decision_logger import DecisionLogger

class HeliosSystem:
    """
    Main Helios Trading System orchestrator.
    Manages the lifecycle of all components and provides system-level controls.
    """
    
    def __init__(self):
        """Initialize the Helios system."""
        self.logger = None
        self.config = None
        self.is_running = False
        self.start_time = None
        
        # System components
        self.redis_manager = None
        self.api_client = None
        self.data_ingestor = None
        self.public_price_ingestor = None
        self.signal_engine = None
        self.risk_manager = None
        self.execution_manager = None
        self.reddit_ingestor = None
        self.monitoring_agent = None
        self.performance_tracker = None
        self.decision_logger = None
        self.health_dashboard = None
        
        # Resolved operational mode: 'live' | 'public' | 'synthetic'
        self.operational_mode: Optional[str] = None
        
        # Watchlist management
        self.watchlist = []
        self.watchlist_thread = None
        
        # System state
        self.shutdown_event = threading.Event()
        
    def initialize(self) -> bool:
        """
        Initialize the system configuration and components.
        
        Returns:
            True if initialization successful
        """
        try:
            # Initialize configuration
            self.config = init_config()
            
            # Create logs directory
            os.makedirs('logs', exist_ok=True)
            
            # Set up logging
            log_level = self.config.get('system', 'log_level', str)
            setup_logging(log_level)
            self.logger = logging.getLogger(__name__)
            
            self.logger.info("Starting Helios High-Frequency Trading System")
            self.logger.info("=" * 50)
            
            # Initialize Redis connection
            self._initialize_redis()
            
            # Initialize Binance API client
            self._initialize_api_client()
            
            # Initialize watchlist
            self._initialize_watchlist()
            
            # Initialize components
            self._initialize_components()
            
            self.logger.info("System initialization completed successfully")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"System initialization failed: {e}")
            else:
                print(f"System initialization failed: {e}")
            return False
    
    def _initialize_redis(self) -> None:
        """Initialize Redis connection."""
        try:
            redis_host = self.config.get('system', 'redis_host', str)
            redis_port = self.config.get('system', 'redis_port', int)
            redis_db = self.config.get('system', 'redis_db', int)
            
            self.redis_manager = RedisManager(redis_host, redis_port, redis_db)
            self.logger.info("Redis connection established")
            
            # Optional: clear persisted Redis state on startup to avoid inheriting
            # previous run metrics/positions during development or CI.
            try:
                clear_flag_env = os.getenv('CLEAR_REDIS_ON_STARTUP')
                clear_flag_cfg = None
                try:
                    # Config uses string -> fallback to 'false' if not present
                    clear_flag_cfg = str(self.config.get('system', 'clear_redis_on_startup', fallback='false'))
                except Exception:
                    clear_flag_cfg = 'false'
                clear_flag = (str(clear_flag_env).lower() in ("1","true","yes","on")) or (str(clear_flag_cfg).lower() in ("1","true","yes","on"))
                if clear_flag:
                    self.logger.info("CLEAR_REDIS_ON_STARTUP enabled - flushing Redis database to reset state")
                    try:
                        self.redis_manager.redis_client.flushdb()
                        self.logger.info("Redis database flushed successfully")
                    except Exception as e:
                        self.logger.error(f"Failed to flush Redis database: {e}")
            except Exception:
                # Non-fatal: don't block startup if clearing check fails
                pass
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis: {e}")
            raise
    
    def _initialize_api_client(self) -> None:
        """Initialize Binance API client.

        Hardened logic:
        - Case-insensitive / pattern-based placeholder detection
        - No legacy demo auto-fallback; missing/invalid credentials -> public or synthetic (if enabled) else abort
        - Explicit operational modes: live | public | synthetic (ALLOW_PUBLIC_MODE / ALLOW_SYNTHETIC gates)
        - DEMO_MODE env var deprecated and ignored (warning emitted)
        """
        try:
            api_key = self.config.get('binance', 'api_key', str)
            api_secret = self.config.get('binance', 'api_secret', str)
            testnet = self.config.get('binance', 'testnet', bool)

            # Environment overrides (prefer env over config for secrets)
            env_api_key = os.getenv('BINANCE_API_KEY')
            env_api_secret = os.getenv('BINANCE_API_SECRET')
            env_testnet = os.getenv('BINANCE_TESTNET')

            if env_api_key:
                api_key = env_api_key.strip()
                self.logger.info("Using BINANCE_API_KEY from environment (config file value overridden)")
            if env_api_secret:
                api_secret = env_api_secret.strip()
                self.logger.info("Using BINANCE_API_SECRET from environment (config file value overridden)")
            if env_testnet:
                testnet = str(env_testnet).lower() in ("1", "true", "yes", "on")
                self.logger.info(f"BINANCE_TESTNET override detected -> testnet={testnet}")

            # DEMO_MODE env is deprecated; ignore but note
            if os.getenv('DEMO_MODE'):
                self.logger.warning("DEMO_MODE env variable is deprecated; system uses live/testnet or public/synthetic modes")
            
            def is_placeholder(value: Optional[str]) -> bool:
                if not value:
                    return True
                v = value.strip().lower()
                # Detect generic placeholder patterns or obviously invalid short strings
                if v in {"your_api_key_here", "your_api_secret_here"}:
                    return True
                if "your_" in v and ("api_key" in v or "api_secret" in v):
                    return True
                if len(v) < 10:  # Binance keys are longer; very short means invalid
                    return True
                return False

            key_placeholder = is_placeholder(api_key)
            secret_placeholder = is_placeholder(api_secret)

            allow_public = os.getenv("ALLOW_PUBLIC_MODE", "true").lower() in ("1", "true", "yes", "on")
            allow_synthetic = os.getenv("ALLOW_SYNTHETIC", "false").lower() in ("1", "true", "yes", "on")

            if key_placeholder or secret_placeholder:
                if allow_public:
                    self.logger.warning(
                        "No valid Binance API credentials provided. Entering PUBLIC (read-only) mode "
                        "- trading disabled; using unauthenticated REST polling."
                    )
                    self.api_client = None
                    self.operational_mode = 'public'
                    return
                if allow_synthetic:
                    self.logger.warning(
                        "No valid Binance API credentials and ALLOW_PUBLIC_MODE disabled but ALLOW_SYNTHETIC enabled. "
                        "Entering SYNTHETIC (simulated) mode - trading disabled."
                    )
                    self.api_client = None
                    self.operational_mode = 'synthetic'
                    return
                raise RuntimeError(
                    "Invalid / placeholder Binance API credentials detected and both ALLOW_PUBLIC_MODE & ALLOW_SYNTHETIC disabled. "
                    "Provide valid TESTNET keys or enable one of the modes (ALLOW_PUBLIC_MODE or ALLOW_SYNTHETIC)."
                )

            if testnet:
                base_url = "https://testnet.binancefuture.com"
                self.logger.info("Using Binance TESTNET - No real money at risk")
            else:
                base_url = "https://fapi.binance.com"
                self.logger.warning("Using Binance MAINNET - Real money trading enabled")

            self.api_client = UMFutures(
                key=api_key,
                secret=api_secret,
                base_url=base_url
            )

            # Test API connection (fail hard if not authenticated)
            try:
                account_info = self.api_client.account()
                balance = float(account_info.get('totalWalletBalance', 0.0))
                self.logger.info(f"Binance API connection established - Wallet Balance: ${balance:.2f}")
                self.operational_mode = 'live'
            except Exception as api_error:
                # Log detailed error and abort
                self.logger.error(f"Binance API validation failed: {api_error}")
                raise RuntimeError("Binance API validation failed; aborting startup (no simulated fallback).") from api_error

        except Exception as e:
            # Fail hard: propagate after logging; do NOT set api_client to None (prevents simulated data usage)
            if self.logger:
                self.logger.error(f"Failed to initialize Binance API (hard fail): {e}")
            raise
    
    def _initialize_watchlist(self) -> None:
        """Initialize trading symbol watchlist.
        
        Priority:
          1. Explicit comma-separated `watchlist` under [trading] in config (preferred)
          2. Defaults / exchange-derived watchlist (existing behavior)
        """
        try:
            # 1) Check config for explicit watchlist string
            try:
                configured = self.config.get('trading', 'watchlist', fallback='')
                if configured:
                    # Parse comma-separated list and normalize
                    parsed = [s.strip().upper() for s in configured.split(',') if s.strip()]
                    if parsed:
                        self.watchlist = parsed
                        self.logger.info(f"Using configured watchlist from config: {self.watchlist}")
                        return
            except Exception:
                # Ignore parsing errors and fall back to automatic behavior
                pass
    
            # 2) Fallback to previous default / exchange-driven logic
            default_watchlist = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSUT']  # keep original intent
            # Correct any obvious typo
            default_watchlist = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']
    
            if self.api_client is None:
                # Public mode - we may later enhance to fetch top volume via public endpoint
                self.watchlist = default_watchlist
                self.logger.info(f"Public mode watchlist: {self.watchlist}")
            else:
                exchange_info = self.api_client.exchange_info()
                available_symbols = [s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']
                # Prefer configured symbols if they exist in the exchange; otherwise use defaults intersect
                self.watchlist = [symbol for symbol in default_watchlist if symbol in available_symbols]
                if not self.watchlist:
                    # As a last resort use top-volume symbols from exchange
                    try:
                        tickers = self.api_client.ticker_24hr_price_change()
                        usdt_pairs = [t['symbol'] for t in tickers if t['symbol'].endswith('USDT')]
                        self.watchlist = usdt_pairs[:5]
                    except Exception:
                        raise ValueError("No valid trading symbols found")
                self.logger.info(f"Initialized watchlist: {self.watchlist}")
    
        except Exception as e:
            self.logger.error(f"Failed to initialize watchlist: {e}")
            self.watchlist = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']
            self.logger.warning(f"Using fallback watchlist: {self.watchlist}")
    
    def _initialize_components(self) -> None:
        """Initialize all system components."""
        try:
            # Risk manager (even in public mode for future evaluation logic, but may be limited)
            self.logger.info("Initializing Risk Manager...")
            self.risk_manager = RiskManager(self.api_client)

            # Ingestion: choose based on resolved operational mode
            if self.operational_mode == 'live':
                self.logger.info("Initializing Data Ingestor (websocket live)...")
                self.data_ingestor = DataIngestor(self.watchlist, self.redis_manager, self.api_client, synthetic_mode=False)
            elif self.operational_mode == 'public':
                self.logger.info("Initializing Public Price Ingestor (public mode)...")
                self.public_price_ingestor = PublicPriceIngestor(self.watchlist, self.redis_manager)
            elif self.operational_mode == 'synthetic':
                self.logger.info("Initializing Data Ingestor (synthetic simulated mode)...")
                self.data_ingestor = DataIngestor(self.watchlist, self.redis_manager, api_client=None, synthetic_mode=True)
            else:
                raise RuntimeError(f"Unknown operational mode: {self.operational_mode}")

            # Signal Engine
            self.logger.info("Initializing Signal Engine...")
            self.signal_engine = SignalEngine(self.redis_manager, self.watchlist)
            
            # Reddit Ingestor
            self.logger.info("Initializing Reddit Ingestor...")
            self.reddit_ingestor = RedditIngestor(self.redis_manager)
            
            # Performance Tracker
            self.logger.info("Initializing Performance Tracker...")
            self.performance_tracker = PerformanceTracker(self.redis_manager)

            # Decision Logger (captures trading_signals -> decision_logs)
            self.logger.info("Initializing Decision Logger...")
            self.decision_logger = DecisionLogger(
                self.redis_manager,
                mode_resolver=lambda: (self.operational_mode or ("live" if self.api_client else ("public" if self.public_price_ingestor else "synthetic")))
            )

            # Execution Manager only if trading possible
            if self.operational_mode == 'live' and self.api_client:
                self.logger.info("Initializing Execution Manager...")
                self.execution_manager = ExecutionManager(
                    self.api_client,
                    self.risk_manager,
                    self.redis_manager
                )
            else:
                self.logger.info(f"Skipping Execution Manager initialization ({self.operational_mode} mode - trading disabled)")

            # Monitoring Agent
            self.logger.info("Initializing Monitoring Agent...")
            self.monitoring_agent = MonitoringAgent(
                self.data_ingestor if self.data_ingestor else self.public_price_ingestor,
                self.signal_engine,
                self.risk_manager,
                self.execution_manager,
                self.reddit_ingestor,
                self.performance_tracker
            )

            # Health Dashboard
            self.logger.info("Initializing Health Dashboard...")
            dashboard_port = self.config.get('system', 'dashboard_port', int, 8080)
            self.health_dashboard = HealthDashboard(
                self.redis_manager,
                system_reference=self,
                port=dashboard_port
            )

            self.logger.info("All components initialized")

        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    def start(self) -> None:
        """Start the trading system."""
        try:
            if self.is_running:
                self.logger.warning("System is already running")
                return
            
            self.start_time = time.time() * 1000  # milliseconds
            self.is_running = True
            
            self.logger.info("Starting Helios Trading System components...")
            
            # Start components in order
            self.risk_manager.start()
            if self.data_ingestor:
                self.data_ingestor.start()
            elif self.public_price_ingestor:
                self.public_price_ingestor.start()
            self.reddit_ingestor.start()
            self.performance_tracker.start()
            self.signal_engine.start()
            if self.decision_logger:
                self.decision_logger.start()
            if self.execution_manager:
                self.execution_manager.start()
            self.monitoring_agent.start()
            self.health_dashboard.start()
            
            # Start dynamic watchlist management
            self._start_watchlist_management()
            
            # Set up signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            self.logger.info("Helios Trading System started successfully")
            self.logger.info("Press Ctrl+C to stop the system")
            
            # Print startup summary
            self._print_startup_summary()
            
        except Exception as e:
            self.logger.error(f"Failed to start system: {e}")
            self.stop()
            raise
    
    def stop(self) -> None:
        """Stop the trading system gracefully."""
        if not self.is_running:
            return
        
        self.logger.info("Stopping Helios Trading System...")
        self.is_running = False
        self.shutdown_event.set()
        
        # Stop components in reverse order
        components = [
            ("Health Dashboard", self.health_dashboard),
            ("Monitoring Agent", self.monitoring_agent),
            ("Execution Manager", self.execution_manager),
            ("Signal Engine", self.signal_engine),
            ("Decision Logger", self.decision_logger),
            ("Performance Tracker", self.performance_tracker),
            ("Reddit Ingestor", self.reddit_ingestor),
            ("Data Ingestor", self.data_ingestor),
            ("Risk Manager", self.risk_manager)
        ]
        
        for name, component in components:
            if component:
                try:
                    self.logger.info(f"Stopping {name}...")
                    component.stop()
                    self.logger.info(f"{name} stopped")
                except Exception as e:
                    self.logger.error(f"Error stopping {name}: {e}")
        
        # Stop watchlist management
        if self.watchlist_thread and self.watchlist_thread.is_alive():
            self.watchlist_thread.join(timeout=5)
        
        self.logger.info("Helios Trading System stopped")
    
    def run(self) -> None:
        """Run the system (blocking)."""
        try:
            # Main system loop
            while self.is_running:
                try:
                    # Check system health
                    self._check_system_health()
                    
                    # Sleep for a short interval
                    time.sleep(5)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}")
                    time.sleep(10)
            
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
    
    def _start_watchlist_management(self) -> None:
        """Start dynamic watchlist management."""
        def watchlist_manager():
            while self.is_running and not self.shutdown_event.is_set():
                try:
                    self._update_dynamic_watchlist()
                    # Update every 5 minutes
                    self.shutdown_event.wait(300)
                except Exception as e:
                    self.logger.error(f"Error in watchlist management: {e}")
                    self.shutdown_event.wait(60)
        
        self.watchlist_thread = threading.Thread(target=watchlist_manager, daemon=True)
        self.watchlist_thread.start()
        self.logger.info("Dynamic watchlist management started")
    
    def _update_dynamic_watchlist(self) -> None:
        """Update the watchlist based on market conditions."""
        try:
            # Skip dynamic watchlist updates when no authenticated API client (public/synthetic modes)
            if self.api_client is None:
                self.logger.debug("Public/Synthetic mode: Skipping dynamic watchlist updates")
                return
                
            # This is a simplified implementation
            # In the full system, this would analyze volatility and liquidity metrics
            watchlist_size = self.config.get('trading', 'watchlist_size', int)
            
            # Get 24h ticker statistics
            tickers = self.api_client.ticker_24hr_price_change()
            
            # Filter USDT pairs and sort by volume
            usdt_pairs = [t for t in tickers if t['symbol'].endswith('USDT')]
            usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
            
            # Take top symbols
            new_watchlist = [t['symbol'] for t in usdt_pairs[:watchlist_size]]
            
            # Update if changed
            if new_watchlist != self.watchlist:
                old_watchlist = self.watchlist.copy()
                self.watchlist = new_watchlist
                
                # Update components
                if self.data_ingestor:
                    # Remove old symbols
                    for symbol in old_watchlist:
                        if symbol not in new_watchlist:
                            self.data_ingestor.remove_symbol(symbol)
                    
                    # Add new symbols
                    for symbol in new_watchlist:
                        if symbol not in old_watchlist:
                            self.data_ingestor.add_symbol(symbol)
                
                self.logger.info(f"Updated watchlist: {self.watchlist}")
                
        except Exception as e:
            self.logger.error(f"Error updating watchlist: {e}")
    
    def _check_system_health(self) -> None:
        """Check overall system health."""
        try:
            # Get health status from all components
            health_data = {}
            
            components = {
                'data_ingestor': self.data_ingestor,
                'reddit_ingestor': self.reddit_ingestor,
                'performance_tracker': self.performance_tracker,
                'signal_engine': self.signal_engine,
                'risk_manager': self.risk_manager,
                'execution_manager': self.execution_manager,
                'monitoring_agent': self.monitoring_agent,
                'health_dashboard': self.health_dashboard
            }
            
            unhealthy_components = []
            
            for name, component in components.items():
                if component and hasattr(component, 'get_health_status'):
                    health = component.get_health_status()
                    health_data[name] = health
                    
                    if not health.get('is_running', False):
                        unhealthy_components.append(name)
            
            # Log health issues
            if unhealthy_components:
                self.logger.warning(f"Unhealthy components: {', '.join(unhealthy_components)}")
            
        except Exception as e:
            self.logger.error(f"Error checking system health: {e}")
    
    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop()
    
    def _print_startup_summary(self) -> None:
        """Print system startup summary."""
        try:
            if self.operational_mode == 'live':
                trading_mode = "LIVE (TESTNET)" if self.config.get('binance', 'testnet', bool) else "LIVE (MAINNET)"
            elif self.operational_mode == 'public':
                trading_mode = "PUBLIC (read-only)"
            elif self.operational_mode == 'synthetic':
                trading_mode = "SYNTHETIC (simulated)"
            else:
                trading_mode = "UNKNOWN"

            components_total = 8  # legacy count; compute active
            active_components = sum([
                1 if self.risk_manager else 0,
                1 if (self.data_ingestor or self.public_price_ingestor) else 0,
                1 if self.signal_engine else 0,
                1 if self.execution_manager else 0,
                1 if self.reddit_ingestor else 0,
                1 if self.performance_tracker else 0,
                1 if self.monitoring_agent else 0,
                1 if self.health_dashboard else 0
            ])
            summary = [
                "",
                "🚀 HELIOS TRADING SYSTEM - OPERATIONAL 🚀",
                "=" * 50,
                f"Mode: {trading_mode}",
                f"Trading Symbols: {', '.join(self.watchlist)}",
                f"Components Running: {active_components}/{components_total}",
                f"Risk Management: {'ACTIVE' if self.risk_manager else 'N/A'}",
                f"Execution: {'ENABLED' if self.execution_manager else 'DISABLED'}",
                "=" * 50,
                ("System is monitoring markets and ready to trade." if self.execution_manager
                 else ("System is in read-only PUBLIC mode (signals generated; orders NOT executed)." if self.operational_mode == 'public'
                       else "System is in SYNTHETIC simulation mode (no external data; trading disabled).")),
                "Monitor logs for trading signals and system status.",
                ""
            ]

            for line in summary:
                self.logger.info(line)

        except Exception as e:
            self.logger.error(f"Error printing startup summary: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get comprehensive system status.
        
        Returns:
            System status information
        """
        try:
            status = {
                'is_running': self.is_running,
                'start_time': self.start_time,
                'uptime_seconds': (time.time() * 1000 - self.start_time) / 1000 if self.start_time else 0,
                'watchlist': self.watchlist,
                'operational_mode': self.operational_mode,
                'components': {}
            }
            
            # Get component status
            components = {
                'data_ingestor': self.data_ingestor,
                'public_price_ingestor': self.public_price_ingestor,
                'reddit_ingestor': self.reddit_ingestor,
                'performance_tracker': self.performance_tracker,
                'decision_logger': self.decision_logger,
                'signal_engine': self.signal_engine,
                'risk_manager': self.risk_manager,
                'execution_manager': self.execution_manager,
                'monitoring_agent': self.monitoring_agent,
                'health_dashboard': self.health_dashboard
            }
            
            for name, component in components.items():
                if component and hasattr(component, 'get_health_status'):
                    status['components'][name] = component.get_health_status()
                else:
                    status['components'][name] = {'is_running': False}
            
            return status
            
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {'error': str(e)}


def main():
    """Main entry point."""
    helios = HeliosSystem()
    
    try:
        # Initialize system
        if not helios.initialize():
            sys.exit(1)
        
        # Start system
        helios.start()
        
        # Run main loop
        helios.run()
        
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        if helios.logger:
            helios.logger.error(f"Fatal error: {e}")
        else:
            print(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        helios.stop()


if __name__ == "__main__":
    main()
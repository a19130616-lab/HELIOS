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
from app.signal_engine import SignalEngine
from app.risk_manager import RiskManager
from app.execution_manager import ExecutionManager
from app.monitoring_agent import MonitoringAgent

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
        self.signal_engine = None
        self.risk_manager = None
        self.execution_manager = None
        self.monitoring_agent = None
        
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
            
            # Set up logging
            log_level = self.config.get('system', 'log_level', str)
            setup_logging(log_level)
            self.logger = logging.getLogger(__name__)
            
            # Create logs directory
            os.makedirs('logs', exist_ok=True)
            
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
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis: {e}")
            raise
    
    def _initialize_api_client(self) -> None:
        """Initialize Binance API client."""
        try:
            api_key = self.config.get('binance', 'api_key', str)
            api_secret = self.config.get('binance', 'api_secret', str)
            testnet = self.config.get('binance', 'testnet', bool)
            
            if api_key == "YOUR_API_KEY_HERE" or api_secret == "YOUR_API_SECRET_HERE":
                raise ValueError("Please configure your Binance API credentials in config.ini")
            
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
            
            # Test API connection
            account_info = self.api_client.account()
            balance = float(account_info['totalWalletBalance'])
            
            self.logger.info(f"Binance API connection established - Balance: ${balance:.2f}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance API: {e}")
            raise
    
    def _initialize_watchlist(self) -> None:
        """Initialize trading symbol watchlist."""
        try:
            # Start with popular crypto pairs
            default_watchlist = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']
            
            # Validate symbols with exchange
            exchange_info = self.api_client.exchange_info()
            available_symbols = [s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']
            
            # Filter to only available symbols
            self.watchlist = [symbol for symbol in default_watchlist if symbol in available_symbols]
            
            if not self.watchlist:
                raise ValueError("No valid trading symbols found")
            
            self.logger.info(f"Initialized watchlist: {self.watchlist}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize watchlist: {e}")
            raise
    
    def _initialize_components(self) -> None:
        """Initialize all system components."""
        try:
            # Initialize Risk Manager first (needed by Execution Manager)
            self.logger.info("Initializing Risk Manager...")
            self.risk_manager = RiskManager(self.api_client)
            
            # Initialize Data Ingestor
            self.logger.info("Initializing Data Ingestor...")
            self.data_ingestor = DataIngestor(self.watchlist, self.redis_manager)
            
            # Initialize Signal Engine
            self.logger.info("Initializing Signal Engine...")
            self.signal_engine = SignalEngine(self.redis_manager, self.watchlist)
            
            # Initialize Execution Manager
            self.logger.info("Initializing Execution Manager...")
            self.execution_manager = ExecutionManager(
                self.api_client, 
                self.risk_manager, 
                self.redis_manager
            )
            
            # Initialize Monitoring Agent
            self.logger.info("Initializing Monitoring Agent...")
            self.monitoring_agent = MonitoringAgent(
                self.data_ingestor,
                self.signal_engine,
                self.risk_manager,
                self.execution_manager
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
            self.data_ingestor.start()
            self.signal_engine.start()
            self.execution_manager.start()
            self.monitoring_agent.start()
            
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
            ("Monitoring Agent", self.monitoring_agent),
            ("Execution Manager", self.execution_manager),
            ("Signal Engine", self.signal_engine),
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
                'signal_engine': self.signal_engine,
                'risk_manager': self.risk_manager,
                'execution_manager': self.execution_manager,
                'monitoring_agent': self.monitoring_agent
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
            summary = [
                "",
                "🚀 HELIOS TRADING SYSTEM - OPERATIONAL 🚀",
                "=" * 50,
                f"Trading Symbols: {', '.join(self.watchlist)}",
                f"Components Running: 5/5",
                f"Risk Management: ACTIVE",
                f"API Mode: {'TESTNET' if self.config.get('binance', 'testnet', bool) else 'MAINNET'}",
                "=" * 50,
                "System is now actively monitoring markets and ready to trade.",
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
                'components': {}
            }
            
            # Get component status
            components = {
                'data_ingestor': self.data_ingestor,
                'signal_engine': self.signal_engine,
                'risk_manager': self.risk_manager,
                'execution_manager': self.execution_manager,
                'monitoring_agent': self.monitoring_agent
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
# Helios High-Frequency Trading System

A sophisticated, AI-powered cryptocurrency trading bot designed for minute-level contract trading on major exchanges like Binance Futures. Helios combines market microstructure analysis with machine learning to identify and execute high-probability trading opportunities.

## 🚀 Key Features

### Core Strategy
- **Normalized Order Book Imbalance (NOBI)**: Real-time analysis of order book pressure to identify trading opportunities
- **AI-Powered Signal Filtering**: LSTM+XGBoost hybrid model for sophisticated signal validation
- **Reddit Sentiment Analysis**: Real-time sentiment analysis from cryptocurrency-related subreddits
- **Enhanced Trend Analysis**: Multi-timeframe trend detection with momentum indicators
- **Dynamic Asset Selection**: Automated scanning for optimal volatility-liquidity balance
- **Market Regime Detection**: Adaptive strategy parameters based on market conditions

### Risk Management
- **Kelly Criterion Position Sizing**: Mathematical optimization of position sizes
- **Dynamic Trailing Stops**: ATR-based stop losses that adapt to market volatility
- **Multi-Level Risk Controls**: Account-level liquidation avoidance protocol
- **Emergency De-risking**: Automated position reduction during high-risk scenarios

### System Architecture
- **Microservices Design**: Modular, scalable architecture with independent components
- **Real-time Data Processing**: High-frequency WebSocket streams from Binance
- **Redis-based Communication**: Fast inter-service messaging and data storage
- **Azure Monitor Integration**: Cloud-based monitoring and alerting

### Operating Modes
Helios operates in explicitly gated modes to ensure safe defaults:
| Mode | Trigger Condition | Data Source | Trading | Active Components |
|------|------------------|-------------|---------|-------------------|
| live (testnet/mainnet) | Valid Binance API key & secret | Authenticated WebSocket streams | Enabled | DataIngestor + ExecutionManager + PerformanceTracker |
| public (read‑only) | Missing/placeholder credentials AND ALLOW_PUBLIC_MODE=true | Binance REST polling + CoinGecko fallback | Disabled | PublicPriceIngestor + DecisionLogger |
| synthetic (legacy) | ALLOW_SYNTHETIC=true (no valid creds) | Internal generators | Disabled | DataIngestor (synthetic threads) |

Resolution priority: (1) live if valid credentials → (2) public if allowed → (3) synthetic if explicitly enabled → otherwise abort.

### Decision Logging & Simulated PnL
When trading is disabled (public / synthetic), signals are still produced and persisted:
- DecisionLogger subscribes to the Redis channel trading_signals and appends normalized entries (list: decision_logs, length ≤500, 24h TTL).
- Signal directions now include EXIT_LONG and EXIT_SHORT enabling explicit position closure events.
- Realized PnL is computed only on EXIT_* events using a configurable fixed position size (simulation mode) and stored per entry (realized_pnl, position_size).
- Aggregated simulated performance metrics are maintained in Redis key simulated_pnl_summary (fields: total_pnl, total_trades, wins, losses, win_rate, max_drawdown_pct, peak_equity, updated_ts).
- /api/performance automatically falls back to simulated_pnl_summary when no live execution data is available (public/synthetic modes) so the dashboard shows continuous performance feedback.

### EXIT Signal Semantics
- LONG / SHORT: Open (or replace) a single simulated position per symbol direction.
- EXIT_LONG / EXIT_SHORT: Close the existing position (if any) and realize PnL = (exit_price - entry_price) * position_size (sign adjusted for direction).
- Positions are tracked in-memory by DecisionLogger; unmatched EXIT_* events are safely ignored (logged).

### Configuration Additions
Add a [simulation] section to config/config.ini to control position sizing for simulated PnL (see Configuration section below).

### Simulation Modes
Helios supports 4 distinct simulation modes to test different strategies and fee structures:

| Mode | Description | Fees | Cooldowns |
|------|-------------|------|-----------|
| **HF_MARKET** | High Frequency, Market Orders | Taker (0.04%) | Standard (2m) |
| **HF_LIMIT** | High Frequency, Limit Orders | Maker (0.02%)* | Standard (2m) |
| **LF_MARKET** | Low Frequency, Market Orders | Taker (0.04%) | Extended (15m) |
| **LF_LIMIT** | Low Frequency, Limit Orders | Maker (0.02%)* | Extended (15m) |

*\*Note: Limit modes use Maker fees for Entry/Exit, but Taker fees for Stop Loss and Time Exits.*

Configure this in `config.ini`:
```ini
[simulation]
mode = HF_MARKET  # or HF_LIMIT, LF_MARKET, LF_LIMIT
```

### Safety Principles
- No implicit "demo" fallback: system aborts if neither credentials nor an allowed read-only/synthetic mode are configured.
- Mode badge (LIVE / PUBLIC / SYNTHETIC) shown on dashboard and included in /api/prices response.
- Staleness indicators flag price rows older than threshold (e.g., >60s).

## 📋 Prerequisites

- **Python 3.9+**: Required for all dependencies
- **Redis Server**: For inter-service communication and caching
- **Binance Futures Account**: API keys with futures trading permissions
- **Azure Account** (optional): For cloud monitoring integration

## 🛠️ Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd Helios
```

### 2. Create Virtual Environment
```bash
python -m venv helios-env
source helios-env/bin/activate  # On Windows: helios-env\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Install Redis
**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install redis-server
sudo systemctl start redis-server
```

**macOS:**
```bash
brew install redis
brew services start redis
```

**Windows:**
Download and install Redis from the official website or use WSL.

## ⚙️ Configuration

### 1. API Configuration
Edit `config/config.ini` and add your Binance API credentials:

```ini
[binance]
api_key = your_actual_api_key_here
api_secret = your_actual_api_secret_here
testnet = True  # Set to False for live trading
```

### 2. Risk Parameters
Adjust risk settings according to your preferences:

```ini
[risk]
kelly_fraction = 0.5          # Use 50% of Kelly recommendation
leverage = 10                 # Maximum leverage to use
warning_margin_ratio = 30     # Warning threshold (%)
critical_margin_ratio = 15    # Emergency threshold (%)
```

### 3. Trading & Simulation Configuration
Configure signal, trading, and (optional) simulation parameters:

```ini
[signals]
nobi_depth = 10                    # Order book depth for NOBI calculation
nobi_trigger_threshold = 0.4       # NOBI threshold for signal generation
ml_confidence_threshold = 0.75     # ML model confidence requirement

[trading]
watchlist_size = 5                 # Number of symbols to monitor
rescan_interval_sec = 300          # Watchlist update interval

[simulation]
position_size = 1.0                # Fixed contract/asset size used for simulated PnL when trading disabled
```

Notes:
- position_size only affects simulated realized_pnl calculations in public/synthetic modes.
- EXIT_LONG / EXIT_SHORT signals must be emitted by the strategy logic (SignalEngine) to realize PnL; without EXIT events simulated results remain unrealized.
### 4. Azure Monitor (Optional)
For cloud monitoring, add your Azure connection string:

```ini
[azure]
connection_string = your_azure_monitor_connection_string
```

## 🏃‍♂️ Running the System

### Option 1: Docker (Recommended)

**Prerequisites:**
- Docker and Docker Compose installed

**Steps:**
1. **Configure Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and settings
   ```

2. **Configure Settings**
   ```bash
   cp config/config.ini.template config/config.ini
   # Edit config/config.ini with your preferences
   ```

3. **Run with Docker Compose**
   ```bash
   # Production mode
   docker-compose up -d
   
   # Development mode (with live code reloading)
   docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
   
   # With Redis Commander for database management
   docker-compose --profile debug up -d
   ```

4. **Monitor Logs**
   ```bash
   docker-compose logs -f helios-app
   ```

5. **Stop the System**
   ```bash
   docker-compose down
   ```

### Option 2: Local Installation

**Prerequisites:**
- Python 3.9+
- Redis Server

**Steps:**

1. **Start Redis (if not running)**
   ```bash
   redis-server
   ```

2. **Create Required Directories**
   ```bash
   mkdir -p logs models
   ```

3. **Prepare ML Model (Optional)**
   If you have a trained XGBoost model, place it in the `models/` directory:
   ```bash
   cp your_model.json models/xgb_model.json
   cp your_scaler.pkl models/xgb_model_scaler.pkl
   ```

4. **Start Helios**
   ```bash
   python main.py
   ```

## ☁️ Deployment

### Azure VM Deployment Examples

**1. Connect to Azure VM via SSH**
```bash
ssh -i keys/HeliosBot_key.pem azureuser@20.39.199.2
```

**2. Deploy Code via Rsync**
Syncs local code to the remote server, excluding unnecessary files.
```bash
rsync -avz -e "ssh -i /Users/yizhuowang/Documents/dev/keys/HeliosBot_key.pem" \
--exclude 'helios-env' \
--exclude '.git' \
--exclude '__pycache__' \
--exclude 'logs' \
/Users/yizhuowang/src/Helios/ azureuser@20.39.199.2:~/Helios/
```

**3. Restart Application on Server**
Rebuilds and restarts the Docker containers in detached mode.
```bash
docker-compose down && docker-compose up -d --build
```

## 📊 System Components

### Data Ingestor
- Connects to Binance WebSocket streams
- Processes order book, trade, and kline data
- Stores normalized data in Redis for other components

### Reddit Ingestor
- Monitors cryptocurrency-related subreddits
- Performs sentiment analysis on posts and comments
- Integrates sentiment data with trading signals

### Signal Engine
- Calculates NOBI from real-time order book data
- Integrates sentiment and enhanced trend analysis
- Applies ML model for signal validation
- Publishes trading signals to execution queue

### Risk Manager
- Implements Kelly Criterion position sizing
- Manages dynamic trailing stop losses
- Monitors account margin and executes emergency protocols

### Execution Manager
- Receives and validates trading signals
- Places and manages orders via Binance API
- Coordinates with Risk Manager for position tracking

### Monitoring Agent
- Collects system and performance metrics
- Sends data to Azure Monitor (if configured)
- Generates alerts for critical conditions

## 📈 Monitoring and Logs

### Log Files
- `logs/helios.log`: Main system log
- `logs/alerts.log`: Critical alerts and warnings

### Key Metrics Monitored
- **Performance**: Win rate, PnL, Sharpe ratio, drawdown
- **Risk**: Margin ratio, position exposure, leverage usage
- **System**: Component health, latency, error rates
- **Trading**: Signal generation, execution success, order fill rates

### Health Checks
The system continuously monitors:
- Component responsiveness
- API connectivity
- Data stream integrity
- Risk threshold violations

## ⚠️ Important Warnings

### Financial Risk
- **This is experimental software**: Use at your own risk
- **Start with testnet**: Always test thoroughly before live trading
- **Never risk more than you can afford to lose**
- **High-frequency trading carries significant risks**

### Security
- **Protect your API keys**: Never commit them to version control
- **Use IP restrictions**: Configure Binance API for specific IPs only
- **Enable 2FA**: Use two-factor authentication on your exchange account
- **Monitor regularly**: Keep track of all trading activity

### Technical Considerations
- **Latency matters**: Use a VPS close to exchange servers for optimal performance
- **Monitor data feeds**: Ensure continuous data connectivity
- **Resource usage**: The system requires sufficient CPU and memory
- **Network stability**: Unstable connections can cause trading issues

## 🔧 Troubleshooting

### Common Issues

**Redis Connection Failed**
```bash
# Check if Redis is running
redis-cli ping
# Should return "PONG"
```

**Binance API Errors**
- Verify API key and secret are correct
- Check if API has futures trading permissions
- Ensure IP is whitelisted (if IP restriction enabled)

**No Trading Signals Generated**
- Check if market is open and liquid
- Verify NOBI threshold is not too high
- Ensure ML model is loaded correctly

**High Memory Usage**
- Reduce watchlist size
- Increase data cleanup intervals
- Monitor Redis memory usage

### Debug Mode
Enable debug logging by setting in `config.ini`:
```ini
[system]
log_level = DEBUG
```

## 📚 Algorithm Details

### NOBI Calculation
```
NOBI = (Depth_ask - Depth_bid) / (Depth_ask + Depth_bid)
```
Where depth is calculated as the sum of price × quantity for the top L levels.

### Kelly Criterion Position Sizing
```
Kelly% = Win_Rate - (1 - Win_Rate) / Win_Loss_Ratio
Position_Size = Kelly% × Kelly_Fraction × Available_Balance
```

### Trailing Stop Logic
- Initial stop: Entry_Price ± (ATR × Multiplier)
- Trailing update: Stop follows favorable price movement
- Trigger: Current price crosses stop level

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is for educational and research purposes. Please review and comply with all applicable regulations in your jurisdiction before using for live trading.

## 🆘 Support

For questions, issues, or discussions:
- Open an issue on GitHub
- Review the troubleshooting section
- Check system logs for error details

---

**Disclaimer**: This software is provided "as is" without any guarantees. Cryptocurrency trading involves substantial risk of loss. The authors are not responsible for any financial losses incurred through the use of this software.


TODO: 
1. Decisions should based on the current opening positions
2. Add current holding and order opening in dashboard
3. Add one-click stop and start in dashboard
# Helios High-Frequency Trading System

A sophisticated, AI-powered cryptocurrency trading bot designed for minute-level contract trading on major exchanges like Binance Futures. Helios combines market microstructure analysis with machine learning to identify and execute high-probability trading opportunities.

## 🚀 Key Features

### Core Strategy
- **Normalized Order Book Imbalance (NOBI)**: Real-time analysis of order book pressure to identify trading opportunities
- **AI-Powered Signal Filtering**: LSTM+XGBoost hybrid model for sophisticated signal validation
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

### 3. Trading Configuration
Configure signal and trading parameters:

```ini
[signals]
nobi_depth = 10                    # Order book depth for NOBI calculation
nobi_trigger_threshold = 0.4       # NOBI threshold for signal generation
ml_confidence_threshold = 0.75     # ML model confidence requirement

[trading]
watchlist_size = 5                 # Number of symbols to monitor
rescan_interval_sec = 300          # Watchlist update interval
```

### 4. Azure Monitor (Optional)
For cloud monitoring, add your Azure connection string:

```ini
[azure]
connection_string = your_azure_monitor_connection_string
```

## 🏃‍♂️ Running the System

### 1. Start Redis (if not running)
```bash
redis-server
```

### 2. Create Required Directories
```bash
mkdir -p logs models
```

### 3. Prepare ML Model (Optional)
If you have a trained XGBoost model, place it in the `models/` directory:
```bash
cp your_model.json models/xgb_model.json
cp your_scaler.pkl models/xgb_model_scaler.pkl
```

### 4. Start Helios
```bash
python main.py
```

## 📊 System Components

### Data Ingestor
- Connects to Binance WebSocket streams
- Processes order book, trade, and kline data
- Stores normalized data in Redis for other components

### Signal Engine
- Calculates NOBI from real-time order book data
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